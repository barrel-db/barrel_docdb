%%%-------------------------------------------------------------------
%%% @doc Changes feed API for barrel_docdb
%%%
%%% Provides functions to track and query document changes in a
%%% database. Changes are ordered by HLC (Hybrid Logical Clock)
%%% timestamps for distributed ordering.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_changes).

-include("barrel_docdb.hrl").

%% API
-export([
    fold_changes/5,
    get_changes/4,
    get_last_seq/2,  %% Returns opaque sequence (encoded HLC binary)
    get_last_hlc/2,  %% Returns decoded HLC timestamp
    count_changes_since/3
]).

%% Internal - for use by barrel_db_writer
-export([
    write_change/4,
    write_change_ops/3,
    delete_old_change/4
]).

%% Path-indexed changes API
-export([
    write_path_index_ops/3,
    update_path_index_ops/5,
    get_changes_by_path/5
]).

%%====================================================================
%% Types
%%====================================================================

-type changes_result() :: #{
    changes := [change()],
    last_hlc := barrel_hlc:timestamp(),
    pending := non_neg_integer()
}.

-type fold_fun() :: fun((change(), Acc :: term()) ->
    {ok, Acc :: term()} | {stop, Acc :: term()} | stop).

-type changes_opts() :: #{
    include_docs => boolean(),
    limit => non_neg_integer(),
    descending => boolean(),
    style => main_only | all_docs,
    doc_ids => [docid()],
    paths => [binary()],  % MQTT-style path patterns to filter by
    query => barrel_query:query_spec()  % Query to filter by
}.

-export_type([changes_result/0, fold_fun/0, changes_opts/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Fold over changes since a given HLC timestamp (exclusive)
%% Changes returned are strictly after the given HLC.
%% Use 'first' to get all changes from the beginning.
-spec fold_changes(barrel_store_rocksdb:db_ref(), db_name(),
                   barrel_hlc:timestamp() | first, fold_fun(), term()) ->
    {ok, term(), barrel_hlc:timestamp()}.
fold_changes(StoreRef, DbName, Since, Fun, Acc) ->
    fold_changes_internal(StoreRef, DbName, Since, Fun, Acc, normal).

%% @doc Fold over changes optimized for full sequential scans.
%% Uses readahead and avoids cache pollution for large scans.
%% Best for scanning all changes from the beginning without limit.
-spec fold_changes_long_scan(barrel_store_rocksdb:db_ref(), db_name(),
                              barrel_hlc:timestamp() | first, fold_fun(), term()) ->
    {ok, term(), barrel_hlc:timestamp()}.
fold_changes_long_scan(StoreRef, DbName, Since, Fun, Acc) ->
    fold_changes_internal(StoreRef, DbName, Since, Fun, Acc, long_scan).

%% @private Internal fold with scan mode selection
fold_changes_internal(StoreRef, DbName, Since, Fun, Acc, ScanMode) ->
    {StartHlc, StartKey} = case Since of
        first ->
            %% Start from the very beginning
            Min = barrel_hlc:min(),
            {Min, barrel_store_keys:doc_hlc(DbName, Min)};
        SinceHlc ->
            %% Exclusive: start at SinceHlc, we'll skip matching entries
            {SinceHlc, barrel_store_keys:doc_hlc(DbName, SinceHlc)}
    end,
    EndKey = barrel_store_keys:doc_hlc_end(DbName),

    FoldFun = fun(Key, Value, {CurrentHlc, AccIn}) ->
        ChangeHlc = barrel_store_keys:decode_hlc_key(DbName, Key),
        %% Skip if we're at the exact Since HLC (exclusive)
        case Since =/= first andalso barrel_hlc:equal(ChangeHlc, Since) of
            true ->
                {ok, {CurrentHlc, AccIn}};
            false ->
                Change = decode_change(Value, ChangeHlc),
                case Fun(Change, AccIn) of
                    {ok, AccOut} ->
                        {ok, {ChangeHlc, AccOut}};
                    {stop, AccOut} ->
                        {stop, {ChangeHlc, AccOut}};
                    stop ->
                        {stop, {CurrentHlc, AccIn}}
                end
        end
    end,

    {LastHlc, FinalAcc} = case ScanMode of
        long_scan ->
            barrel_store_rocksdb:fold_range_long_scan(
                StoreRef, StartKey, EndKey, FoldFun, {StartHlc, Acc});
        normal ->
            barrel_store_rocksdb:fold_range(
                StoreRef, StartKey, EndKey, FoldFun, {StartHlc, Acc})
    end,
    {ok, FinalAcc, LastHlc}.

%% @doc Get a list of changes since an HLC timestamp
-spec get_changes(barrel_store_rocksdb:db_ref(), db_name(),
                  barrel_hlc:timestamp() | first, changes_opts()) ->
    {ok, [change()], barrel_hlc:timestamp()}.
get_changes(StoreRef, DbName, Since, Opts) ->
    DocIds = maps:get(doc_ids, Opts, undefined),
    PathPatterns = maps:get(paths, Opts, undefined),
    QuerySpec = maps:get(query, Opts, undefined),

    %% Determine if we can use path index for more efficient query
    %% We can use path index if:
    %% - Path patterns are specified
    %% - No doc_ids filter (can't combine with path index efficiently)
    %% - No query filter (path index entries don't include doc body for query matching)
    case {PathPatterns, DocIds, QuerySpec} of
        {[SinglePath], undefined, undefined} when is_binary(SinglePath) ->
            %% Single path pattern, no doc_ids, no query - use path index directly
            get_changes_with_path_index(StoreRef, DbName, SinglePath, Since, Opts, QuerySpec);
        {Paths, undefined, undefined} when is_list(Paths), length(Paths) > 1 ->
            %% Multiple path patterns, no query - merge results from path indexes
            get_changes_with_multiple_paths(StoreRef, DbName, Paths, Since, Opts, QuerySpec);
        _ ->
            %% Fall back to full scan with filtering when:
            %% - doc_ids is specified (can't use path index)
            %% - query is specified (path index entries don't have doc body)
            %% - no paths specified
            get_changes_full_scan(StoreRef, DbName, Since, Opts)
    end.

%% @private Get changes using path index for a single path pattern
%% Falls back to full scan if path index is empty (backwards compatibility)
get_changes_with_path_index(StoreRef, DbName, PathPattern, Since, Opts, QuerySpec) ->
    %% Try path index first
    {ok, PathChanges, PathLastHlc} = get_changes_by_path(StoreRef, DbName, PathPattern, Since,
                                                          #{limit => maps:get(limit, Opts, infinity)}),

    case PathChanges of
        [] ->
            %% Path index empty - fall back to full scan
            %% This handles old data without path index or direct write_change calls
            get_changes_full_scan(StoreRef, DbName, Since, Opts#{paths => [PathPattern]});
        _ ->
            %% Path index has entries - use them
            Limit = maps:get(limit, Opts, infinity),
            Style = maps:get(style, Opts, all_docs),
            CompiledQuery = compile_query(QuerySpec),

            %% Apply query filter and style if needed
            FilteredChanges = lists:filtermap(
                fun(Change) ->
                    case maybe_match_query(CompiledQuery, Change) of
                        true ->
                            %% Apply style filter
                            StyledChange = apply_style(Style, Change),
                            {true, StyledChange};
                        false ->
                            false
                    end
                end,
                PathChanges
            ),

            %% Apply limit if query filter was used (might have reduced count)
            LimitedChanges = case {CompiledQuery, Limit} of
                {undefined, _} -> FilteredChanges;
                {_, infinity} -> FilteredChanges;
                {_, N} -> lists:sublist(FilteredChanges, N)
            end,

            Changes = case maps:get(descending, Opts, false) of
                true -> lists:reverse(LimitedChanges);
                false -> LimitedChanges
            end,

            {ok, Changes, PathLastHlc}
    end.

%% @private Get changes from multiple path patterns and merge by HLC
%% Falls back to full scan if path indexes are empty (backwards compatibility)
get_changes_with_multiple_paths(StoreRef, DbName, Paths, Since, Opts, QuerySpec) ->
    %% Get changes from each path index
    AllChanges = lists:flatmap(
        fun(PathPattern) ->
            case get_changes_by_path(StoreRef, DbName, PathPattern, Since, #{}) of
                {ok, Changes, _} -> Changes;
                _ -> []
            end
        end,
        Paths
    ),

    case AllChanges of
        [] ->
            %% Path indexes empty - fall back to full scan
            get_changes_full_scan(StoreRef, DbName, Since, Opts#{paths => Paths});
        _ ->
            Limit = maps:get(limit, Opts, infinity),
            Style = maps:get(style, Opts, all_docs),
            CompiledQuery = compile_query(QuerySpec),

            %% Remove duplicates (same doc_id) keeping latest HLC
            Deduped = dedupe_changes_by_id(AllChanges),

            %% Sort by HLC
            Sorted = lists:sort(
                fun(A, B) ->
                    barrel_hlc:compare(maps:get(hlc, A), maps:get(hlc, B)) =/= gt
                end,
                Deduped
            ),

            %% Apply query filter and limit
            {FilteredChanges, _Count} = lists:foldl(
                fun(Change, {Acc, Count}) ->
                    case Limit =/= infinity andalso Count >= Limit of
                        true ->
                            {Acc, Count};
                        false ->
                            case maybe_match_query(CompiledQuery, Change) of
                                true ->
                                    StyledChange = apply_style(Style, Change),
                                    {[StyledChange | Acc], Count + 1};
                                false ->
                                    {Acc, Count}
                            end
                    end
                end,
                {[], 0},
                Sorted
            ),

            Changes = case maps:get(descending, Opts, false) of
                true -> FilteredChanges;
                false -> lists:reverse(FilteredChanges)
            end,

            LastHlc = case FilteredChanges of
                [] -> case Since of first -> barrel_hlc:min(); _ -> Since end;
                _ -> maps:get(hlc, hd(FilteredChanges))
            end,

            {ok, Changes, LastHlc}
    end.

%% @private Full scan with filtering (original implementation)
get_changes_full_scan(StoreRef, DbName, Since, Opts) ->
    Limit = maps:get(limit, Opts, infinity),
    DocIds = maps:get(doc_ids, Opts, undefined),
    Style = maps:get(style, Opts, all_docs),
    PathPatterns = maps:get(paths, Opts, undefined),
    QuerySpec = maps:get(query, Opts, undefined),

    %% If path filter specified, prepare a match trie for efficient matching
    PathMatcher = case PathPatterns of
        undefined ->
            undefined;
        Patterns when is_list(Patterns) ->
            Trie = match_trie:new(public),
            lists:foreach(fun(P) -> match_trie:insert(Trie, P) end, Patterns),
            Trie
    end,

    %% If query filter specified, compile it
    CompiledQuery = compile_query(QuerySpec),

    FoldFun = fun(Change, {Count, Changes}) ->
        %% Check doc_ids filter
        IncludeByDocId = case DocIds of
            undefined -> true;
            Ids when is_list(Ids) -> lists:member(maps:get(id, Change), Ids)
        end,

        %% Check path patterns filter
        IncludeByPath = case PathMatcher of
            undefined ->
                true;
            MatchTrie ->
                case maps:get(doc, Change, undefined) of
                    undefined ->
                        %% No doc body, can't match paths - skip
                        false;
                    Doc when is_map(Doc) ->
                        %% Extract topics from document and check for matches
                        DocPaths = barrel_ars:analyze(Doc),
                        Topics = barrel_ars:paths_to_topics(DocPaths),
                        matches_any_pattern(MatchTrie, Topics);
                    _ ->
                        false
                end
        end,

        %% Check query filter
        IncludeByQuery = maybe_match_query(CompiledQuery, Change),

        case IncludeByDocId andalso IncludeByPath andalso IncludeByQuery of
            false ->
                {ok, {Count, Changes}};
            true ->
                FilteredChange = apply_style(Style, Change),
                NewCount = Count + 1,
                NewChanges = [FilteredChange | Changes],
                case Limit of
                    infinity ->
                        {ok, {NewCount, NewChanges}};
                    N when NewCount >= N ->
                        {stop, {NewCount, NewChanges}};
                    _ ->
                        {ok, {NewCount, NewChanges}}
                end
        end
    end,

    %% Use long_scan optimization for full unbounded scans
    FoldChanges = case {Since, Limit} of
        {first, infinity} -> fun fold_changes_long_scan/5;
        _ -> fun fold_changes/5
    end,
    {ok, {_Count, RevChanges}, LastHlc} = FoldChanges(StoreRef, DbName, Since, FoldFun, {0, []}),

    %% Cleanup the trie if we created one
    case PathMatcher of
        undefined -> ok;
        CleanupTrie -> match_trie:delete(CleanupTrie)
    end,

    Changes = case maps:get(descending, Opts, false) of
        true -> RevChanges;
        false -> lists:reverse(RevChanges)
    end,

    {ok, Changes, LastHlc}.

%% @private Compile query spec if provided
compile_query(undefined) -> undefined;
compile_query(QuerySpec) when is_map(QuerySpec) ->
    case barrel_query:compile(QuerySpec) of
        {ok, Plan} -> Plan;
        {error, _} -> undefined
    end.

%% @private Check if change matches query
maybe_match_query(undefined, _Change) -> true;
maybe_match_query(QueryPlan, Change) ->
    case maps:get(doc, Change, undefined) of
        undefined -> false;
        Doc when is_map(Doc) -> barrel_query:match(QueryPlan, Doc);
        _ -> false
    end.

%% @private Apply style filter to change
apply_style(main_only, Change) ->
    case maps:get(changes, Change, undefined) of
        undefined ->
            %% Path index changes don't have changes list, create one from rev
            Rev = maps:get(rev, Change, <<>>),
            Change#{changes => [#{rev => Rev}]};
        [_ | _] = Changes ->
            Change#{changes => [hd(Changes)]}
    end;
apply_style(all_docs, Change) ->
    case maps:get(changes, Change, undefined) of
        undefined ->
            %% Path index changes don't have changes list, create one from rev
            Rev = maps:get(rev, Change, <<>>),
            Change#{changes => [#{rev => Rev}]};
        _ ->
            Change
    end.

%% @private Deduplicate changes by doc ID, keeping latest HLC
dedupe_changes_by_id(Changes) ->
    %% Build map of doc_id -> change (keeping latest by HLC)
    ById = lists:foldl(
        fun(Change, Acc) ->
            DocId = maps:get(id, Change),
            case maps:get(DocId, Acc, undefined) of
                undefined ->
                    Acc#{DocId => Change};
                Existing ->
                    ExistingHlc = maps:get(hlc, Existing),
                    ChangeHlc = maps:get(hlc, Change),
                    case barrel_hlc:compare(ChangeHlc, ExistingHlc) of
                        gt -> Acc#{DocId => Change};
                        _ -> Acc
                    end
            end
        end,
        #{},
        Changes
    ),
    maps:values(ById).

%% @private Check if any topic matches any pattern in the trie
matches_any_pattern(_Trie, []) ->
    false;
matches_any_pattern(Trie, [Topic | Rest]) ->
    case match_trie:match(Trie, Topic) of
        [] -> matches_any_pattern(Trie, Rest);
        [_ | _] -> true
    end.

%% @doc Get the last sequence (opaque encoded HLC) for a database
%% The sequence is an opaque binary that can be used for ordering.
-spec get_last_seq(barrel_store_rocksdb:db_ref(), db_name()) -> binary().
get_last_seq(StoreRef, DbName) ->
    Hlc = get_last_hlc(StoreRef, DbName),
    barrel_hlc:encode(Hlc).

%% @doc Get the last HLC timestamp for a database
%% First tries the fast path (metadata key), falls back to reverse iteration
-spec get_last_hlc(barrel_store_rocksdb:db_ref(), db_name()) -> barrel_hlc:timestamp().
get_last_hlc(StoreRef, DbName) ->
    %% Try fast path: read from metadata key (O(1))
    LastHlcKey = barrel_store_keys:db_last_hlc(DbName),
    case barrel_store_rocksdb:get(StoreRef, LastHlcKey) of
        {ok, EncodedHlc} ->
            barrel_hlc:decode(EncodedHlc);
        not_found ->
            %% Fallback for databases created before this optimization
            get_last_hlc_slow(StoreRef, DbName)
    end.

%% @private Get last HLC by reverse iteration (slow path, O(n) worst case)
get_last_hlc_slow(StoreRef, DbName) ->
    StartKey = barrel_store_keys:doc_hlc_prefix(DbName),
    EndKey = barrel_store_keys:doc_hlc_end(DbName),

    barrel_store_rocksdb:fold_range_reverse(
        StoreRef, StartKey, EndKey,
        fun(Key, _Value, _Acc) ->
            Hlc = barrel_store_keys:decode_hlc_key(DbName, Key),
            %% Stop immediately after finding the first (last in order) entry
            {stop, Hlc}
        end,
        barrel_hlc:min()
    ).

%% @doc Count changes since a given HLC timestamp (exclusive)
-spec count_changes_since(barrel_store_rocksdb:db_ref(), db_name(),
                          barrel_hlc:timestamp()) -> non_neg_integer().
count_changes_since(StoreRef, DbName, Since) ->
    StartKey = barrel_store_keys:doc_hlc(DbName, Since),
    EndKey = barrel_store_keys:doc_hlc_end(DbName),

    barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey,
        fun(Key, _Value, Count) ->
            ChangeHlc = barrel_store_keys:decode_hlc_key(DbName, Key),
            %% Skip if we're at the exact Since HLC (exclusive)
            case barrel_hlc:equal(ChangeHlc, Since) of
                true -> {ok, Count};
                false -> {ok, Count + 1}
            end
        end,
        0
    ).

%%====================================================================
%% Internal API - for barrel_db_writer
%%====================================================================

%% @doc Write a change entry for a document
-spec write_change(barrel_store_rocksdb:db_ref(), db_name(),
                   barrel_hlc:timestamp(), doc_info()) -> ok.
write_change(StoreRef, DbName, Hlc, DocInfo) ->
    Ops = write_change_ops(DbName, Hlc, DocInfo),
    barrel_store_rocksdb:write_batch(StoreRef, Ops).

%% @doc Return batch operation to write a change entry.
%% Use this to combine with other operations in a single write_batch.
%% Also updates the last_hlc metadata for efficient get_last_seq lookups.
-spec write_change_ops(db_name(), barrel_hlc:timestamp(), doc_info()) ->
    [{put, binary(), binary()}].
write_change_ops(DbName, Hlc, DocInfo) ->
    Key = barrel_store_keys:doc_hlc(DbName, Hlc),
    Value = encode_change(DocInfo),
    %% Also update last_hlc metadata for O(1) get_last_seq
    LastHlcKey = barrel_store_keys:db_last_hlc(DbName),
    LastHlcValue = barrel_hlc:encode(Hlc),
    [{put, Key, Value}, {put, LastHlcKey, LastHlcValue}].

%% @doc Delete an old HLC entry (when document is updated)
-spec delete_old_change(barrel_store_rocksdb:db_ref(), db_name(),
                        barrel_hlc:timestamp(), docid()) -> ok.
delete_old_change(StoreRef, DbName, OldHlc, _DocId) ->
    Key = barrel_store_keys:doc_hlc(DbName, OldHlc),
    barrel_store_rocksdb:delete(StoreRef, Key).

%%====================================================================
%% Internal Functions
%%====================================================================

encode_change(DocInfo) ->
    term_to_binary(DocInfo).

decode_change(Value, Hlc) ->
    DocInfo = binary_to_term(Value),
    Rev = maps:get(rev, DocInfo),
    Deleted = maps:get(deleted, DocInfo, false),

    ConflictRevs = get_conflict_revs(DocInfo),
    AllRevs = [#{rev => Rev} | [#{rev => R} || R <- ConflictRevs]],

    Change = #{
        id => maps:get(id, DocInfo),
        hlc => Hlc,
        rev => Rev,
        changes => AllRevs
    },

    %% Include document body if present
    Change1 = case maps:get(doc, DocInfo, undefined) of
        undefined -> Change;
        Doc -> Change#{doc => Doc}
    end,

    case Deleted of
        true -> Change1#{deleted => true};
        false -> Change1
    end.

get_conflict_revs(#{revtree := RevTree}) when is_map(RevTree) ->
    case barrel_revtree:conflicts(RevTree) of
        [] -> [];
        Conflicts -> [maps:get(id, C) || C <- Conflicts]
    end;
get_conflict_revs(_) ->
    [].

%%====================================================================
%% Path-Indexed Changes API
%%====================================================================

%% @doc Generate operations to index a change by all its paths.
%% Creates entries under path_hlc/{db}/{topic}/{hlc} for each path and its prefixes.
%% This enables efficient filtered queries by scanning specific path indexes.
-spec write_path_index_ops(db_name(), barrel_hlc:timestamp(), doc_info()) ->
    [{put, binary(), binary()}].
write_path_index_ops(DbName, Hlc, DocInfo) ->
    #{id := DocId, rev := Rev, deleted := Deleted} = DocInfo,

    %% Extract topics from document paths
    Topics = case Deleted of
        true ->
            %% For deleted docs, just use the doc ID as a topic
            [DocId];
        false ->
            Doc = maps:get(doc, DocInfo, #{}),
            case Doc of
                #{} ->
                    Paths = barrel_ars:analyze(Doc),
                    barrel_ars:paths_to_topics(Paths);
                _ ->
                    [DocId]
            end
    end,

    %% Create index entry value: {doc_id, rev, deleted}
    Value = term_to_binary({DocId, Rev, Deleted}),

    %% Create index entry for each topic and all its prefixes
    AllPrefixes = lists:usort(lists:flatmap(fun topic_prefixes/1, Topics)),
    [{put, barrel_store_keys:path_hlc(DbName, Prefix, Hlc), Value}
     || Prefix <- AllPrefixes].

%% @doc Generate operations to update path index (remove old entries + add new).
%% Used when a document is updated to maintain a current path index without stale entries.
-spec update_path_index_ops(db_name(), barrel_hlc:timestamp(), doc_info(),
                            barrel_hlc:timestamp() | undefined, map() | undefined) ->
    [{put | delete, binary(), binary()}].
update_path_index_ops(DbName, NewHlc, NewDocInfo, OldHlc, OldDoc) ->
    %% Generate new path entries
    NewOps = write_path_index_ops(DbName, NewHlc, NewDocInfo),

    %% Generate delete ops for old entries if document existed before
    DeleteOps = case {OldHlc, OldDoc} of
        {undefined, _} ->
            [];
        {_, undefined} ->
            [];
        {_, _} ->
            %% Extract old topics and delete their path_hlc entries
            OldPaths = barrel_ars:analyze(OldDoc),
            OldTopics = barrel_ars:paths_to_topics(OldPaths),
            OldPrefixes = lists:usort(lists:flatmap(fun topic_prefixes/1, OldTopics)),
            [{delete, barrel_store_keys:path_hlc(DbName, P, OldHlc)}
             || P <- OldPrefixes]
    end,

    DeleteOps ++ NewOps.

%% @doc Get changes for a specific path pattern since HLC.
%% Scans the path_hlc index directly for efficient filtered queries.
-spec get_changes_by_path(barrel_store_rocksdb:db_ref(), db_name(),
                          binary(), barrel_hlc:timestamp() | first, map()) ->
    {ok, [change()], barrel_hlc:timestamp()}.
get_changes_by_path(StoreRef, DbName, PathPattern, Since, Opts) ->
    Limit = maps:get(limit, Opts, infinity),

    %% Parse the path pattern
    case parse_path_pattern(PathPattern) of
        {exact, Topic} ->
            %% Exact match: scan path_hlc/{db}/{topic}/{since}..{end}
            scan_path_hlc(StoreRef, DbName, Topic, Since, Limit);
        {prefix, TopicPrefix} ->
            %% Prefix match (ends with #): scan all topics with prefix
            %% For prefix match, we scan the prefix and filter by HLC
            scan_path_hlc_prefix(StoreRef, DbName, TopicPrefix, Since, Limit)
    end.

%% @private Parse path pattern to determine scan type
%% - Exact: "users/123/name" -> {exact, <<"users/123/name">>}
%% - Prefix: "users/#" -> {prefix, <<"users/">>}
parse_path_pattern(Pattern) ->
    case binary:last(Pattern) of
        $# ->
            %% Remove # and trailing /# to get prefix
            Len = byte_size(Pattern) - 1,
            <<Prefix:Len/binary, _/binary>> = Pattern,
            %% Also remove trailing / if present
            Prefix2 = case binary:last(Prefix) of
                $/ -> binary:part(Prefix, 0, byte_size(Prefix) - 1);
                _ -> Prefix
            end,
            {prefix, Prefix2};
        _ ->
            {exact, Pattern}
    end.

%% @private Scan path_hlc index for exact topic
scan_path_hlc(StoreRef, DbName, Topic, Since, Limit) ->
    {StartHlc, StartKey} = case Since of
        first ->
            Min = barrel_hlc:min(),
            {Min, barrel_store_keys:path_hlc(DbName, Topic, Min)};
        SinceHlc ->
            {SinceHlc, barrel_store_keys:path_hlc(DbName, Topic, SinceHlc)}
    end,
    EndKey = barrel_store_keys:path_hlc_end(DbName, Topic),

    {LastHlc, _Count, Changes} = barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey,
        fun(Key, Value, {CurrentHlc, Count, Acc}) ->
            case Limit =/= infinity andalso Count >= Limit of
                true ->
                    {stop, {CurrentHlc, Count, Acc}};
                false ->
                    {_KeyTopic, ChangeHlc} = barrel_store_keys:decode_path_hlc_key(DbName, Key),
                    %% Skip if we're at the exact Since HLC (exclusive)
                    case Since =/= first andalso barrel_hlc:equal(ChangeHlc, Since) of
                        true ->
                            {ok, {CurrentHlc, Count, Acc}};
                        false ->
                            {DocId, Rev, Deleted} = binary_to_term(Value),
                            Change = #{
                                id => DocId,
                                rev => Rev,
                                hlc => ChangeHlc,
                                deleted => Deleted
                            },
                            {ok, {ChangeHlc, Count + 1, [Change | Acc]}}
                    end
            end
        end,
        {StartHlc, 0, []}
    ),

    {ok, lists:reverse(Changes), LastHlc}.

%% @private Scan path_hlc index with prefix (for # wildcard)
%% This scans all topics that start with the prefix
scan_path_hlc_prefix(StoreRef, DbName, TopicPrefix, Since, Limit) ->
    %% For prefix scan, we need to scan all keys with the topic prefix
    %% and filter by HLC afterward
    SinceHlc = case Since of
        first -> barrel_hlc:min();
        Hlc -> Hlc
    end,

    %% We'll use the topic prefix as the scan range
    %% Note: This is less efficient than exact topic scan, but handles # wildcard
    StartKey = barrel_store_keys:path_hlc_prefix(DbName, TopicPrefix),
    %% End at the next byte after prefix
    EndKey = <<StartKey/binary, 16#FF>>,

    {LastHlc, _Count, Changes} = barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey,
        fun(Key, Value, {CurrentHlc, Count, Acc}) ->
            case Limit =/= infinity andalso Count >= Limit of
                true ->
                    {stop, {CurrentHlc, Count, Acc}};
                false ->
                    {_KeyTopic, ChangeHlc} = barrel_store_keys:decode_path_hlc_key(DbName, Key),
                    %% Filter by HLC (exclusive of Since)
                    case barrel_hlc:compare(ChangeHlc, SinceHlc) of
                        lt -> {ok, {CurrentHlc, Count, Acc}};
                        eq when Since =/= first -> {ok, {CurrentHlc, Count, Acc}};
                        _ ->
                            {DocId, Rev, Deleted} = binary_to_term(Value),
                            Change = #{
                                id => DocId,
                                rev => Rev,
                                hlc => ChangeHlc,
                                deleted => Deleted
                            },
                            {ok, {ChangeHlc, Count + 1, [Change | Acc]}}
                    end
            end
        end,
        {SinceHlc, 0, []}
    ),

    {ok, lists:reverse(Changes), LastHlc}.

%% @private Generate topic prefixes for hierarchical matching
%% "users/123/name" -> ["users", "users/123", "users/123/name"]
-spec topic_prefixes(binary()) -> [binary()].
topic_prefixes(Topic) ->
    Parts = binary:split(Topic, <<"/">>, [global]),
    build_prefixes(Parts, <<>>, []).

build_prefixes([], _Current, Acc) ->
    lists:reverse(Acc);
build_prefixes([Part | Rest], <<>>, Acc) ->
    build_prefixes(Rest, Part, [Part | Acc]);
build_prefixes([Part | Rest], Current, Acc) ->
    New = <<Current/binary, "/", Part/binary>>,
    build_prefixes(Rest, New, [New | Acc]).
