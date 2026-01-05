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
    fold_changes_compact/5,
    get_changes/4,
    get_changes_chunked/4,  %% Returns continuation token for pagination
    get_last_seq/2,  %% Returns opaque sequence (encoded HLC binary)
    get_last_hlc/2,  %% Returns decoded HLC timestamp
    count_changes_since/3,
    has_changes_since/3  %% Fast O(1) check using buckets
]).

%% Internal - for use by barrel_db_writer
-export([
    write_change/4,
    write_change_ops/3,
    update_change_bucket_ops/3,
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

-type compact_change() :: {docid(), barrel_hlc:timestamp(), binary(), boolean(), non_neg_integer()}.
-type compact_fold_fun() :: fun((compact_change(), Acc :: term()) ->
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

-type continuation_info() :: #{
    last_hlc := barrel_hlc:timestamp(),
    has_more := boolean(),
    continuation => binary()  %% Only present when has_more=true
}.

-export_type([changes_result/0, fold_fun/0, compact_change/0, compact_fold_fun/0,
              changes_opts/0, continuation_info/0]).

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

%% @doc Fold over changes with compact tuple format for efficiency.
%% Callback receives {DocId, Hlc, Rev, Deleted, NumConflicts} tuples.
%% Use for internal iteration; convert to maps at API boundary.
-spec fold_changes_compact(barrel_store_rocksdb:db_ref(), db_name(),
                           barrel_hlc:timestamp() | first, compact_fold_fun(), term()) ->
    {ok, term(), barrel_hlc:timestamp()}.
fold_changes_compact(StoreRef, DbName, Since, Fun, Acc) ->
    fold_changes_compact_internal(StoreRef, DbName, Since, Fun, Acc, long_scan).

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

%% @private Internal fold with compact tuple format
fold_changes_compact_internal(StoreRef, DbName, Since, Fun, Acc, ScanMode) ->
    {StartHlc, StartKey} = case Since of
        first ->
            Min = barrel_hlc:min(),
            {Min, barrel_store_keys:doc_hlc(DbName, Min)};
        SinceHlc ->
            {SinceHlc, barrel_store_keys:doc_hlc(DbName, SinceHlc)}
    end,
    EndKey = barrel_store_keys:doc_hlc_end(DbName),

    FoldFun = fun(Key, Value, {CurrentHlc, AccIn}) ->
        ChangeHlc = barrel_store_keys:decode_hlc_key(DbName, Key),
        case Since =/= first andalso barrel_hlc:equal(ChangeHlc, Since) of
            true ->
                {ok, {CurrentHlc, AccIn}};
            false ->
                CompactChange = decode_change_compact(Value, ChangeHlc),
                case Fun(CompactChange, AccIn) of
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

%% @doc Get changes with continuation support for pagination.
%% Accepts either an HLC timestamp, 'first', or a continuation token (binary).
%% Returns changes along with continuation info for fetching the next batch.
%%
%% Example usage:
%% ```
%% %% First request
%% {ok, Changes1, Info1} = get_changes_chunked(Store, Db, first, #{limit => 100}),
%% %% Check if more changes available
%% case maps:get(has_more, Info1) of
%%     true ->
%%         Continuation = maps:get(continuation, Info1),
%%         {ok, Changes2, Info2} = get_changes_chunked(Store, Db, Continuation, #{limit => 100});
%%     false ->
%%         done
%% end.
%% '''
-spec get_changes_chunked(barrel_store_rocksdb:db_ref(), db_name(),
                          barrel_hlc:timestamp() | first | binary(), changes_opts()) ->
    {ok, [change()], continuation_info()}.
get_changes_chunked(StoreRef, DbName, ContinuationOrSince, Opts) ->
    %% Decode continuation token if binary
    Since = case ContinuationOrSince of
        first -> first;
        Bin when is_binary(Bin), byte_size(Bin) =:= 12 ->
            %% This is an encoded HLC (continuation token)
            barrel_hlc:decode(Bin);
        Hlc when is_tuple(Hlc) ->
            %% Already an HLC timestamp
            Hlc
    end,

    %% Get one extra to determine if there are more
    Limit = maps:get(limit, Opts, 100),
    OptsWithExtra = Opts#{limit => Limit + 1},

    {ok, AllChanges, _LastHlc} = get_changes(StoreRef, DbName, Since, OptsWithExtra),

    %% Check if we got the extra one (meaning there are more)
    {Changes, HasMore} = case length(AllChanges) > Limit of
        true ->
            {lists:sublist(AllChanges, Limit), true};
        false ->
            {AllChanges, false}
    end,

    %% Build continuation info
    ResultLastHlc = case Changes of
        [] ->
            case Since of
                first -> barrel_hlc:min();
                _ -> Since
            end;
        _ ->
            LastChange = lists:last(Changes),
            maps:get(hlc, LastChange)
    end,

    ContinuationInfo = case HasMore of
        true ->
            #{
                last_hlc => ResultLastHlc,
                has_more => true,
                continuation => barrel_hlc:encode(ResultLastHlc)
            };
        false ->
            #{
                last_hlc => ResultLastHlc,
                has_more => false
            }
    end,

    {ok, Changes, ContinuationInfo}.

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

%% @private Full scan with filtering
get_changes_full_scan(StoreRef, DbName, Since, Opts) ->
    DocIds = maps:get(doc_ids, Opts, undefined),
    PathPatterns = maps:get(paths, Opts, undefined),
    QuerySpec = maps:get(query, Opts, undefined),

    %% Fast path: no filters - use compact format for efficiency
    case {DocIds, PathPatterns, QuerySpec} of
        {undefined, undefined, undefined} ->
            get_changes_fast(StoreRef, DbName, Since, Opts);
        _ ->
            get_changes_filtered(StoreRef, DbName, Since, Opts)
    end.

%% @private Fast path using compact format (no filtering needed)
get_changes_fast(StoreRef, DbName, Since, Opts) ->
    Limit = maps:get(limit, Opts, infinity),
    Style = maps:get(style, Opts, all_docs),
    StartHlc = case Since of first -> barrel_hlc:min(); H -> H end,

    %% Accumulator: {Count, LastProcessedHlc, Changes}
    FoldFun = fun({DocId, Hlc, Rev, Deleted, NumConflicts}, {Count, _LastHlc, Acc}) ->
        Change = compact_to_change(DocId, Hlc, Rev, Deleted, NumConflicts, Style),
        NewCount = Count + 1,
        NewAcc = {NewCount, Hlc, [Change | Acc]},
        case Limit =/= infinity andalso NewCount >= Limit of
            true -> {stop, NewAcc};
            false -> {ok, NewAcc}
        end
    end,

    {ok, {_Count, LastHlc, RevChanges}, _FoldHlc} = fold_changes_compact(
        StoreRef, DbName, Since, FoldFun, {0, StartHlc, []}),

    Changes = case maps:get(descending, Opts, false) of
        true -> RevChanges;
        false -> lists:reverse(RevChanges)
    end,
    {ok, Changes, LastHlc}.

%% @private Convert compact tuple to change map
compact_to_change(DocId, Hlc, Rev, Deleted, NumConflicts, _Style) ->
    Change = #{
        id => DocId,
        hlc => Hlc,
        rev => Rev,
        changes => [#{rev => Rev}],
        num_conflicts => NumConflicts
    },
    case Deleted of
        true -> Change#{deleted => true};
        false -> Change
    end.

%% Batch size for chunked doc fetching
-define(DOC_FETCH_BATCH_SIZE, 100).

%% @private Filtered scan (needs full change format)
%% Uses chunked batch processing for efficient doc fetching.
get_changes_filtered(StoreRef, DbName, Since, Opts) ->
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

    %% Do we need doc body for filtering?
    NeedsDoc = PathMatcher =/= undefined orelse CompiledQuery =/= undefined,

    %% Use chunked processing with batch doc fetching for efficiency
    case NeedsDoc of
        true ->
            %% Chunked batch processing when docs are needed
            get_changes_filtered_chunked(StoreRef, DbName, Since, Opts,
                                          DocIds, Style, PathMatcher, CompiledQuery);
        false ->
            %% Simple fold when no doc fetching needed (doc_ids filter only)
            get_changes_filtered_simple(StoreRef, DbName, Since, Opts,
                                         DocIds, Style)
    end.

%% @private Chunked processing with batch doc fetching
get_changes_filtered_chunked(StoreRef, DbName, Since, Opts,
                              DocIds, Style, PathMatcher, CompiledQuery) ->
    Limit = maps:get(limit, Opts, infinity),

    %% Collect changes in chunks, batch fetch docs, apply filters
    FoldFun = fun(Change, {Chunk, ChunkSize, Results, ResultCount, _LastHlc}) ->
        DocId = maps:get(id, Change),

        %% Pre-filter by doc_ids before adding to chunk
        IncludeByDocId = case DocIds of
            undefined -> true;
            Ids -> lists:member(DocId, Ids)
        end,

        case IncludeByDocId of
            false ->
                %% Skip this change entirely
                {ok, {Chunk, ChunkSize, Results, ResultCount, maps:get(hlc, Change)}};
            true ->
                NewChunk = [{DocId, Change} | Chunk],
                NewChunkSize = ChunkSize + 1,
                NewLastHlc = maps:get(hlc, Change),

                %% Process chunk when it reaches batch size
                case NewChunkSize >= ?DOC_FETCH_BATCH_SIZE of
                    true ->
                        {NewResults, NewResultCount, Done} =
                            process_change_chunk(StoreRef, DbName, lists:reverse(NewChunk),
                                                  PathMatcher, CompiledQuery, Style,
                                                  Results, ResultCount, Limit),
                        case Done of
                            true ->
                                {stop, {[], 0, NewResults, NewResultCount, NewLastHlc}};
                            false ->
                                {ok, {[], 0, NewResults, NewResultCount, NewLastHlc}}
                        end;
                    false ->
                        {ok, {NewChunk, NewChunkSize, Results, ResultCount, NewLastHlc}}
                end
        end
    end,

    StartHlc = case Since of first -> barrel_hlc:min(); H -> H end,

    %% Use long_scan optimization for full unbounded scans
    FoldChanges = case {Since, Limit} of
        {first, infinity} -> fun fold_changes_long_scan/5;
        _ -> fun fold_changes/5
    end,

    {ok, {FinalChunk, _, RevResults, FinalCount, LastHlc}, _FoldHlc} =
        FoldChanges(StoreRef, DbName, Since, FoldFun, {[], 0, [], 0, StartHlc}),

    %% Process any remaining chunk
    {AllResults, _FinalResultCount, _} = case FinalChunk of
        [] ->
            {RevResults, FinalCount, false};
        _ ->
            process_change_chunk(StoreRef, DbName, lists:reverse(FinalChunk),
                                  PathMatcher, CompiledQuery, Style,
                                  RevResults, FinalCount, Limit)
    end,

    %% Cleanup the trie if we created one
    case PathMatcher of
        undefined -> ok;
        CleanupTrie -> match_trie:delete(CleanupTrie)
    end,

    Changes = case maps:get(descending, Opts, false) of
        true -> AllResults;
        false -> lists:reverse(AllResults)
    end,

    {ok, Changes, LastHlc}.

%% @private Process a chunk of changes with batch doc fetching
process_change_chunk(StoreRef, DbName, DocChangePairs,
                      PathMatcher, CompiledQuery, Style,
                      Results, ResultCount, Limit) ->
    %% Batch fetch doc bodies for all changes in chunk
    ChangesWithDocs = batch_fetch_doc_bodies(StoreRef, DbName, DocChangePairs),

    %% Apply filters to each change
    lists:foldl(
        fun(ChangeWithDoc, {Acc, Count, Done}) ->
            case Done of
                true ->
                    {Acc, Count, true};
                false ->
                    %% Check path patterns filter
                    IncludeByPath = case PathMatcher of
                        undefined ->
                            true;
                        MatchTrie ->
                            case maps:get(doc, ChangeWithDoc, undefined) of
                                undefined ->
                                    false;
                                DocBody when is_map(DocBody) ->
                                    DocPaths = barrel_ars:analyze(DocBody),
                                    Topics = barrel_ars:paths_to_topics(DocPaths),
                                    matches_any_pattern(MatchTrie, Topics);
                                _ ->
                                    false
                            end
                    end,

                    %% Check query filter
                    IncludeByQuery = maybe_match_query(CompiledQuery, ChangeWithDoc),

                    case IncludeByPath andalso IncludeByQuery of
                        false ->
                            {Acc, Count, false};
                        true ->
                            FilteredChange = apply_style(Style, ChangeWithDoc),
                            NewCount = Count + 1,
                            NewAcc = [FilteredChange | Acc],
                            IsDone = Limit =/= infinity andalso NewCount >= Limit,
                            {NewAcc, NewCount, IsDone}
                    end
            end
        end,
        {Results, ResultCount, false},
        ChangesWithDocs
    ).

%% @private Simple filtered scan (doc_ids filter only, no doc fetching)
get_changes_filtered_simple(StoreRef, DbName, Since, Opts, DocIds, Style) ->
    Limit = maps:get(limit, Opts, infinity),

    FoldFun = fun(Change, {Count, Changes}) ->
        DocId = maps:get(id, Change),
        IncludeByDocId = case DocIds of
            undefined -> true;
            Ids -> lists:member(DocId, Ids)
        end,

        case IncludeByDocId of
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

    FoldChanges = case {Since, Limit} of
        {first, infinity} -> fun fold_changes_long_scan/5;
        _ -> fun fold_changes/5
    end,
    {ok, {_Count, RevChanges}, LastHlc} = FoldChanges(StoreRef, DbName, Since, FoldFun, {0, []}),

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

%% Bucket granularity: 1 minute
-define(BUCKET_GRANULARITY_SECS, 60).
%% Number of recent buckets to check
-define(BUCKET_CHECK_COUNT, 5).

%% @doc Fast O(1) check if there are changes since a given HLC.
%% Uses time-bucketed hints to avoid scanning the change log.
%% Returns true if there might be changes, false if definitely no changes.
%% Note: May return true even if no changes (false positive OK, false negative not OK).
-spec has_changes_since(barrel_store_rocksdb:db_ref(), db_name(),
                        barrel_hlc:timestamp()) -> boolean().
has_changes_since(StoreRef, DbName, Since) ->
    %% Get current bucket and check recent buckets
    NowSecs = erlang:system_time(second),
    CurrentBucket = NowSecs div ?BUCKET_GRANULARITY_SECS,

    %% Check the last N buckets for any max_hlc > Since
    check_recent_buckets(StoreRef, DbName, Since, CurrentBucket, ?BUCKET_CHECK_COUNT).

%% @private Check recent buckets for changes
check_recent_buckets(_StoreRef, _DbName, _Since, _Bucket, 0) ->
    %% No buckets had changes > Since, but bucket data might be stale
    %% Fall back to true to be safe (will do full scan)
    true;
check_recent_buckets(StoreRef, DbName, Since, Bucket, Count) when Bucket >= 0 ->
    BucketKey = barrel_store_keys:change_bucket(DbName, Bucket),
    case barrel_store_rocksdb:get(StoreRef, BucketKey) of
        {ok, <<_MinBin:12/binary, MaxBin:12/binary, BucketCount:32>>} ->
            MaxHlc = barrel_hlc:decode(MaxBin),
            case BucketCount > 0 andalso barrel_hlc:compare(MaxHlc, Since) =:= gt of
                true -> true;  %% Found changes after Since
                false -> check_recent_buckets(StoreRef, DbName, Since, Bucket - 1, Count - 1)
            end;
        not_found ->
            %% No bucket data, check older bucket
            check_recent_buckets(StoreRef, DbName, Since, Bucket - 1, Count - 1)
    end;
check_recent_buckets(_StoreRef, _DbName, _Since, _Bucket, _Count) ->
    %% Bucket < 0, shouldn't happen in practice
    true.

%% @private Batch fetch doc bodies using multi_get for efficiency.
%% Takes a list of {DocId, Change} pairs and returns changes with doc bodies added.
%% Uses two multi_get calls: first for doc_current (to get revs), then for doc_body.
-spec batch_fetch_doc_bodies(barrel_store_rocksdb:db_ref(), db_name(),
                              [{docid(), change()}]) -> [change()].
batch_fetch_doc_bodies(_StoreRef, _DbName, []) ->
    [];
batch_fetch_doc_bodies(StoreRef, DbName, DocChangePairs) ->
    %% Step 1: Build keys for doc_current lookup
    DocIds = [DocId || {DocId, _Change} <- DocChangePairs],
    CurrentKeys = [barrel_store_keys:doc_current(DbName, DocId) || DocId <- DocIds],

    %% Step 2: Batch fetch doc_current entries
    CurrentResults = barrel_store_rocksdb:multi_get(StoreRef, CurrentKeys),

    %% Step 3: Parse results and build doc_body keys for non-deleted docs
    {BodyKeyMap, DeletedSet} = lists:foldl(
        fun({DocId, Result}, {BodyKeys, Deleted}) ->
            case Result of
                {ok, CurrentBin} ->
                    {Rev, IsDeleted, _Hlc} = binary_to_term(CurrentBin),
                    case IsDeleted of
                        true ->
                            {BodyKeys, sets:add_element(DocId, Deleted)};
                        false ->
                            BodyKey = barrel_store_keys:doc_body(DbName, DocId, Rev),
                            {maps:put(DocId, BodyKey, BodyKeys), Deleted}
                    end;
                not_found ->
                    {BodyKeys, Deleted};
                {error, _} ->
                    {BodyKeys, Deleted}
            end
        end,
        {#{}, sets:new()},
        lists:zip(DocIds, CurrentResults)
    ),

    %% Step 4: Batch fetch doc bodies
    BodyKeys = maps:values(BodyKeyMap),
    BodyDocIds = maps:keys(BodyKeyMap),
    DocBodies = case BodyKeys of
        [] ->
            #{};
        _ ->
            BodyResults = barrel_store_rocksdb:multi_get(StoreRef, BodyKeys),
            lists:foldl(
                fun({DocId, Result}, Acc) ->
                    case Result of
                        {ok, CborBin} ->
                            DocBody = barrel_docdb_codec_cbor:decode(CborBin),
                            maps:put(DocId, DocBody, Acc);
                        _ ->
                            Acc
                    end
                end,
                #{},
                lists:zip(BodyDocIds, BodyResults)
            )
    end,

    %% Step 5: Merge doc bodies back into changes
    [case sets:is_element(DocId, DeletedSet) of
        true ->
            Change;  %% Deleted doc, no body to add
        false ->
            case maps:get(DocId, DocBodies, undefined) of
                undefined -> Change;
                DocBody -> Change#{doc => DocBody}
            end
    end || {DocId, Change} <- DocChangePairs].

%%====================================================================
%% Internal API - for barrel_db_writer
%%====================================================================

%% @doc Write a change entry for a document.
%% Also updates change buckets for idle poll optimization.
-spec write_change(barrel_store_rocksdb:db_ref(), db_name(),
                   barrel_hlc:timestamp(), doc_info()) -> ok.
write_change(StoreRef, DbName, Hlc, DocInfo) ->
    ChangeOps = write_change_ops(DbName, Hlc, DocInfo),
    BucketOps = update_change_bucket_ops(StoreRef, DbName, Hlc),
    barrel_store_rocksdb:write_batch(StoreRef, ChangeOps ++ BucketOps).

%% @doc Return batch operation to write a change entry.
%% Use this to combine with other operations in a single write_batch.
%% Also updates the last_hlc metadata for efficient get_last_seq lookups.
%% Note: Does NOT update change buckets - call write_change/4 for full update.
-spec write_change_ops(db_name(), barrel_hlc:timestamp(), doc_info()) ->
    [{put, binary(), binary()}].
write_change_ops(DbName, Hlc, DocInfo) ->
    Key = barrel_store_keys:doc_hlc(DbName, Hlc),
    Value = encode_change(DocInfo),
    %% Also update last_hlc metadata for O(1) get_last_seq
    LastHlcKey = barrel_store_keys:db_last_hlc(DbName),
    LastHlcValue = barrel_hlc:encode(Hlc),
    [{put, Key, Value}, {put, LastHlcKey, LastHlcValue}].

%% @doc Return batch operation to update change bucket.
%% Buckets store {MinHlc, MaxHlc, Count} in compact binary format.
%% Format: <<MinHlc:12/binary, MaxHlc:12/binary, Count:32>>
-spec update_change_bucket_ops(barrel_store_rocksdb:db_ref(), db_name(),
                                barrel_hlc:timestamp()) -> [{put, binary(), binary()}].
update_change_bucket_ops(StoreRef, DbName, Hlc) ->
    NowSecs = erlang:system_time(second),
    BucketTs = NowSecs div ?BUCKET_GRANULARITY_SECS,
    BucketKey = barrel_store_keys:change_bucket(DbName, BucketTs),
    HlcBin = barrel_hlc:encode(Hlc),

    %% Read current bucket value and update
    NewValue = case barrel_store_rocksdb:get(StoreRef, BucketKey) of
        {ok, <<MinBin:12/binary, MaxBin:12/binary, Count:32>>} ->
            MinHlc = barrel_hlc:decode(MinBin),
            MaxHlc = barrel_hlc:decode(MaxBin),
            NewMinBin = case barrel_hlc:compare(Hlc, MinHlc) of
                lt -> HlcBin;
                _ -> MinBin
            end,
            NewMaxBin = case barrel_hlc:compare(Hlc, MaxHlc) of
                gt -> HlcBin;
                _ -> MaxBin
            end,
            <<NewMinBin/binary, NewMaxBin/binary, (Count + 1):32>>;
        not_found ->
            <<HlcBin/binary, HlcBin/binary, 1:32>>
    end,
    [{put, BucketKey, NewValue}].

%% @doc Delete an old HLC entry (when document is updated)
-spec delete_old_change(barrel_store_rocksdb:db_ref(), db_name(),
                        barrel_hlc:timestamp(), docid()) -> ok.
delete_old_change(StoreRef, DbName, OldHlc, _DocId) ->
    Key = barrel_store_keys:doc_hlc(DbName, OldHlc),
    barrel_store_rocksdb:delete(StoreRef, Key).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Encode change to compact binary format.
%% Format: <<DocIdLen:16, DocId/binary, RevLen:16, Rev/binary, Deleted:8, NumConflicts:16, HasDoc:8, [DocCbor/binary]>>
%% Stores conflict count for quick check; optionally includes doc body for filtering.
-spec encode_change(doc_info()) -> binary().
encode_change(DocInfo) ->
    DocId = maps:get(id, DocInfo),
    Rev = maps:get(rev, DocInfo),
    Deleted = case maps:get(deleted, DocInfo, false) of true -> 1; false -> 0 end,
    NumConflicts = count_conflicts(DocInfo),
    DocIdLen = byte_size(DocId),
    RevLen = byte_size(Rev),
    Base = <<DocIdLen:16, DocId/binary, RevLen:16, Rev/binary, Deleted:8, NumConflicts:16>>,
    case maps:get(doc, DocInfo, undefined) of
        undefined ->
            <<Base/binary, 0:8>>;
        Doc when is_map(Doc) ->
            DocCbor = barrel_docdb_codec_cbor:encode(Doc),
            <<Base/binary, 1:8, DocCbor/binary>>;
        _ ->
            <<Base/binary, 0:8>>
    end.

count_conflicts(#{revtree := RevTree}) when is_map(RevTree) ->
    length(barrel_revtree:conflicts(RevTree));
count_conflicts(_) ->
    0.

%% @doc Decode change to compact tuple format for efficient iteration.
%% Returns {DocId, Hlc, Rev, Deleted, NumConflicts} - minimal allocation.
-spec decode_change_compact(binary(), barrel_hlc:timestamp()) ->
    {docid(), barrel_hlc:timestamp(), binary(), boolean(), non_neg_integer()}.
decode_change_compact(<<DocIdLen:16, DocId:DocIdLen/binary,
                        RevLen:16, Rev:RevLen/binary,
                        Deleted:8, NumConflicts:16, _Rest/binary>>, Hlc) ->
    {DocId, Hlc, Rev, Deleted =:= 1, NumConflicts}.

%% @doc Extract just DocId from encoded change binary (for cheap dedup).
%% Avoids full decode when only DocId is needed.
-spec decode_docid(binary()) -> docid().
decode_docid(<<DocIdLen:16, DocId:DocIdLen/binary, _Rest/binary>>) ->
    DocId.

%% @doc Decode change to full map format for API responses.
%% Includes doc body if present in the change record.
-spec decode_change(binary(), barrel_hlc:timestamp()) -> change().
decode_change(<<DocIdLen:16, DocId:DocIdLen/binary,
                RevLen:16, Rev:RevLen/binary,
                Deleted:8, NumConflicts:16, HasDoc:8, Rest/binary>>, Hlc) ->
    Change = #{
        id => DocId,
        hlc => Hlc,
        rev => Rev,
        changes => [#{rev => Rev}],
        num_conflicts => NumConflicts
    },
    Change1 = case Deleted =:= 1 of
        true -> Change#{deleted => true};
        false -> Change
    end,
    case HasDoc of
        1 ->
            Doc = barrel_docdb_codec_cbor:decode(Rest),
            Change1#{doc => Doc};
        0 ->
            Change1
    end.


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

    %% Create index entry value using compact binary format (same as change feed)
    %% Format: <<DocIdLen:16, DocId, RevLen:16, Rev, Deleted:8, NumConflicts:16, HasDoc:8>>
    Value = encode_change(#{id => DocId, rev => Rev, deleted => Deleted}),

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

    %% Use fold_range_limit to select read profile based on expected result size
    FoldFun = fun(Key, Value, {CurrentHlc, Count, Acc}) ->
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
                        %% Decode from compact binary format
                        Change = decode_path_hlc_value(Value, ChangeHlc),
                        {ok, {ChangeHlc, Count + 1, [Change | Acc]}}
                end
        end
    end,
    {LastHlc, _Count, Changes} = barrel_store_rocksdb:fold_range_limit(
        StoreRef, StartKey, EndKey, FoldFun, {StartHlc, 0, []}, Limit
    ),

    {ok, lists:reverse(Changes), LastHlc}.

%% @private Scan path_hlc index with prefix (for # wildcard)
%% This scans all topics that start with the prefix.
%% Deduplicates by DocId since a document may have multiple topic prefixes indexed.
scan_path_hlc_prefix(StoreRef, DbName, TopicPrefix, Since, Limit) ->
    %% For prefix scan, we need to scan all keys with the topic prefix
    %% and filter by HLC afterward
    SinceHlc = case Since of
        first -> barrel_hlc:min();
        Hlc -> Hlc
    end,

    %% Use wildcard range keys (without null terminator) to capture all topics
    %% that START with TopicPrefix, e.g. "users" matches "users", "users/123", etc.
    StartKey = barrel_store_keys:path_hlc_wildcard_start(DbName, TopicPrefix),
    EndKey = barrel_store_keys:path_hlc_wildcard_end(DbName, TopicPrefix),

    %% Track seen DocIds to deduplicate (a doc may have multiple topic prefixes)
    %% Use fold_range_limit to select read profile based on expected result size
    FoldFun = fun(Key, Value, {CurrentHlc, Count, Seen, Acc}) ->
        case Limit =/= infinity andalso Count >= Limit of
            true ->
                {stop, {CurrentHlc, Count, Seen, Acc}};
            false ->
                {_KeyTopic, ChangeHlc} = barrel_store_keys:decode_path_hlc_key(DbName, Key),
                %% Filter by HLC (exclusive of Since)
                case barrel_hlc:compare(ChangeHlc, SinceHlc) of
                    lt -> {ok, {CurrentHlc, Count, Seen, Acc}};
                    eq when Since =/= first -> {ok, {CurrentHlc, Count, Seen, Acc}};
                    _ ->
                        %% Extract DocId cheaply for dedup check
                        DocId = decode_docid(Value),
                        case maps:is_key(DocId, Seen) of
                            true ->
                                %% Already seen this doc, skip
                                {ok, {ChangeHlc, Count, Seen, Acc}};
                            false ->
                                %% Decode full change and add
                                Change = decode_path_hlc_value(Value, ChangeHlc),
                                {ok, {ChangeHlc, Count + 1, Seen#{DocId => true}, [Change | Acc]}}
                        end
                end
        end
    end,
    {LastHlc, _Count, _Seen, Changes} = barrel_store_rocksdb:fold_range_limit(
        StoreRef, StartKey, EndKey, FoldFun, {SinceHlc, 0, #{}, []}, Limit
    ),

    {ok, lists:reverse(Changes), LastHlc}.

%% @private Decode path_hlc index value to change map.
%% Uses same compact binary format as change feed.
decode_path_hlc_value(<<DocIdLen:16, DocId:DocIdLen/binary,
                        RevLen:16, Rev:RevLen/binary,
                        Deleted:8, _Rest/binary>>, Hlc) ->
    Change = #{
        id => DocId,
        rev => Rev,
        hlc => Hlc
    },
    case Deleted =:= 1 of
        true -> Change#{deleted => true};
        false -> Change
    end.

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
