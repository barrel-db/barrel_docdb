%%%-------------------------------------------------------------------
%%% @doc Virtual Database (VDB) - Sharded Document Database
%%%
%%% Provides a sharded database layer on top of barrel_docdb.
%%% Documents are distributed across multiple physical databases
%%% (shards) based on consistent hashing of document IDs.
%%%
%%% Example:
%%% ```
%%% %% Create a VDB with 4 shards
%%% ok = barrel_vdb:create(<<"users">>, #{shard_count => 4}).
%%%
%%% %% Put a document (routed to correct shard)
%%% {ok, DocId, Rev} = barrel_vdb:put_doc(<<"users">>, #{
%%%     <<"id">> => <<"user123">>,
%%%     <<"name">> => <<"Alice">>
%%% }).
%%%
%%% %% Get a document (routed to correct shard)
%%% {ok, Doc} = barrel_vdb:get_doc(<<"users">>, <<"user123">>).
%%%
%%% %% Query across all shards (scatter-gather)
%%% {ok, Results} = barrel_vdb:find(<<"users">>, #{
%%%     <<"selector">> => #{<<"active">> => true}
%%% }).
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb).

%% VDB Lifecycle
-export([
    create/2,
    delete/1,
    exists/1,
    list/0,
    info/1
]).

%% Document Operations (routed)
-export([
    put_doc/2,
    put_doc/3,
    get_doc/2,
    get_doc/3,
    delete_doc/2,
    delete_doc/3
]).

%% Bulk Operations
-export([
    bulk_docs/2,
    bulk_docs/3
]).

%% Query Operations (scatter-gather) - Step 4
-export([
    find/2,
    find/3,
    get_changes/2,
    get_changes/3,
    fold_docs/4
]).

%%====================================================================
%% VDB Lifecycle
%%====================================================================

%% @doc Create a new virtual database
%% Creates the shard map and all underlying physical databases
%% If replica_factor > 1 in placement config, sets up replication
%% Broadcasts config to meta database for cross-node sync
-spec create(binary(), map()) -> ok | {error, term()}.
create(VdbName, Opts) when is_binary(VdbName), is_map(Opts) ->
    case barrel_shard_map:create(VdbName, Opts) of
        ok ->
            %% Create all physical shard databases
            case create_shard_dbs(VdbName) of
                ok ->
                    %% Register with VDB registry
                    barrel_vdb_registry:register_vdb(VdbName),
                    %% Setup replication if replica_factor > 1
                    setup_replication_if_needed(VdbName, Opts),
                    %% Broadcast config to meta database for cross-node sync
                    broadcast_config_async(VdbName),
                    ok;
                {error, _} = Err ->
                    %% Rollback shard map on failure
                    _ = barrel_shard_map:delete(VdbName),
                    Err
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Delete a virtual database
%% Deletes the shard map, replication policies, and all underlying physical databases
-spec delete(binary()) -> ok | {error, term()}.
delete(VdbName) when is_binary(VdbName) ->
    case barrel_shard_map:exists(VdbName) of
        false ->
            {error, not_found};
        true ->
            %% Teardown replication first
            _ = barrel_vdb_replication:teardown_replication(VdbName),
            %% Delete all physical shard databases
            {ok, ShardDbs} = barrel_shard_map:all_physical_dbs(VdbName),
            lists:foreach(fun(DbName) ->
                barrel_docdb:delete_db(DbName)
            end, ShardDbs),
            %% Unregister from VDB registry
            barrel_vdb_registry:unregister_vdb(VdbName),
            %% Delete shard map
            barrel_shard_map:delete(VdbName)
    end.

%% @doc Check if a VDB exists
-spec exists(binary()) -> boolean().
exists(VdbName) when is_binary(VdbName) ->
    barrel_shard_map:exists(VdbName).

%% @doc List all VDBs
-spec list() -> {ok, [binary()]}.
list() ->
    barrel_shard_map:list().

%% @doc Get VDB info including shard statistics and replication status
%% Will attempt to pull config from peers if not found locally
-spec info(binary()) -> {ok, map()} | {error, not_found}.
info(VdbName) when is_binary(VdbName) ->
    %% Try local first, then pull from peers if not found
    case barrel_shard_map:get_config(VdbName) of
        {error, not_found} ->
            %% Try to ensure config from peers
            case ensure_vdb_config(VdbName) of
                ok -> info_internal(VdbName);
                {error, _} -> {error, not_found}
            end;
        {ok, _Config} ->
            info_internal(VdbName)
    end.

%% @private Internal info function after config is ensured
info_internal(VdbName) ->
    case barrel_shard_map:get_config(VdbName) of
        {ok, Config} ->
            {ok, ShardDbs} = barrel_shard_map:all_physical_dbs(VdbName),
            {ok, Assignments} = barrel_shard_map:get_all_assignments(VdbName),

            %% Get doc counts per shard (use fold_docs which excludes deleted docs)
            ShardStats = lists:map(fun(DbName) ->
                case barrel_docdb:db_info(DbName) of
                    {ok, DbInfo} ->
                        %% Get actual doc count via fold_docs (excludes deleted docs)
                        {ok, DocCount} = barrel_docdb:fold_docs(DbName,
                                fun(_Doc, Acc) -> {ok, Acc + 1} end, 0),
                        #{
                            db => DbName,
                            doc_count => DocCount,
                            disk_size => maps:get(disk_size, DbInfo, 0)
                        };
                    {error, not_found} ->
                        #{db => DbName, doc_count => 0, disk_size => 0}
                end
            end, ShardDbs),

            TotalDocs = lists:sum([maps:get(doc_count, S) || S <- ShardStats]),
            TotalSize = lists:sum([maps:get(disk_size, S) || S <- ShardStats]),

            %% Get replication status
            ReplicationStatus = case barrel_vdb_replication:get_status(VdbName) of
                {ok, RepStatus} -> RepStatus;
                {error, _} -> #{enabled => false}
            end,

            Info = #{
                name => VdbName,
                shard_count => maps:get(shard_count, Config),
                hash_function => maps:get(hash_function, Config),
                placement => maps:get(placement, Config),
                created_at => maps:get(created_at, Config),
                total_docs => TotalDocs,
                total_disk_size => TotalSize,
                shards => ShardStats,
                assignments => Assignments,
                replication => ReplicationStatus
            },
            {ok, Info};
        {error, not_found} ->
            {error, not_found}
    end.

%%====================================================================
%% Document Operations
%%====================================================================

%% @doc Put a document (routed to correct shard)
-spec put_doc(binary(), map()) -> {ok, map()} | {error, term()}.
put_doc(VdbName, Doc) ->
    put_doc(VdbName, Doc, #{}).

-spec put_doc(binary(), map(), map()) -> {ok, map()} | {error, term()}.
put_doc(VdbName, Doc, Opts) when is_binary(VdbName), is_map(Doc), is_map(Opts) ->
    DocId = get_doc_id(Doc),
    case route_to_shard(VdbName, DocId) of
        {ok, ShardDb} ->
            barrel_docdb:put_doc(ShardDb, Doc, Opts);
        {error, _} = Err ->
            Err
    end.

%% @doc Get a document (routed to correct shard)
-spec get_doc(binary(), binary()) -> {ok, map()} | {error, term()}.
get_doc(VdbName, DocId) ->
    get_doc(VdbName, DocId, #{}).

-spec get_doc(binary(), binary(), map()) -> {ok, map()} | {error, term()}.
get_doc(VdbName, DocId, Opts) when is_binary(VdbName), is_binary(DocId) ->
    case route_to_shard(VdbName, DocId) of
        {ok, ShardDb} ->
            barrel_docdb:get_doc(ShardDb, DocId, Opts);
        {error, _} = Err ->
            Err
    end.

%% @doc Delete a document (routed to correct shard)
-spec delete_doc(binary(), binary()) -> {ok, map()} | {error, term()}.
delete_doc(VdbName, DocId) ->
    delete_doc(VdbName, DocId, #{}).

-spec delete_doc(binary(), binary(), map()) -> {ok, map()} | {error, term()}.
delete_doc(VdbName, DocId, Opts) when is_binary(VdbName), is_binary(DocId), is_map(Opts) ->
    case route_to_shard(VdbName, DocId) of
        {ok, ShardDb} ->
            barrel_docdb:delete_doc(ShardDb, DocId, Opts);
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Bulk Operations
%%====================================================================

%% @doc Bulk document operations
%% Routes each doc to its shard, executes in parallel per shard
-spec bulk_docs(binary(), [map()]) -> {ok, [map()]} | {error, term()}.
bulk_docs(VdbName, Docs) ->
    bulk_docs(VdbName, Docs, #{}).

-spec bulk_docs(binary(), [map()], map()) -> {ok, [map()]} | {error, term()}.
bulk_docs(VdbName, Docs, Opts) when is_binary(VdbName), is_list(Docs) ->
    case barrel_shard_map:exists(VdbName) of
        false ->
            {error, not_found};
        true ->
            %% Group docs by shard
            GroupedDocs = group_docs_by_shard(VdbName, Docs),

            %% Execute bulk_docs per shard in parallel
            Results = barrel_parallel:pmap(
                fun({ShardDb, ShardDocs}) ->
                    %% Process each doc with put_doc (no bulk_docs in barrel_docdb)
                    lists:map(fun(Doc) ->
                        case barrel_docdb:put_doc(ShardDb, Doc, Opts) of
                            {ok, Result} -> Result;
                            {error, Reason} ->
                                #{error => Reason, <<"id">> => maps:get(<<"id">>, Doc, null)}
                        end
                    end, ShardDocs)
                end,
                maps:to_list(GroupedDocs)
            ),

            %% Flatten and reorder results
            {ok, lists:flatten(Results)}
    end.

%%====================================================================
%% Query Operations (Scatter-Gather) - Implemented in Step 4
%%====================================================================

%% @doc Find documents across all shards
-spec find(binary(), map()) -> {ok, [map()]} | {error, term()}.
find(VdbName, Query) ->
    find(VdbName, Query, #{}).

-spec find(binary(), map(), map()) -> {ok, [map()]} | {error, term()}.
find(VdbName, Query, Opts) when is_binary(VdbName), is_map(Query) ->
    case barrel_shard_map:all_physical_dbs(VdbName) of
        {ok, ShardDbs} ->
            %% Normalize query - ensure where clause exists
            NormalizedQuery = normalize_query(Query),
            %% Remove pagination opts from shard query - we apply at VDB level after merge
            ShardOpts = maps:without([limit, offset, <<"limit">>, <<"offset">>], Opts),
            %% Scatter: query all shards in parallel
            Results = barrel_parallel:pmap(
                fun(ShardDb) ->
                    case barrel_docdb:find(ShardDb, NormalizedQuery, ShardOpts) of
                        {ok, Docs, _Meta} ->
                            %% Extract actual doc from result format
                            [extract_doc(D) || D <- Docs];
                        {error, _} -> []
                    end
                end,
                ShardDbs
            ),
            %% Gather: merge results with VDB-level pagination
            MergedResults = merge_query_results(lists:flatten(Results), Query, Opts),
            {ok, MergedResults};
        {error, _} = Err ->
            Err
    end.

%% @doc Get changes across all shards
-spec get_changes(binary(), map()) -> {ok, map()} | {error, term()}.
get_changes(VdbName, Opts) ->
    get_changes(VdbName, first, Opts).

-spec get_changes(binary(), first | non_neg_integer() | binary(), map()) -> {ok, map()} | {error, term()}.
get_changes(VdbName, Since, Opts) when is_binary(VdbName) ->
    case barrel_shard_map:all_physical_dbs(VdbName) of
        {ok, ShardDbs} ->
            %% Get changes from all shards in parallel
            %% Note: We fetch more from each shard and merge/limit at VDB level
            ShardOpts = maps:remove(limit, Opts),
            AllChanges = barrel_parallel:pmap(
                fun(ShardDb) ->
                    {ok, Changes, LastSeq} = barrel_docdb:get_changes(ShardDb, Since, ShardOpts),
                    #{changes => Changes, last_seq => LastSeq}
                end,
                ShardDbs
            ),
            %% Merge changes by HLC timestamp and apply VDB-level limit
            MergedChanges = merge_changes(AllChanges, Opts),
            {ok, MergedChanges};
        {error, _} = Err ->
            Err
    end.

%% @doc Fold over all documents in all shards
-spec fold_docs(binary(), fun(), term(), map()) -> {ok, term()} | {error, term()}.
fold_docs(VdbName, Fun, Acc0, _Opts) when is_binary(VdbName), is_function(Fun, 2) ->
    case barrel_shard_map:all_physical_dbs(VdbName) of
        {ok, ShardDbs} ->
            %% Fold over each shard sequentially
            FinalAcc = lists:foldl(
                fun(ShardDb, Acc) ->
                    {ok, NewAcc} = barrel_docdb:fold_docs(ShardDb, Fun, Acc),
                    NewAcc
                end,
                Acc0,
                ShardDbs
            ),
            {ok, FinalAcc};
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Create all physical shard databases
create_shard_dbs(VdbName) ->
    case barrel_shard_map:all_physical_dbs(VdbName) of
        {ok, ShardDbs} ->
            Results = lists:map(fun(DbName) ->
                barrel_docdb:create_db(DbName, #{})
            end, ShardDbs),
            %% Check if all succeeded (create_db returns {ok, Pid})
            case lists:all(fun({ok, _}) -> true; (_) -> false end, Results) of
                true -> ok;
                false ->
                    %% Cleanup any created DBs
                    lists:foreach(fun(DbName) ->
                        barrel_docdb:delete_db(DbName)
                    end, ShardDbs),
                    {error, failed_to_create_shards}
            end;
        {error, _} = Err ->
            Err
    end.

%% @private Get document ID from doc map
get_doc_id(Doc) ->
    case maps:get(<<"id">>, Doc, undefined) of
        undefined ->
            %% Generate a new ID
            barrel_doc:generate_docid();
        Id ->
            Id
    end.

%% @private Route a document to its shard database
%% If VDB config not found locally, attempts to pull from peers
route_to_shard(VdbName, DocId) ->
    case barrel_shard_map:shard_for_doc(VdbName, DocId) of
        {ok, ShardId} ->
            ShardDb = barrel_shard_map:physical_db_name(VdbName, ShardId),
            {ok, ShardDb};
        {error, not_found} ->
            %% VDB not found locally - try to pull config from peers
            case ensure_vdb_config(VdbName) of
                ok ->
                    %% Config imported, retry routing
                    case barrel_shard_map:shard_for_doc(VdbName, DocId) of
                        {ok, ShardId} ->
                            ShardDb = barrel_shard_map:physical_db_name(VdbName, ShardId),
                            {ok, ShardDb};
                        {error, _} = Err ->
                            Err
                    end;
                {error, _} = Err ->
                    Err
            end
    end.

%% @private Group documents by their target shard
group_docs_by_shard(VdbName, Docs) ->
    lists:foldl(
        fun(Doc, Acc) ->
            DocId = get_doc_id(Doc),
            case route_to_shard(VdbName, DocId) of
                {ok, ShardDb} ->
                    maps:update_with(
                        ShardDb,
                        fun(Existing) -> [Doc | Existing] end,
                        [Doc],
                        Acc
                    );
                {error, _} ->
                    Acc  %% Skip docs that can't be routed
            end
        end,
        #{},
        Docs
    ).

%% @private Normalize query for barrel_docdb:find
%% Converts simplified selectors to where clause format and ensures where clause exists
normalize_query(Query) ->
    case maps:is_key(where, Query) of
        true ->
            Query;
        false ->
            %% Check for selector syntax (CouchDB-style)
            case maps:get(<<"selector">>, Query, undefined) of
                undefined ->
                    %% Empty query - return all docs
                    Query#{where => []};
                Selector when is_map(Selector) ->
                    %% Convert selector to where clauses
                    WhereClauses = selector_to_where(Selector),
                    Query#{where => WhereClauses};
                _ ->
                    Query#{where => []}
            end
    end.

%% @private Convert CouchDB-style selector to barrel_docdb where clauses
selector_to_where(Selector) when is_map(Selector) ->
    maps:fold(
        fun(Key, Value, Acc) when is_binary(Key), not is_map(Value) ->
            [{path, [Key], Value} | Acc];
           (_Key, _Value, Acc) ->
            %% Skip complex selectors for now
            Acc
        end,
        [],
        Selector
    ).

%% @private Extract the actual document from barrel_docdb:find result format
%% Result format: #{<<"doc">> => ActualDoc, <<"id">> => DocId, ...}
extract_doc(Result) when is_map(Result) ->
    case maps:get(<<"doc">>, Result, undefined) of
        undefined -> Result;  %% Already a plain doc
        Doc -> Doc
    end.

%% @private Merge query results from multiple shards
merge_query_results(Results, Query, Opts) ->
    %% Apply sort if specified (must sort before offset/limit)
    Sort = maps:get(sort, Opts, maps:get(<<"sort">>, Query,
           maps:get(order_by, Opts, maps:get(<<"order_by">>, Query, undefined)))),
    Sorted = case Sort of
        undefined -> Results;
        SortSpec -> sort_results(Results, SortSpec)
    end,

    %% Apply offset if specified
    Offset = maps:get(offset, Opts, maps:get(<<"offset">>, Query, 0)),
    AfterOffset = case Offset of
        0 -> Sorted;
        N when is_integer(N), N > 0 -> safe_drop(N, Sorted);
        _ -> Sorted
    end,

    %% Apply limit if specified
    Limit = maps:get(limit, Opts, maps:get(<<"limit">>, Query, undefined)),
    case Limit of
        undefined -> AfterOffset;
        M when is_integer(M) -> lists:sublist(AfterOffset, M);
        _ -> AfterOffset
    end.

%% @private Safely drop N elements from list
safe_drop(N, List) when N >= length(List) -> [];
safe_drop(N, List) -> lists:nthtail(N, List).

%% @private Sort results by sort specification
%% Supports formats:
%%   - [{#{<<"field">> => <<"asc">>}] - list of maps
%%   - [{field, asc}] - list of tuples
%%   - #{<<"field">> => <<"asc">>} - single map
%%   - {field, asc} - single tuple
%%   - <<"field">> or field - single field ascending
sort_results(Results, SortSpec) when is_list(SortSpec), length(SortSpec) > 0 ->
    [FirstSort | _] = SortSpec,
    sort_by_spec(Results, FirstSort);
sort_results(Results, SortSpec) when is_map(SortSpec) ->
    sort_by_spec(Results, SortSpec);
sort_results(Results, {Field, Direction}) ->
    sort_by_field(Results, Field, Direction);
sort_results(Results, Field) when is_binary(Field); is_atom(Field) ->
    sort_by_field(Results, Field, asc);
sort_results(Results, _) ->
    Results.

%% @private Sort by a single sort spec (map or tuple)
sort_by_spec(Results, Spec) when is_map(Spec) ->
    case maps:to_list(Spec) of
        [{Field, Direction}] -> sort_by_field(Results, Field, Direction);
        _ -> Results
    end;
sort_by_spec(Results, {Field, Direction}) ->
    sort_by_field(Results, Field, Direction);
sort_by_spec(Results, Field) when is_binary(Field); is_atom(Field) ->
    sort_by_field(Results, Field, asc);
sort_by_spec(Results, _) ->
    Results.

%% @private Sort results by field and direction
sort_by_field(Results, Field, Direction) ->
    FieldBin = if is_atom(Field) -> atom_to_binary(Field, utf8); true -> Field end,
    IsAsc = case Direction of
        asc -> true;
        <<"asc">> -> true;
        desc -> false;
        <<"desc">> -> false;
        _ -> true
    end,
    Comparator = if
        IsAsc -> fun(A, B) -> compare_values(maps:get(FieldBin, A, null), maps:get(FieldBin, B, null)) end;
        true -> fun(A, B) -> compare_values(maps:get(FieldBin, B, null), maps:get(FieldBin, A, null)) end
    end,
    lists:sort(Comparator, Results).

%% @private Compare two values for sorting (null-safe)
compare_values(null, _) -> true;
compare_values(_, null) -> false;
compare_values(A, B) -> A =< B.

%% @private Merge changes from multiple shards
merge_changes(AllChanges, Opts) ->
    %% Collect all changes
    AllChangesList = lists:flatmap(
        fun(#{changes := Changes}) -> Changes;
           (#{<<"changes">> := Changes}) -> Changes;
           (_) -> []
        end,
        AllChanges
    ),

    %% Sort by HLC timestamp (seq contains HLC timestamp for ordering)
    %% HLC format allows direct comparison for causal ordering
    SortedChanges = lists:sort(
        fun(A, B) ->
            %% Try seq first (HLC timestamp), then fall back to ts
            SeqA = maps:get(seq, A, maps:get(<<"seq">>, A, maps:get(ts, A, 0))),
            SeqB = maps:get(seq, B, maps:get(<<"seq">>, B, maps:get(ts, B, 0))),
            SeqA =< SeqB
        end,
        AllChangesList
    ),

    %% Apply limit if specified
    Limit = maps:get(limit, Opts, undefined),
    LimitedChanges = case Limit of
        undefined -> SortedChanges;
        N when is_integer(N), N > 0 -> lists:sublist(SortedChanges, N);
        _ -> SortedChanges
    end,

    %% Calculate last_seq (max across shards)
    LastSeq = lists:foldl(
        fun(Change, Max) ->
            Seq = case Change of
                #{last_seq := S} -> S;
                #{<<"last_seq">> := S} -> S;
                _ -> 0
            end,
            if Seq > Max -> Seq; true -> Max end
        end,
        0,
        AllChanges
    ),

    %% Calculate if there are more changes
    HasMore = length(SortedChanges) > length(LimitedChanges),

    #{
        changes => LimitedChanges,
        last_seq => LastSeq,
        has_more => HasMore
    }.

%% @private Setup replication if placement config has replica_factor > 1
setup_replication_if_needed(VdbName, Opts) ->
    Placement = maps:get(placement, Opts, #{}),
    ReplicaFactor = maps:get(replica_factor, Placement, 1),
    case ReplicaFactor of
        1 ->
            %% No replication needed
            ok;
        _ ->
            %% Setup replication - extract auth from opts if provided
            RepOpts = maps:with([auth], Opts),
            case barrel_vdb_replication:setup_replication(VdbName, RepOpts) of
                ok ->
                    logger:info("VDB ~s: replication setup complete with replica_factor=~p",
                               [VdbName, ReplicaFactor]);
                {error, Reason} ->
                    logger:warning("VDB ~s: replication setup failed: ~p",
                                  [VdbName, Reason])
            end
    end.

%% @private Broadcast VDB config to meta database asynchronously
broadcast_config_async(VdbName) ->
    spawn(fun() ->
        case barrel_vdb_sync:broadcast_config(VdbName) of
            ok ->
                logger:debug("VDB ~s: config broadcast to meta database", [VdbName]);
            {error, Reason} ->
                logger:warning("VDB ~s: failed to broadcast config: ~p", [VdbName, Reason])
        end
    end).

%% @private Ensure VDB config exists locally, pull from peers if needed
%% This is used for operations that might access VDBs created on other nodes
ensure_vdb_config(VdbName) ->
    case barrel_shard_map:exists(VdbName) of
        true ->
            ok;
        false ->
            case barrel_vdb_sync:ensure_config(VdbName) of
                {ok, _Config} -> ok;
                {error, _} = Err -> Err
            end
    end.
