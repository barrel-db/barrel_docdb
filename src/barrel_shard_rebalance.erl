%%%-------------------------------------------------------------------
%%% @doc Shard Rebalancing for Virtual Database (VDB)
%%%
%%% Provides split and merge operations for VDB shards:
%%% - Split a shard when it grows too large
%%% - Merge adjacent shards when they become underutilized
%%%
%%% Example:
%%% ```
%%% %% Split shard 2 into two shards (2 and new shard N)
%%% {ok, NewShardId} = barrel_shard_rebalance:split_shard(<<"users">>, 2).
%%%
%%% %% Merge shards 2 and 3 (3 is merged into 2)
%%% ok = barrel_shard_rebalance:merge_shards(<<"users">>, 2, 3).
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_shard_rebalance).

%% API
-export([
    split_shard/2,
    split_shard/3,
    merge_shards/3,
    merge_shards/4,
    can_merge/3,
    estimate_migration/3
]).

%% Types
-export_type([
    split_opts/0,
    merge_opts/0,
    progress_callback/0
]).

-type split_opts() :: #{
    progress_callback => progress_callback(),
    batch_size => pos_integer()
}.

-type merge_opts() :: #{
    progress_callback => progress_callback(),
    batch_size => pos_integer()
}.

-type progress_callback() :: fun((progress_info()) -> ok).

-type progress_info() :: #{
    phase := preparing | migrating | finalizing,
    migrated := non_neg_integer(),
    total := non_neg_integer(),
    current_doc => binary()
}.

%%====================================================================
%% API
%%====================================================================

%% @doc Split a shard into two shards
%% The original shard keeps the lower half of its hash range,
%% a new shard is created for the upper half.
%% Returns the new shard ID.
-spec split_shard(binary(), non_neg_integer()) ->
    {ok, non_neg_integer()} | {error, term()}.
split_shard(VdbName, ShardId) ->
    split_shard(VdbName, ShardId, #{}).

-spec split_shard(binary(), non_neg_integer(), split_opts()) ->
    {ok, non_neg_integer()} | {error, term()}.
split_shard(VdbName, ShardId, Opts) when is_binary(VdbName), is_integer(ShardId) ->
    case barrel_shard_map:get_status(VdbName, ShardId) of
        {ok, active} ->
            do_split_shard(VdbName, ShardId, Opts);
        {ok, Status} ->
            {error, {shard_not_active, Status}};
        {error, _} = Err ->
            Err
    end.

%% @doc Merge two adjacent shards
%% The second shard (ShardId2) is merged into the first (ShardId1).
%% ShardId2's physical database is deleted after migration.
-spec merge_shards(binary(), non_neg_integer(), non_neg_integer()) ->
    ok | {error, term()}.
merge_shards(VdbName, ShardId1, ShardId2) ->
    merge_shards(VdbName, ShardId1, ShardId2, #{}).

-spec merge_shards(binary(), non_neg_integer(), non_neg_integer(), merge_opts()) ->
    ok | {error, term()}.
merge_shards(VdbName, ShardId1, ShardId2, Opts)
  when is_binary(VdbName), is_integer(ShardId1), is_integer(ShardId2) ->
    case can_merge(VdbName, ShardId1, ShardId2) of
        {ok, true} ->
            do_merge_shards(VdbName, ShardId1, ShardId2, Opts);
        {ok, false} ->
            {error, shards_not_adjacent};
        {error, _} = Err ->
            Err
    end.

%% @doc Check if two shards can be merged (must be adjacent in hash space)
-spec can_merge(binary(), non_neg_integer(), non_neg_integer()) ->
    {ok, boolean()} | {error, term()}.
can_merge(VdbName, ShardId1, ShardId2) ->
    case {barrel_shard_map:get_assignment(VdbName, ShardId1),
          barrel_shard_map:get_assignment(VdbName, ShardId2)} of
        {{ok, #{status := active}}, {ok, #{status := active}}} ->
            case barrel_shard_map:get_ranges(VdbName) of
                {ok, Ranges} ->
                    Range1 = find_range(ShardId1, Ranges),
                    Range2 = find_range(ShardId2, Ranges),
                    case {Range1, Range2} of
                        {#{end_hash := End1}, #{start_hash := Start2}}
                          when End1 + 1 =:= Start2 ->
                            {ok, true};
                        {#{start_hash := Start1}, #{end_hash := End2}}
                          when End2 + 1 =:= Start1 ->
                            %% Shards in reverse order - still adjacent
                            {ok, true};
                        _ ->
                            {ok, false}
                    end;
                {error, _} = Err ->
                    Err
            end;
        {{ok, #{status := Status1}}, _} when Status1 =/= active ->
            {error, {shard_not_active, ShardId1, Status1}};
        {_, {ok, #{status := Status2}}} when Status2 =/= active ->
            {error, {shard_not_active, ShardId2, Status2}};
        {{error, _} = Err, _} ->
            Err;
        {_, {error, _} = Err} ->
            Err
    end.

%% @doc Estimate migration cost between two shards
%% Returns the number of documents that would need to move
-spec estimate_migration(binary(), non_neg_integer(), non_neg_integer()) ->
    {ok, non_neg_integer()} | {error, term()}.
estimate_migration(VdbName, FromShardId, _ToShardId) ->
    FromDb = barrel_shard_map:physical_db_name(VdbName, FromShardId),
    %% Count docs via fold since db_info may not have doc_count
    case barrel_docdb:fold_docs(FromDb,
            fun(_Doc, Acc) -> {ok, Acc + 1} end,
            0) of
        {ok, Count} ->
            {ok, Count};
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Internal Functions - Split
%%====================================================================

%% @private Execute shard split
do_split_shard(VdbName, ShardId, Opts) ->
    ProgressCb = maps:get(progress_callback, Opts, fun(_) -> ok end),
    BatchSize = maps:get(batch_size, Opts, 100),

    %% Phase 1: Prepare
    ProgressCb(#{phase => preparing, migrated => 0, total => 0}),

    %% Get current range
    case barrel_shard_map:get_ranges(VdbName) of
        {ok, Ranges} ->
            case find_range(ShardId, Ranges) of
                #{start_hash := StartHash, end_hash := EndHash} = _Range ->
                    %% Calculate split point (midpoint)
                    MidHash = StartHash + ((EndHash - StartHash) div 2),

                    %% Allocate new shard ID
                    NewShardId = allocate_shard_id(Ranges),

                    %% Set original shard to splitting status
                    ok = barrel_shard_map:set_status(VdbName, ShardId, splitting),

                    try
                        %% Create new shard's physical database
                        NewShardDb = barrel_shard_map:physical_db_name(VdbName, NewShardId),
                        case barrel_docdb:create_db(NewShardDb, #{}) of
                            {ok, _} -> ok;
                            {error, already_exists} -> ok;
                            {error, CreateErr} -> throw({create_db_failed, CreateErr})
                        end,

                        %% Update ranges: original gets [Start, Mid], new gets [Mid+1, End]
                        ok = barrel_shard_map:set_range(VdbName, ShardId, #{
                            shard_id => ShardId,
                            start_hash => StartHash,
                            end_hash => MidHash
                        }),
                        ok = barrel_shard_map:set_range(VdbName, NewShardId, #{
                            shard_id => NewShardId,
                            start_hash => MidHash + 1,
                            end_hash => EndHash
                        }),

                        %% Initialize assignment for new shard
                        ok = barrel_shard_map:set_assignment(VdbName, NewShardId, #{
                            shard_id => NewShardId,
                            primary => undefined,
                            replicas => [],
                            status => splitting
                        }),

                        %% Phase 2: Migrate documents in upper range to new shard
                        OriginalDb = barrel_shard_map:physical_db_name(VdbName, ShardId),
                        MigratedCount = migrate_docs_to_new_shard(
                            OriginalDb, NewShardDb, MidHash + 1, EndHash,
                            BatchSize, ProgressCb
                        ),

                        %% Phase 3: Finalize
                        ProgressCb(#{phase => finalizing, migrated => MigratedCount, total => MigratedCount}),

                        %% Update shard count in config
                        ok = update_shard_count(VdbName, 1),

                        %% Set both shards to active
                        ok = barrel_shard_map:set_status(VdbName, ShardId, active),
                        ok = barrel_shard_map:set_status(VdbName, NewShardId, active),

                        logger:info("VDB ~s: split shard ~p into ~p and ~p (~p docs migrated)",
                                   [VdbName, ShardId, ShardId, NewShardId, MigratedCount]),

                        {ok, NewShardId}
                    catch
                        throw:Reason ->
                            %% Rollback: restore original status
                            barrel_shard_map:set_status(VdbName, ShardId, active),
                            {error, Reason}
                    end;
                undefined ->
                    {error, {shard_not_found, ShardId}}
            end;
        {error, _} = Err ->
            Err
    end.

%% @private Migrate documents from original shard to new shard based on hash range
migrate_docs_to_new_shard(FromDb, ToDb, MinHash, MaxHash, BatchSize, ProgressCb) ->
    %% Collect documents that belong to the new range
    DocsToMigrate = collect_docs_in_range(FromDb, MinHash, MaxHash),
    TotalDocs = length(DocsToMigrate),

    %% Migrate in batches
    migrate_docs_batch(FromDb, ToDb, DocsToMigrate, 0, TotalDocs, BatchSize, ProgressCb).

%% @private Collect documents whose hash falls in the given range
collect_docs_in_range(DbName, MinHash, MaxHash) ->
    {ok, Docs} = barrel_docdb:fold_docs(DbName,
        fun(Doc, Acc) ->
            DocId = maps:get(<<"id">>, Doc),
            Hash = erlang:phash2(DocId, 16#FFFFFFFF),
            case Hash >= MinHash andalso Hash =< MaxHash of
                true -> {ok, [Doc | Acc]};
                false -> {ok, Acc}
            end
        end,
        []
    ),
    lists:reverse(Docs).

%% @private Migrate documents in batches
migrate_docs_batch(_FromDb, _ToDb, [], Migrated, _Total, _BatchSize, _ProgressCb) ->
    Migrated;
migrate_docs_batch(FromDb, ToDb, Docs, Migrated, Total, BatchSize, ProgressCb) ->
    {Batch, Rest} = safe_split(BatchSize, Docs),

    %% Migrate each doc: copy then delete
    lists:foreach(fun(Doc) ->
        DocId = maps:get(<<"id">>, Doc),
        ProgressCb(#{phase => migrating, migrated => Migrated, total => Total, current_doc => DocId}),

        %% Put doc in new shard (without rev to create fresh)
        DocWithoutRev = maps:remove(<<"_rev">>, Doc),
        CopyResult = barrel_docdb:put_doc(ToDb, DocWithoutRev, #{}),

        %% Only delete from source if copy succeeded
        case CopyResult of
            {ok, _} ->
                %% Delete from original shard
                case barrel_docdb:delete_doc(FromDb, DocId, #{}) of
                    {ok, _} -> ok;
                    {error, not_found} -> ok;  %% Already deleted
                    {error, DelErr} ->
                        logger:warning("Failed to delete migrated doc ~s from ~s: ~p",
                                      [DocId, FromDb, DelErr])
                end;
            {error, conflict} ->
                %% Doc already exists in target - still delete from source
                case barrel_docdb:delete_doc(FromDb, DocId, #{}) of
                    {ok, _} -> ok;
                    {error, not_found} -> ok;
                    {error, _} -> ok
                end;
            {error, Err} ->
                logger:warning("Failed to migrate doc ~s to ~s: ~p", [DocId, ToDb, Err])
        end
    end, Batch),

    NewMigrated = Migrated + length(Batch),
    migrate_docs_batch(FromDb, ToDb, Rest, NewMigrated, Total, BatchSize, ProgressCb).

%%====================================================================
%% Internal Functions - Merge
%%====================================================================

%% @private Execute shard merge
do_merge_shards(VdbName, ShardId1, ShardId2, Opts) ->
    ProgressCb = maps:get(progress_callback, Opts, fun(_) -> ok end),
    BatchSize = maps:get(batch_size, Opts, 100),

    %% Phase 1: Prepare
    ProgressCb(#{phase => preparing, migrated => 0, total => 0}),

    %% Get ranges for both shards
    case barrel_shard_map:get_ranges(VdbName) of
        {ok, Ranges} ->
            Range1 = find_range(ShardId1, Ranges),
            Range2 = find_range(ShardId2, Ranges),

            case {Range1, Range2} of
                {#{start_hash := Start1, end_hash := End1},
                 #{start_hash := Start2, end_hash := End2}} ->
                    %% Determine new merged range
                    NewStart = min(Start1, Start2),
                    NewEnd = max(End1, End2),

                    %% Set both shards to merging status
                    ok = barrel_shard_map:set_status(VdbName, ShardId1, merging),
                    ok = barrel_shard_map:set_status(VdbName, ShardId2, merging),

                    try
                        %% Phase 2: Migrate documents from shard2 to shard1
                        Db1 = barrel_shard_map:physical_db_name(VdbName, ShardId1),
                        Db2 = barrel_shard_map:physical_db_name(VdbName, ShardId2),

                        MigratedCount = migrate_all_docs(Db2, Db1, BatchSize, ProgressCb),

                        %% Phase 3: Finalize
                        ProgressCb(#{phase => finalizing, migrated => MigratedCount, total => MigratedCount}),

                        %% Update shard1's range to cover both
                        ok = barrel_shard_map:set_range(VdbName, ShardId1, #{
                            shard_id => ShardId1,
                            start_hash => NewStart,
                            end_hash => NewEnd
                        }),

                        %% Delete shard2's physical database
                        barrel_docdb:delete_db(Db2),

                        %% Remove shard2 from ranges and assignments
                        ok = remove_shard_metadata(VdbName, ShardId2),

                        %% Update shard count in config
                        ok = update_shard_count(VdbName, -1),

                        %% Set merged shard to active
                        ok = barrel_shard_map:set_status(VdbName, ShardId1, active),

                        logger:info("VDB ~s: merged shards ~p and ~p into ~p (~p docs migrated)",
                                   [VdbName, ShardId1, ShardId2, ShardId1, MigratedCount]),

                        ok
                    catch
                        throw:Reason ->
                            %% Rollback: restore statuses
                            barrel_shard_map:set_status(VdbName, ShardId1, active),
                            barrel_shard_map:set_status(VdbName, ShardId2, active),
                            {error, Reason}
                    end;
                _ ->
                    {error, {invalid_ranges, ShardId1, ShardId2}}
            end;
        {error, _} = Err ->
            Err
    end.

%% @private Migrate all documents from one shard to another
migrate_all_docs(FromDb, ToDb, BatchSize, ProgressCb) ->
    %% Collect all documents
    {ok, AllDocs} = barrel_docdb:fold_docs(FromDb,
        fun(Doc, Acc) -> {ok, [Doc | Acc]} end,
        []
    ),
    TotalDocs = length(AllDocs),

    %% Migrate in batches
    migrate_docs_batch(FromDb, ToDb, lists:reverse(AllDocs), 0, TotalDocs, BatchSize, ProgressCb).

%%====================================================================
%% Internal Functions - Helpers
%%====================================================================

%% @private Find range for a shard ID
find_range(ShardId, Ranges) ->
    case lists:filter(fun(#{shard_id := Id}) -> Id =:= ShardId end, Ranges) of
        [Range | _] -> Range;
        [] -> undefined
    end.

%% @private Allocate a new shard ID (max existing + 1)
allocate_shard_id(Ranges) ->
    MaxId = lists:foldl(
        fun(#{shard_id := Id}, Max) -> max(Id, Max) end,
        -1,
        Ranges
    ),
    MaxId + 1.

%% @private Update shard count in VDB config
update_shard_count(VdbName, Delta) ->
    case barrel_shard_map:get_config(VdbName) of
        {ok, Config} ->
            CurrentCount = maps:get(shard_count, Config),
            NewCount = CurrentCount + Delta,
            %% Update config via system doc
            DocId = <<"vdb:config:", VdbName/binary>>,
            NewConfig = Config#{shard_count => NewCount},
            barrel_docdb:put_system_doc(DocId, NewConfig);
        {error, _} = Err ->
            Err
    end.

%% @private Remove shard metadata (ranges and assignments entries)
remove_shard_metadata(VdbName, ShardId) ->
    %% Remove from ranges document
    RangesDocId = <<"vdb:ranges:", VdbName/binary>>,
    case barrel_docdb:get_system_doc(RangesDocId) of
        {ok, RangesDoc} ->
            Ranges = maps:get(<<"ranges">>, RangesDoc, #{}),
            Key = integer_to_binary(ShardId),
            NewRanges = maps:remove(Key, Ranges),
            barrel_docdb:put_system_doc(RangesDocId, RangesDoc#{<<"ranges">> => NewRanges});
        {error, not_found} ->
            ok
    end,

    %% Remove from assignments document
    AssignDocId = <<"vdb:assign:", VdbName/binary>>,
    case barrel_docdb:get_system_doc(AssignDocId) of
        {ok, AssignDoc} ->
            Assignments = maps:get(<<"assignments">>, AssignDoc, #{}),
            Key2 = integer_to_binary(ShardId),
            NewAssignments = maps:remove(Key2, Assignments),
            barrel_docdb:put_system_doc(AssignDocId, AssignDoc#{<<"assignments">> => NewAssignments});
        {error, not_found} ->
            ok
    end.

%% @private Safely split a list
safe_split(N, List) when length(List) =< N ->
    {List, []};
safe_split(N, List) ->
    lists:split(N, List).
