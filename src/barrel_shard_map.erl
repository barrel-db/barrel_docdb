%%%-------------------------------------------------------------------
%%% @doc Shard Map for Virtual Database (VDB) sharding
%%%
%%% Manages shard metadata for logical databases:
%%% - Shard configuration (count, hash function, placement)
%%% - Shard ranges (for split/merge operations)
%%% - Shard assignments (primary node, replicas, status)
%%%
%%% Data is stored in the _barrel_vdb_meta system database.
%%%
%%% Example:
%%% ```
%%% %% Create a new VDB with 4 shards
%%% ok = barrel_shard_map:create(<<"users">>, #{
%%%     shard_count => 4,
%%%     placement => #{
%%%         replica_factor => 2,
%%%         zones => [<<"us-east">>, <<"eu-west">>]
%%%     }
%%% }).
%%%
%%% %% Get shard for a document
%%% {ok, 2} = barrel_shard_map:shard_for_doc(<<"users">>, <<"doc123">>).
%%%
%%% %% Get physical database name
%%% <<"users_s2">> = barrel_shard_map:physical_db_name(<<"users">>, 2).
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_shard_map).

%% API
-export([
    %% VDB lifecycle
    create/2,
    delete/1,
    exists/1,
    list/0,
    get_config/1,

    %% Routing
    shard_for_doc/2,
    physical_db_name/2,
    all_physical_dbs/1,

    %% Shard ranges (for split/merge)
    get_ranges/1,
    set_range/3,

    %% Shard assignments
    get_assignment/2,
    set_assignment/3,
    get_all_assignments/1,

    %% Shard status
    get_status/2,
    set_status/3
]).

-export_type([
    shard_config/0,
    placement_config/0,
    shard_range/0,
    shard_assignment/0,
    shard_status/0
]).

%%====================================================================
%% Types
%%====================================================================

-type shard_config() :: #{
    logical_db := binary(),
    shard_count := pos_integer(),
    hash_function := phash2 | xxhash,
    placement := placement_config(),
    created_at := integer()
}.

-type placement_config() :: #{
    replica_factor := pos_integer(),
    zones => [binary()],
    constraints => [constraint()]
}.

-type constraint() :: {min_per_zone, pos_integer()}
                    | {max_per_zone, pos_integer()}
                    | {prefer_zones, [binary()]}.

-type shard_range() :: #{
    shard_id := non_neg_integer(),
    start_hash := non_neg_integer(),
    end_hash := non_neg_integer()
}.

-type shard_assignment() :: #{
    shard_id := non_neg_integer(),
    primary := node_ref(),
    replicas := [node_ref()],
    status := shard_status()
}.

-type node_ref() :: binary().  %% URL or node identifier

-type shard_status() :: active | splitting | merging | readonly | migrating.

%%====================================================================
%% API - VDB Lifecycle
%%====================================================================

%% @doc Create a new virtual database with sharding configuration
%% Creates the shard map entry and initializes shard ranges
-spec create(binary(), map()) -> ok | {error, term()}.
create(LogicalDb, Opts) when is_binary(LogicalDb), is_map(Opts) ->
    case exists(LogicalDb) of
        true ->
            {error, already_exists};
        false ->
            ShardCount = maps:get(shard_count, Opts, 4),
            HashFn = maps:get(hash_function, Opts, phash2),
            Placement = maps:get(placement, Opts, #{replica_factor => 1}),

            Config = #{
                logical_db => LogicalDb,
                shard_count => ShardCount,
                hash_function => HashFn,
                placement => normalize_placement(Placement),
                created_at => erlang:system_time(millisecond)
            },

            %% Store config
            DocId = config_doc_id(LogicalDb),
            ok = barrel_docdb:put_system_doc(DocId, Config),

            %% Initialize ranges (equal distribution)
            ok = init_ranges(LogicalDb, ShardCount),

            %% Initialize assignments (empty - will be populated by VDB)
            ok = init_assignments(LogicalDb, ShardCount),

            ok
    end.

%% @doc Delete a virtual database configuration
%% Note: Does not delete the physical shard databases
-spec delete(binary()) -> ok | {error, not_found}.
delete(LogicalDb) when is_binary(LogicalDb) ->
    case exists(LogicalDb) of
        false ->
            {error, not_found};
        true ->
            %% Delete config
            ConfigDocId = config_doc_id(LogicalDb),
            barrel_docdb:delete_system_doc(ConfigDocId),

            %% Delete ranges
            RangesDocId = ranges_doc_id(LogicalDb),
            barrel_docdb:delete_system_doc(RangesDocId),

            %% Delete assignments
            AssignDocId = assignments_doc_id(LogicalDb),
            barrel_docdb:delete_system_doc(AssignDocId),

            ok
    end.

%% @doc Check if a virtual database exists
-spec exists(binary()) -> boolean().
exists(LogicalDb) when is_binary(LogicalDb) ->
    DocId = config_doc_id(LogicalDb),
    case barrel_docdb:get_system_doc(DocId) of
        {ok, _} -> true;
        {error, not_found} -> false
    end.

%% @doc List all virtual databases
-spec list() -> {ok, [binary()]}.
list() ->
    {ok, Docs} = barrel_docdb:fold_system_docs(
        <<"vdb:config:">>,
        fun(_DocId, Doc, Acc) ->
            case maps:get(logical_db, Doc, undefined) of
                undefined -> Acc;
                Name -> [Name | Acc]
            end
        end,
        []
    ),
    {ok, lists:sort(Docs)}.

%% @doc Get configuration for a virtual database
-spec get_config(binary()) -> {ok, shard_config()} | {error, not_found}.
get_config(LogicalDb) when is_binary(LogicalDb) ->
    DocId = config_doc_id(LogicalDb),
    barrel_docdb:get_system_doc(DocId).

%%====================================================================
%% API - Routing
%%====================================================================

%% @doc Get the shard ID for a document
%% Uses consistent hashing based on document ID
-spec shard_for_doc(binary(), binary()) -> {ok, non_neg_integer()} | {error, not_found}.
shard_for_doc(LogicalDb, DocId) when is_binary(LogicalDb), is_binary(DocId) ->
    case get_ranges(LogicalDb) of
        {ok, Ranges} ->
            Hash = compute_hash(DocId),
            ShardId = find_shard_for_hash(Hash, Ranges),
            {ok, ShardId};
        {error, _} = Err ->
            Err
    end.

%% @doc Get the physical database name for a shard
-spec physical_db_name(binary(), non_neg_integer()) -> binary().
physical_db_name(LogicalDb, ShardId) when is_binary(LogicalDb), is_integer(ShardId) ->
    <<LogicalDb/binary, "_s", (integer_to_binary(ShardId))/binary>>.

%% @doc Get all physical database names for a VDB
%% Uses actual shard IDs from ranges (may be non-contiguous after merge)
-spec all_physical_dbs(binary()) -> {ok, [binary()]} | {error, not_found}.
all_physical_dbs(LogicalDb) when is_binary(LogicalDb) ->
    case get_ranges(LogicalDb) of
        {ok, Ranges} ->
            ShardIds = [maps:get(shard_id, R) || R <- Ranges],
            Dbs = [physical_db_name(LogicalDb, Id) || Id <- lists:sort(ShardIds)],
            {ok, Dbs};
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% API - Shard Ranges
%%====================================================================

%% @doc Get all shard ranges for a VDB
-spec get_ranges(binary()) -> {ok, [shard_range()]} | {error, not_found}.
get_ranges(LogicalDb) when is_binary(LogicalDb) ->
    DocId = ranges_doc_id(LogicalDb),
    case barrel_docdb:get_system_doc(DocId) of
        {ok, #{<<"ranges">> := Ranges}} ->
            {ok, decode_ranges(Ranges)};
        {error, not_found} ->
            {error, not_found}
    end.

%% @doc Set range for a specific shard (used during split/merge)
-spec set_range(binary(), non_neg_integer(), shard_range()) -> ok | {error, not_found}.
set_range(LogicalDb, ShardId, Range) when is_binary(LogicalDb), is_integer(ShardId), is_map(Range) ->
    DocId = ranges_doc_id(LogicalDb),
    case barrel_docdb:get_system_doc(DocId) of
        {ok, Doc} ->
            Ranges = maps:get(<<"ranges">>, Doc, #{}),
            Key = integer_to_binary(ShardId),
            NewRanges = Ranges#{Key => encode_range(Range)},
            barrel_docdb:put_system_doc(DocId, Doc#{<<"ranges">> => NewRanges});
        {error, not_found} ->
            {error, not_found}
    end.

%%====================================================================
%% API - Shard Assignments
%%====================================================================

%% @doc Get assignment for a specific shard
-spec get_assignment(binary(), non_neg_integer()) -> {ok, shard_assignment()} | {error, not_found}.
get_assignment(LogicalDb, ShardId) when is_binary(LogicalDb), is_integer(ShardId) ->
    DocId = assignments_doc_id(LogicalDb),
    case barrel_docdb:get_system_doc(DocId) of
        {ok, #{<<"assignments">> := Assignments}} ->
            Key = integer_to_binary(ShardId),
            case maps:get(Key, Assignments, undefined) of
                undefined -> {error, not_found};
                Assignment -> {ok, decode_assignment(Assignment)}
            end;
        {error, not_found} ->
            {error, not_found}
    end.

%% @doc Set assignment for a specific shard
-spec set_assignment(binary(), non_neg_integer(), shard_assignment()) -> ok | {error, not_found}.
set_assignment(LogicalDb, ShardId, Assignment) when is_binary(LogicalDb), is_integer(ShardId), is_map(Assignment) ->
    DocId = assignments_doc_id(LogicalDb),
    case barrel_docdb:get_system_doc(DocId) of
        {ok, Doc} ->
            Assignments = maps:get(<<"assignments">>, Doc, #{}),
            Key = integer_to_binary(ShardId),
            NewAssignments = Assignments#{Key => encode_assignment(Assignment)},
            barrel_docdb:put_system_doc(DocId, Doc#{<<"assignments">> => NewAssignments});
        {error, not_found} ->
            {error, not_found}
    end.

%% @doc Get all shard assignments for a VDB
-spec get_all_assignments(binary()) -> {ok, [shard_assignment()]} | {error, not_found}.
get_all_assignments(LogicalDb) when is_binary(LogicalDb) ->
    DocId = assignments_doc_id(LogicalDb),
    case barrel_docdb:get_system_doc(DocId) of
        {ok, #{<<"assignments">> := Assignments}} ->
            Decoded = maps:fold(
                fun(_K, V, Acc) -> [decode_assignment(V) | Acc] end,
                [],
                Assignments
            ),
            {ok, lists:sort(fun(A, B) ->
                maps:get(shard_id, A) =< maps:get(shard_id, B)
            end, Decoded)};
        {error, not_found} ->
            {error, not_found}
    end.

%%====================================================================
%% API - Shard Status
%%====================================================================

%% @doc Get status for a specific shard
-spec get_status(binary(), non_neg_integer()) -> {ok, shard_status()} | {error, not_found}.
get_status(LogicalDb, ShardId) ->
    case get_assignment(LogicalDb, ShardId) of
        {ok, #{status := Status}} -> {ok, Status};
        {error, _} = Err -> Err
    end.

%% @doc Set status for a specific shard
-spec set_status(binary(), non_neg_integer(), shard_status()) -> ok | {error, not_found}.
set_status(LogicalDb, ShardId, Status) when is_atom(Status) ->
    case get_assignment(LogicalDb, ShardId) of
        {ok, Assignment} ->
            set_assignment(LogicalDb, ShardId, Assignment#{status => Status});
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Config document ID
config_doc_id(LogicalDb) ->
    <<"vdb:config:", LogicalDb/binary>>.

%% @private Ranges document ID
ranges_doc_id(LogicalDb) ->
    <<"vdb:ranges:", LogicalDb/binary>>.

%% @private Assignments document ID
assignments_doc_id(LogicalDb) ->
    <<"vdb:assign:", LogicalDb/binary>>.

%% @private Normalize placement config
normalize_placement(Placement) ->
    #{
        replica_factor => maps:get(replica_factor, Placement, 1),
        zones => maps:get(zones, Placement, []),
        constraints => maps:get(constraints, Placement, [])
    }.

%% @private Initialize shard ranges with equal distribution
init_ranges(LogicalDb, ShardCount) ->
    %% Use 32-bit hash space (erlang:phash2 range)
    MaxHash = 16#FFFFFFFF,
    RangeSize = MaxHash div ShardCount,

    Ranges = lists:foldl(
        fun(ShardId, Acc) ->
            StartHash = ShardId * RangeSize,
            EndHash = case ShardId of
                _ when ShardId =:= ShardCount - 1 -> MaxHash;
                _ -> (ShardId + 1) * RangeSize - 1
            end,
            Range = #{
                shard_id => ShardId,
                start_hash => StartHash,
                end_hash => EndHash
            },
            Key = integer_to_binary(ShardId),
            Acc#{Key => encode_range(Range)}
        end,
        #{},
        lists:seq(0, ShardCount - 1)
    ),

    DocId = ranges_doc_id(LogicalDb),
    barrel_docdb:put_system_doc(DocId, #{<<"ranges">> => Ranges}).

%% @private Initialize empty assignments
init_assignments(LogicalDb, ShardCount) ->
    Assignments = lists:foldl(
        fun(ShardId, Acc) ->
            Assignment = #{
                shard_id => ShardId,
                primary => undefined,
                replicas => [],
                status => active
            },
            Key = integer_to_binary(ShardId),
            Acc#{Key => encode_assignment(Assignment)}
        end,
        #{},
        lists:seq(0, ShardCount - 1)
    ),

    DocId = assignments_doc_id(LogicalDb),
    barrel_docdb:put_system_doc(DocId, #{<<"assignments">> => Assignments}).

%% @private Compute hash for document ID
compute_hash(DocId) ->
    erlang:phash2(DocId, 16#FFFFFFFF).

%% @private Find shard for a hash value
find_shard_for_hash(Hash, Ranges) ->
    %% Find the range that contains this hash
    Result = lists:foldl(
        fun(#{shard_id := ShardId, start_hash := Start, end_hash := End}, Acc) ->
            case Acc of
                undefined when Hash >= Start, Hash =< End -> ShardId;
                _ -> Acc
            end
        end,
        undefined,
        Ranges
    ),
    case Result of
        undefined -> 0;  %% Fallback to shard 0
        ShardId -> ShardId
    end.

%% @private Encode range for storage
encode_range(#{shard_id := ShardId, start_hash := Start, end_hash := End}) ->
    #{
        <<"shard_id">> => ShardId,
        <<"start_hash">> => Start,
        <<"end_hash">> => End
    }.

%% @private Decode range from storage
decode_ranges(RangesMap) ->
    maps:fold(
        fun(_K, V, Acc) ->
            Range = #{
                shard_id => maps:get(<<"shard_id">>, V),
                start_hash => maps:get(<<"start_hash">>, V),
                end_hash => maps:get(<<"end_hash">>, V)
            },
            [Range | Acc]
        end,
        [],
        RangesMap
    ).

%% @private Encode assignment for storage
encode_assignment(#{shard_id := ShardId, primary := Primary, replicas := Replicas, status := Status}) ->
    #{
        <<"shard_id">> => ShardId,
        <<"primary">> => Primary,
        <<"replicas">> => Replicas,
        <<"status">> => atom_to_binary(Status)
    }.

%% @private Decode assignment from storage
decode_assignment(Map) ->
    #{
        shard_id => maps:get(<<"shard_id">>, Map),
        primary => maps:get(<<"primary">>, Map),
        replicas => maps:get(<<"replicas">>, Map, []),
        status => binary_to_existing_atom(maps:get(<<"status">>, Map, <<"active">>), utf8)
    }.
