%%%-------------------------------------------------------------------
%%% @doc VDB Replication Manager
%%%
%%% Handles shard replication for Virtual Databases:
%%% - Zone-aware replica placement
%%% - Automatic replication policy creation
%%% - Replication status tracking
%%%
%%% Example:
%%% ```
%%% %% Setup replication for a VDB with 2 replicas
%%% ok = barrel_vdb_replication:setup_replication(<<"users">>, #{
%%%     replica_factor => 2
%%% }).
%%%
%%% %% Get replication status for a VDB
%%% {ok, Status} = barrel_vdb_replication:get_status(<<"users">>).
%%%
%%% %% Disable all replication for a VDB
%%% ok = barrel_vdb_replication:teardown_replication(<<"users">>).
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb_replication).

%% API
-export([
    setup_replication/2,
    teardown_replication/1,
    get_status/1,
    get_shard_status/2,
    update_assignments/1,
    compute_placement/2
]).

%%====================================================================
%% Types
%%====================================================================

-type placement_result() :: #{
    shard_id := non_neg_integer(),
    primary := binary() | undefined,
    replicas := [binary()],
    zone_distribution := #{binary() => [binary()]}
}.

-export_type([placement_result/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Setup replication for a VDB
%% Creates replication policies for each shard based on placement config
-spec setup_replication(binary(), map()) -> ok | {error, term()}.
setup_replication(VdbName, Opts) when is_binary(VdbName), is_map(Opts) ->
    case barrel_shard_map:get_config(VdbName) of
        {ok, Config} ->
            Placement = maps:get(placement, Config, #{}),
            ReplicaFactor = maps:get(replica_factor, Placement, 1),

            case ReplicaFactor of
                1 ->
                    %% No replication needed
                    ok;
                N when N > 1 ->
                    %% Compute placements and create policies
                    setup_shard_replication(VdbName, Config, Opts)
            end;
        {error, _} = Err ->
            Err
    end.

%% @doc Teardown replication for a VDB
%% Removes all replication policies for the VDB's shards
-spec teardown_replication(binary()) -> ok | {error, term()}.
teardown_replication(VdbName) when is_binary(VdbName) ->
    case barrel_shard_map:get_config(VdbName) of
        {ok, #{shard_count := ShardCount}} ->
            %% Delete policies for each shard
            lists:foreach(fun(ShardId) ->
                PolicyName = shard_policy_name(VdbName, ShardId),
                case barrel_rep_policy:get(PolicyName) of
                    {ok, _} ->
                        barrel_rep_policy:disable(PolicyName),
                        barrel_rep_policy:delete(PolicyName);
                    {error, not_found} ->
                        ok
                end
            end, lists:seq(0, ShardCount - 1)),
            ok;
        {error, _} = Err ->
            Err
    end.

%% @doc Get replication status for all shards in a VDB
-spec get_status(binary()) -> {ok, map()} | {error, term()}.
get_status(VdbName) when is_binary(VdbName) ->
    case barrel_shard_map:get_config(VdbName) of
        {ok, #{shard_count := ShardCount, placement := Placement}} ->
            ShardStatuses = lists:map(fun(ShardId) ->
                {ShardId, get_shard_replication_status(VdbName, ShardId)}
            end, lists:seq(0, ShardCount - 1)),

            %% Count enabled/disabled policies
            {EnabledCount, DisabledCount, NotFoundCount} = lists:foldl(
                fun({_, #{policy_found := false}}, {E, D, N}) -> {E, D, N + 1};
                   ({_, #{policy_enabled := true}}, {E, D, N}) -> {E + 1, D, N};
                   ({_, #{policy_enabled := false}}, {E, D, N}) -> {E, D + 1, N};
                   (_, Acc) -> Acc
                end,
                {0, 0, 0},
                ShardStatuses
            ),

            Status = #{
                vdb_name => VdbName,
                replica_factor => maps:get(replica_factor, Placement, 1),
                shard_count => ShardCount,
                policies => #{
                    enabled => EnabledCount,
                    disabled => DisabledCount,
                    not_created => NotFoundCount
                },
                shards => maps:from_list(ShardStatuses)
            },
            {ok, Status};
        {error, _} = Err ->
            Err
    end.

%% @doc Get replication status for a specific shard
-spec get_shard_status(binary(), non_neg_integer()) -> {ok, map()} | {error, term()}.
get_shard_status(VdbName, ShardId) when is_binary(VdbName), is_integer(ShardId) ->
    Status = get_shard_replication_status(VdbName, ShardId),
    {ok, Status}.

%% @doc Update shard assignments based on current peer state
%% Recalculates zone-aware placement and updates assignments
-spec update_assignments(binary()) -> ok | {error, term()}.
update_assignments(VdbName) when is_binary(VdbName) ->
    case barrel_shard_map:get_config(VdbName) of
        {ok, Config} ->
            Placement = maps:get(placement, Config, #{}),
            ShardCount = maps:get(shard_count, Config),

            %% Compute new placements for all shards
            lists:foreach(fun(ShardId) ->
                Placements = compute_placement(ShardId, Placement),
                #{primary := Primary, replicas := Replicas} = Placements,

                Assignment = #{
                    shard_id => ShardId,
                    primary => Primary,
                    replicas => Replicas,
                    status => active
                },
                barrel_shard_map:set_assignment(VdbName, ShardId, Assignment)
            end, lists:seq(0, ShardCount - 1)),
            ok;
        {error, _} = Err ->
            Err
    end.

%% @doc Compute optimal placement for a shard based on placement config
%% Returns primary and replica assignments with zone distribution
-spec compute_placement(non_neg_integer(), map()) -> placement_result().
compute_placement(ShardId, Placement) ->
    ReplicaFactor = maps:get(replica_factor, Placement, 1),
    PreferredZones = maps:get(zones, Placement, []),

    %% Get available peers by zone
    AvailablePeers = get_peers_by_zone(PreferredZones),

    case map_size(AvailablePeers) of
        0 ->
            %% No remote peers - local only
            #{
                shard_id => ShardId,
                primary => undefined,
                replicas => [],
                zone_distribution => #{}
            };
        _ ->
            %% Compute zone-aware placement
            compute_zone_aware_placement(ShardId, ReplicaFactor, AvailablePeers, PreferredZones)
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Setup replication for each shard
setup_shard_replication(VdbName, Config, Opts) ->
    ShardCount = maps:get(shard_count, Config),
    Placement = maps:get(placement, Config, #{}),
    Auth = maps:get(auth, Opts, #{}),

    Results = lists:map(fun(ShardId) ->
        setup_single_shard_replication(VdbName, ShardId, Placement, Auth)
    end, lists:seq(0, ShardCount - 1)),

    %% Check if all succeeded
    case lists:all(fun(R) -> R =:= ok end, Results) of
        true -> ok;
        false -> {error, partial_setup}
    end.

%% @private Setup replication for a single shard
setup_single_shard_replication(VdbName, ShardId, Placement, Auth) ->
    %% Compute placement for this shard
    PlacementResult = compute_placement(ShardId, Placement),
    #{replicas := Replicas} = PlacementResult,

    case Replicas of
        [] ->
            %% No replicas to set up
            ok;
        _ ->
            %% Create fanout replication policy
            ShardDb = barrel_shard_map:physical_db_name(VdbName, ShardId),
            PolicyName = shard_policy_name(VdbName, ShardId),

            %% Build target URLs for replicas
            Targets = lists:map(fun(ReplicaUrl) ->
                <<ReplicaUrl/binary, "/db/", ShardDb/binary>>
            end, Replicas),

            PolicyConfig = #{
                pattern => fanout,
                source => ShardDb,
                targets => Targets,
                mode => continuous,
                enabled => true
            },

            %% Add auth if provided
            PolicyConfig2 = case map_size(Auth) of
                0 -> PolicyConfig;
                _ -> PolicyConfig#{auth => Auth}
            end,

            %% Create or update policy
            case barrel_rep_policy:get(PolicyName) of
                {ok, _} ->
                    %% Policy exists - delete and recreate
                    barrel_rep_policy:disable(PolicyName),
                    barrel_rep_policy:delete(PolicyName),
                    create_and_enable_policy(PolicyName, PolicyConfig2);
                {error, not_found} ->
                    create_and_enable_policy(PolicyName, PolicyConfig2)
            end,

            %% Update shard assignment
            #{primary := Primary} = PlacementResult,
            Assignment = #{
                shard_id => ShardId,
                primary => Primary,
                replicas => Replicas,
                status => active
            },
            barrel_shard_map:set_assignment(VdbName, ShardId, Assignment),
            ok
    end.

%% @private Create and enable a replication policy
create_and_enable_policy(PolicyName, PolicyConfig) ->
    case barrel_rep_policy:create(PolicyName, PolicyConfig) of
        ok ->
            barrel_rep_policy:enable(PolicyName);
        {error, _} = Err ->
            Err
    end.

%% @private Get replication status for a single shard
get_shard_replication_status(VdbName, ShardId) ->
    PolicyName = shard_policy_name(VdbName, ShardId),

    BaseStatus = case barrel_rep_policy:status(PolicyName) of
        {ok, PolicyStatus} ->
            #{
                policy_found => true,
                policy_name => PolicyName,
                policy_enabled => maps:get(enabled, PolicyStatus, false),
                task_count => maps:get(task_count, PolicyStatus, 0),
                tasks => maps:get(tasks, PolicyStatus, [])
            };
        {error, not_found} ->
            #{
                policy_found => false,
                policy_name => PolicyName,
                policy_enabled => false
            }
    end,

    %% Add assignment info
    case barrel_shard_map:get_assignment(VdbName, ShardId) of
        {ok, Assignment} ->
            BaseStatus#{
                primary => maps:get(primary, Assignment, undefined),
                replicas => maps:get(replicas, Assignment, []),
                shard_status => maps:get(status, Assignment, active)
            };
        {error, not_found} ->
            BaseStatus#{
                primary => undefined,
                replicas => [],
                shard_status => unknown
            }
    end.

%% @private Generate policy name for a shard
shard_policy_name(VdbName, ShardId) ->
    <<"vdb:", VdbName/binary, ":shard:", (integer_to_binary(ShardId))/binary>>.

%% @private Get available peers grouped by zone
get_peers_by_zone(PreferredZones) ->
    {ok, AllPeers} = barrel_discovery:list_peers(#{status => active}),

    %% Group peers by zone
    ZonePeers = lists:foldl(
        fun(Peer, Acc) ->
            Url = maps:get(url, Peer),
            Zone = maps:get(zone, Peer, <<"default">>),

            %% Only include if zone is in preferred list (or list is empty = all zones)
            Include = case PreferredZones of
                [] -> true;
                _ -> lists:member(Zone, PreferredZones)
            end,

            case Include of
                true ->
                    maps:update_with(
                        Zone,
                        fun(Existing) -> [Url | Existing] end,
                        [Url],
                        Acc
                    );
                false ->
                    Acc
            end
        end,
        #{},
        AllPeers
    ),

    ZonePeers.

%% @private Compute zone-aware placement for replicas
%% Strategy: spread replicas across zones, round-robin within each zone
compute_zone_aware_placement(ShardId, ReplicaFactor, PeersByZone, _PreferredZones) ->
    Zones = maps:keys(PeersByZone),
    ZoneCount = length(Zones),

    case ZoneCount of
        0 ->
            %% No peers available
            #{
                shard_id => ShardId,
                primary => undefined,
                replicas => [],
                zone_distribution => #{}
            };
        _ ->
            %% Use shard ID to deterministically select starting zone
            StartZoneIdx = ShardId rem ZoneCount,

            %% Distribute replicas across zones
            {SelectedReplicas, ZoneDist} = select_replicas_across_zones(
                ReplicaFactor,
                Zones,
                StartZoneIdx,
                PeersByZone,
                ShardId
            ),

            %% First replica is considered primary
            {Primary, Replicas} = case SelectedReplicas of
                [] -> {undefined, []};
                [P | R] -> {P, R}
            end,

            #{
                shard_id => ShardId,
                primary => Primary,
                replicas => Replicas,
                zone_distribution => ZoneDist
            }
    end.

%% @private Select replicas across zones, distributing evenly
select_replicas_across_zones(ReplicaFactor, Zones, StartZoneIdx, PeersByZone, ShardId) ->
    ZoneCount = length(Zones),
    SortedZones = lists:sort(Zones),

    %% Calculate how many replicas per zone
    ReplicasPerZone = max(1, ReplicaFactor div ZoneCount),

    %% Select replicas from each zone in round-robin order
    {SelectedReplicas, ZoneDist} = lists:foldl(
        fun(Offset, {SelAcc, DistAcc}) ->
            ZoneIdx = (StartZoneIdx + Offset) rem ZoneCount,
            Zone = lists:nth(ZoneIdx + 1, SortedZones),
            ZonePeers = maps:get(Zone, PeersByZone, []),

            %% Deterministically select peers from this zone
            Selected = select_peers_from_zone(ZonePeers, ReplicasPerZone, ShardId, Offset),

            NewSelAcc = SelAcc ++ Selected,
            NewDistAcc = case Selected of
                [] -> DistAcc;
                _ -> DistAcc#{Zone => Selected}
            end,

            {NewSelAcc, NewDistAcc}
        end,
        {[], #{}},
        lists:seq(0, min(ReplicaFactor, ZoneCount) - 1)
    ),

    %% Trim to exact replica factor
    FinalReplicas = lists:sublist(SelectedReplicas, ReplicaFactor),

    {FinalReplicas, ZoneDist}.

%% @private Select peers from a zone using deterministic selection
select_peers_from_zone([], _Count, _ShardId, _Offset) ->
    [];
select_peers_from_zone(Peers, Count, ShardId, Offset) ->
    PeerCount = length(Peers),
    SortedPeers = lists:sort(Peers),

    %% Use shard ID and offset to deterministically select peers
    StartIdx = (ShardId + Offset) rem PeerCount,

    %% Select Count peers starting from StartIdx
    lists:sublist(
        rotate_list(SortedPeers, StartIdx),
        min(Count, PeerCount)
    ).

%% @private Rotate list by N positions
rotate_list(List, 0) -> List;
rotate_list([], _) -> [];
rotate_list(List, N) when N > 0 ->
    Len = length(List),
    Pos = N rem Len,
    {A, B} = lists:split(Pos, List),
    B ++ A.
