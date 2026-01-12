%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_vdb_replication module
%%%
%%% Tests shard replication setup for Virtual Databases.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb_replication_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Common Test callbacks
%%====================================================================

all() ->
    [
        {group, placement},
        {group, replication_lifecycle},
        {group, status}
    ].

groups() ->
    [
        {placement, [], [
            compute_placement_no_peers,
            compute_placement_single_zone,
            compute_placement_multi_zone,
            compute_placement_deterministic
        ]},
        {replication_lifecycle, [], [
            setup_replication_no_replicas,
            setup_replication_with_replicas,
            teardown_replication,
            vdb_create_with_replication,
            vdb_delete_cleans_replication
        ]},
        {status, [], [
            get_status_no_replication,
            get_status_with_replication,
            get_shard_status
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_docdb),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    cleanup_test_vdbs(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    cleanup_test_vdbs(),
    ok.

cleanup_test_vdbs() ->
    {ok, VDBs} = barrel_vdb:list(),
    lists:foreach(fun(Name) ->
        case binary:match(Name, <<"test_">>) of
            {0, _} -> barrel_vdb:delete(Name);
            _ -> ok
        end
    end, VDBs).

%%====================================================================
%% Test Cases - Placement
%%====================================================================

compute_placement_no_peers(_Config) ->
    %% When no peers are available, placement should return empty replicas
    Placement = barrel_vdb_replication:compute_placement(0, #{
        replica_factor => 2,
        zones => []
    }),
    ?assertEqual(0, maps:get(shard_id, Placement)),
    ?assertEqual(undefined, maps:get(primary, Placement)),
    ?assertEqual([], maps:get(replicas, Placement)).

compute_placement_single_zone(_Config) ->
    %% With single zone peers, placement should work within that zone
    Placement = barrel_vdb_replication:compute_placement(0, #{
        replica_factor => 1,
        zones => [<<"us-east">>]
    }),
    %% Without actual peers, should still return structure
    ?assert(is_map(Placement)),
    ?assertEqual(0, maps:get(shard_id, Placement)).

compute_placement_multi_zone(_Config) ->
    %% Multi-zone placement should work with preferred zones
    Placement = barrel_vdb_replication:compute_placement(1, #{
        replica_factor => 3,
        zones => [<<"us-east">>, <<"eu-west">>]
    }),
    ?assert(is_map(Placement)),
    ?assertEqual(1, maps:get(shard_id, Placement)),
    ?assert(maps:is_key(zone_distribution, Placement)).

compute_placement_deterministic(_Config) ->
    %% Same inputs should produce same placement
    Config = #{replica_factor => 2, zones => []},
    Placement1 = barrel_vdb_replication:compute_placement(5, Config),
    Placement2 = barrel_vdb_replication:compute_placement(5, Config),
    ?assertEqual(Placement1, Placement2),
    %% Different shard IDs should produce different starting points
    PlacementA = barrel_vdb_replication:compute_placement(0, Config),
    PlacementB = barrel_vdb_replication:compute_placement(3, Config),
    ?assertEqual(0, maps:get(shard_id, PlacementA)),
    ?assertEqual(3, maps:get(shard_id, PlacementB)).

%%====================================================================
%% Test Cases - Replication Lifecycle
%%====================================================================

setup_replication_no_replicas(_Config) ->
    %% Create VDB with replica_factor = 1 (no replication)
    VdbName = <<"test_rep_none">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{
        shard_count => 2,
        placement => #{replica_factor => 1}
    })),
    %% Setup replication should succeed but do nothing
    ?assertEqual(ok, barrel_vdb_replication:setup_replication(VdbName, #{})),
    %% No policies should be created (but we don't create them when replica_factor=1)
    {ok, Status} = barrel_vdb_replication:get_status(VdbName),
    Policies = maps:get(policies, Status),
    %% With replica_factor=1, policies count should reflect no policies exist
    ?assertEqual(0, maps:get(enabled, Policies)),
    %% Not_created count is 0 because we never attempted to create them
    %% (they're only created when replica_factor > 1)
    TotalPolicies = maps:get(enabled, Policies) + maps:get(disabled, Policies) + maps:get(not_created, Policies),
    %% Total should match shard count
    ?assertEqual(2, TotalPolicies).

setup_replication_with_replicas(_Config) ->
    %% Create VDB with replica_factor > 1
    VdbName = <<"test_rep_multi">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{
        shard_count => 2,
        placement => #{replica_factor => 2}
    })),
    %% Without actual remote peers, setup should succeed but no policies active
    ?assertEqual(ok, barrel_vdb_replication:setup_replication(VdbName, #{})),
    %% Status should reflect the setup
    {ok, Status} = barrel_vdb_replication:get_status(VdbName),
    ?assertEqual(VdbName, maps:get(vdb_name, Status)),
    ?assertEqual(2, maps:get(replica_factor, Status)),
    ?assertEqual(2, maps:get(shard_count, Status)).

teardown_replication(_Config) ->
    %% Create VDB and setup replication
    VdbName = <<"test_rep_teardown">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{
        shard_count => 2,
        placement => #{replica_factor => 2}
    })),
    barrel_vdb_replication:setup_replication(VdbName, #{}),
    %% Teardown should succeed
    ?assertEqual(ok, barrel_vdb_replication:teardown_replication(VdbName)),
    %% Status should show no policies
    {ok, Status} = barrel_vdb_replication:get_status(VdbName),
    Policies = maps:get(policies, Status),
    ?assertEqual(0, maps:get(enabled, Policies)).

vdb_create_with_replication(_Config) ->
    %% VDB create with replica_factor > 1 should auto-setup replication
    VdbName = <<"test_rep_auto">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{
        shard_count => 3,
        placement => #{replica_factor => 2}
    })),
    %% VDB should exist
    ?assert(barrel_vdb:exists(VdbName)),
    %% Replication status should be available
    {ok, Status} = barrel_vdb_replication:get_status(VdbName),
    ?assertEqual(2, maps:get(replica_factor, Status)).

vdb_delete_cleans_replication(_Config) ->
    %% Create VDB with replication
    VdbName = <<"test_rep_cleanup">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{
        shard_count => 2,
        placement => #{replica_factor => 2}
    })),
    ?assert(barrel_vdb:exists(VdbName)),
    %% Delete VDB
    ?assertEqual(ok, barrel_vdb:delete(VdbName)),
    ?assertNot(barrel_vdb:exists(VdbName)),
    %% Replication status should fail (VDB gone)
    ?assertEqual({error, not_found}, barrel_vdb_replication:get_status(VdbName)).

%%====================================================================
%% Test Cases - Status
%%====================================================================

get_status_no_replication(_Config) ->
    %% VDB without replication should still have status
    VdbName = <<"test_status_none">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{
        shard_count => 2
    })),
    {ok, Status} = barrel_vdb_replication:get_status(VdbName),
    ?assertEqual(VdbName, maps:get(vdb_name, Status)),
    ?assertEqual(1, maps:get(replica_factor, Status)),
    ?assertEqual(2, maps:get(shard_count, Status)),
    %% Should have shard status
    Shards = maps:get(shards, Status),
    ?assertEqual(2, map_size(Shards)).

get_status_with_replication(_Config) ->
    %% VDB with replication should show policy status
    VdbName = <<"test_status_rep">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{
        shard_count => 3,
        placement => #{replica_factor => 2}
    })),
    {ok, Status} = barrel_vdb_replication:get_status(VdbName),
    ?assertEqual(VdbName, maps:get(vdb_name, Status)),
    ?assertEqual(2, maps:get(replica_factor, Status)),
    ?assertEqual(3, maps:get(shard_count, Status)),
    %% Should have policies info
    ?assert(maps:is_key(policies, Status)),
    %% Should have shards info
    Shards = maps:get(shards, Status),
    ?assertEqual(3, map_size(Shards)).

get_shard_status(_Config) ->
    %% Get status for individual shard
    VdbName = <<"test_shard_status">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 4})),
    %% Get status for each shard
    lists:foreach(fun(ShardId) ->
        {ok, Status} = barrel_vdb_replication:get_shard_status(VdbName, ShardId),
        ?assert(is_map(Status)),
        ?assert(maps:is_key(policy_found, Status)),
        ?assert(maps:is_key(shard_status, Status))
    end, lists:seq(0, 3)).
