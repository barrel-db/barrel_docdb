%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_shard_map module
%%%
%%% Tests shard configuration, routing, ranges, and assignments.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_shard_map_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Common Test callbacks
%%====================================================================

all() ->
    [
        {group, lifecycle},
        {group, routing},
        {group, ranges},
        {group, assignments}
    ].

groups() ->
    [
        {lifecycle, [], [
            create_vdb,
            create_vdb_already_exists,
            delete_vdb,
            delete_vdb_not_found,
            exists_vdb,
            list_vdbs,
            get_config
        ]},
        {routing, [], [
            shard_for_doc_basic,
            shard_for_doc_consistency,
            shard_for_doc_distribution,
            physical_db_name,
            all_physical_dbs
        ]},
        {ranges, [], [
            get_ranges,
            set_range,
            ranges_cover_full_hash_space
        ]},
        {assignments, [], [
            get_assignment,
            set_assignment,
            get_all_assignments,
            get_status,
            set_status
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
    %% Clean up any test VDBs
    cleanup_test_vdbs(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    cleanup_test_vdbs(),
    ok.

cleanup_test_vdbs() ->
    {ok, VDBs} = barrel_shard_map:list(),
    lists:foreach(fun(Name) ->
        case binary:match(Name, <<"test_">>) of
            {0, _} -> barrel_shard_map:delete(Name);
            _ -> ok
        end
    end, VDBs).

%%====================================================================
%% Test Cases - Lifecycle
%%====================================================================

create_vdb(_Config) ->
    VdbName = <<"test_users">>,
    Opts = #{
        shard_count => 4,
        placement => #{
            replica_factor => 2,
            zones => [<<"us-east">>, <<"eu-west">>]
        }
    },
    ?assertEqual(ok, barrel_shard_map:create(VdbName, Opts)),
    ?assert(barrel_shard_map:exists(VdbName)),
    {ok, Config} = barrel_shard_map:get_config(VdbName),
    ?assertEqual(VdbName, maps:get(logical_db, Config)),
    ?assertEqual(4, maps:get(shard_count, Config)),
    ?assertEqual(phash2, maps:get(hash_function, Config)).

create_vdb_already_exists(_Config) ->
    VdbName = <<"test_dup">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{})),
    ?assertEqual({error, already_exists}, barrel_shard_map:create(VdbName, #{})).

delete_vdb(_Config) ->
    VdbName = <<"test_delete">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{})),
    ?assert(barrel_shard_map:exists(VdbName)),
    ?assertEqual(ok, barrel_shard_map:delete(VdbName)),
    ?assertNot(barrel_shard_map:exists(VdbName)).

delete_vdb_not_found(_Config) ->
    ?assertEqual({error, not_found}, barrel_shard_map:delete(<<"nonexistent">>)).

exists_vdb(_Config) ->
    VdbName = <<"test_exists">>,
    ?assertNot(barrel_shard_map:exists(VdbName)),
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{})),
    ?assert(barrel_shard_map:exists(VdbName)).

list_vdbs(_Config) ->
    %% Create a few VDBs
    ?assertEqual(ok, barrel_shard_map:create(<<"test_list_a">>, #{})),
    ?assertEqual(ok, barrel_shard_map:create(<<"test_list_b">>, #{})),
    ?assertEqual(ok, barrel_shard_map:create(<<"test_list_c">>, #{})),
    {ok, VDBs} = barrel_shard_map:list(),
    ?assert(lists:member(<<"test_list_a">>, VDBs)),
    ?assert(lists:member(<<"test_list_b">>, VDBs)),
    ?assert(lists:member(<<"test_list_c">>, VDBs)).

get_config(_Config) ->
    VdbName = <<"test_config">>,
    Opts = #{
        shard_count => 8,
        hash_function => phash2,
        placement => #{
            replica_factor => 3,
            zones => [<<"zone1">>, <<"zone2">>],
            constraints => [{min_per_zone, 1}]
        }
    },
    ?assertEqual(ok, barrel_shard_map:create(VdbName, Opts)),
    {ok, Config} = barrel_shard_map:get_config(VdbName),
    ?assertEqual(8, maps:get(shard_count, Config)),
    Placement = maps:get(placement, Config),
    ?assertEqual(3, maps:get(replica_factor, Placement)),
    ?assertEqual([<<"zone1">>, <<"zone2">>], maps:get(zones, Placement)).

%%====================================================================
%% Test Cases - Routing
%%====================================================================

shard_for_doc_basic(_Config) ->
    VdbName = <<"test_routing">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => 4})),
    %% Get shard for a document
    {ok, ShardId} = barrel_shard_map:shard_for_doc(VdbName, <<"doc1">>),
    ?assert(is_integer(ShardId)),
    ?assert(ShardId >= 0),
    ?assert(ShardId < 4).

shard_for_doc_consistency(_Config) ->
    VdbName = <<"test_consistency">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => 4})),
    %% Same doc ID should always go to same shard
    {ok, ShardId1} = barrel_shard_map:shard_for_doc(VdbName, <<"mydoc">>),
    {ok, ShardId2} = barrel_shard_map:shard_for_doc(VdbName, <<"mydoc">>),
    {ok, ShardId3} = barrel_shard_map:shard_for_doc(VdbName, <<"mydoc">>),
    ?assertEqual(ShardId1, ShardId2),
    ?assertEqual(ShardId2, ShardId3).

shard_for_doc_distribution(_Config) ->
    VdbName = <<"test_distribution">>,
    ShardCount = 4,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => ShardCount})),
    %% Generate many doc IDs and check distribution
    DocIds = [list_to_binary("doc" ++ integer_to_list(I)) || I <- lists:seq(1, 1000)],
    ShardCounts = lists:foldl(
        fun(DocId, Acc) ->
            {ok, ShardId} = barrel_shard_map:shard_for_doc(VdbName, DocId),
            maps:update_with(ShardId, fun(V) -> V + 1 end, 1, Acc)
        end,
        #{},
        DocIds
    ),
    %% Check all shards got some documents (distribution should be roughly even)
    lists:foreach(fun(ShardId) ->
        Count = maps:get(ShardId, ShardCounts, 0),
        %% Each shard should have at least 100 docs (25% of 1000 with some variance)
        ?assert(Count > 50, io_lib:format("Shard ~p only got ~p docs", [ShardId, Count]))
    end, lists:seq(0, ShardCount - 1)).

physical_db_name(_Config) ->
    ?assertEqual(<<"users_s0">>, barrel_shard_map:physical_db_name(<<"users">>, 0)),
    ?assertEqual(<<"users_s1">>, barrel_shard_map:physical_db_name(<<"users">>, 1)),
    ?assertEqual(<<"users_s99">>, barrel_shard_map:physical_db_name(<<"users">>, 99)),
    ?assertEqual(<<"my_db_s5">>, barrel_shard_map:physical_db_name(<<"my_db">>, 5)).

all_physical_dbs(_Config) ->
    VdbName = <<"test_all_dbs">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => 3})),
    {ok, Dbs} = barrel_shard_map:all_physical_dbs(VdbName),
    ?assertEqual([<<"test_all_dbs_s0">>, <<"test_all_dbs_s1">>, <<"test_all_dbs_s2">>], Dbs).

%%====================================================================
%% Test Cases - Ranges
%%====================================================================

get_ranges(_Config) ->
    VdbName = <<"test_ranges">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => 4})),
    {ok, Ranges} = barrel_shard_map:get_ranges(VdbName),
    ?assertEqual(4, length(Ranges)),
    %% Check each range has required fields
    lists:foreach(fun(Range) ->
        ?assert(maps:is_key(shard_id, Range)),
        ?assert(maps:is_key(start_hash, Range)),
        ?assert(maps:is_key(end_hash, Range))
    end, Ranges).

set_range(_Config) ->
    VdbName = <<"test_set_range">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => 2})),
    %% Modify a range (simulating a split)
    NewRange = #{
        shard_id => 0,
        start_hash => 0,
        end_hash => 1000000
    },
    ?assertEqual(ok, barrel_shard_map:set_range(VdbName, 0, NewRange)),
    {ok, Ranges} = barrel_shard_map:get_ranges(VdbName),
    Shard0Range = lists:keyfind(0, 1, [{maps:get(shard_id, R), R} || R <- Ranges]),
    {0, Range} = Shard0Range,
    ?assertEqual(0, maps:get(start_hash, Range)),
    ?assertEqual(1000000, maps:get(end_hash, Range)).

ranges_cover_full_hash_space(_Config) ->
    VdbName = <<"test_full_range">>,
    ShardCount = 8,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => ShardCount})),
    {ok, Ranges} = barrel_shard_map:get_ranges(VdbName),
    %% Sort ranges by start_hash
    SortedRanges = lists:sort(
        fun(A, B) -> maps:get(start_hash, A) =< maps:get(start_hash, B) end,
        Ranges
    ),
    %% First range should start at 0
    [FirstRange | _] = SortedRanges,
    ?assertEqual(0, maps:get(start_hash, FirstRange)),
    %% Last range should end at max hash (0xFFFFFFFF)
    LastRange = lists:last(SortedRanges),
    ?assertEqual(16#FFFFFFFF, maps:get(end_hash, LastRange)),
    %% Ranges should be contiguous (no gaps)
    check_contiguous(SortedRanges).

check_contiguous([]) -> ok;
check_contiguous([_]) -> ok;
check_contiguous([R1, R2 | Rest]) ->
    End1 = maps:get(end_hash, R1),
    Start2 = maps:get(start_hash, R2),
    ?assertEqual(End1 + 1, Start2, "Gap in ranges"),
    check_contiguous([R2 | Rest]).

%%====================================================================
%% Test Cases - Assignments
%%====================================================================

get_assignment(_Config) ->
    VdbName = <<"test_assign">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => 2})),
    {ok, Assignment} = barrel_shard_map:get_assignment(VdbName, 0),
    ?assertEqual(0, maps:get(shard_id, Assignment)),
    ?assertEqual(active, maps:get(status, Assignment)).

set_assignment(_Config) ->
    VdbName = <<"test_set_assign">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => 2})),
    NewAssignment = #{
        shard_id => 0,
        primary => <<"http://node1:8080">>,
        replicas => [<<"http://node2:8080">>, <<"http://node3:8080">>],
        status => active
    },
    ?assertEqual(ok, barrel_shard_map:set_assignment(VdbName, 0, NewAssignment)),
    {ok, Assignment} = barrel_shard_map:get_assignment(VdbName, 0),
    ?assertEqual(<<"http://node1:8080">>, maps:get(primary, Assignment)),
    ?assertEqual([<<"http://node2:8080">>, <<"http://node3:8080">>], maps:get(replicas, Assignment)).

get_all_assignments(_Config) ->
    VdbName = <<"test_all_assign">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => 3})),
    {ok, Assignments} = barrel_shard_map:get_all_assignments(VdbName),
    ?assertEqual(3, length(Assignments)),
    %% Should be sorted by shard_id
    ShardIds = [maps:get(shard_id, A) || A <- Assignments],
    ?assertEqual([0, 1, 2], ShardIds).

get_status(_Config) ->
    VdbName = <<"test_get_status">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => 2})),
    {ok, Status} = barrel_shard_map:get_status(VdbName, 0),
    ?assertEqual(active, Status).

set_status(_Config) ->
    VdbName = <<"test_set_status">>,
    ?assertEqual(ok, barrel_shard_map:create(VdbName, #{shard_count => 2})),
    ?assertEqual(ok, barrel_shard_map:set_status(VdbName, 0, splitting)),
    {ok, Status} = barrel_shard_map:get_status(VdbName, 0),
    ?assertEqual(splitting, Status),
    %% Change to another status
    ?assertEqual(ok, barrel_shard_map:set_status(VdbName, 0, readonly)),
    {ok, Status2} = barrel_shard_map:get_status(VdbName, 0),
    ?assertEqual(readonly, Status2).
