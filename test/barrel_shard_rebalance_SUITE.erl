%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_shard_rebalance
%%%-------------------------------------------------------------------
-module(barrel_shard_rebalance_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% CT callbacks
-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    split_shard_basic/1,
    split_shard_with_documents/1,
    split_shard_progress_callback/1,
    split_shard_not_active/1,
    merge_shards_basic/1,
    merge_shards_with_documents/1,
    merge_shards_not_adjacent/1,
    can_merge_adjacent/1,
    can_merge_not_adjacent/1,
    estimate_migration/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        split_shard_basic,
        split_shard_with_documents,
        split_shard_progress_callback,
        split_shard_not_active,
        merge_shards_basic,
        merge_shards_with_documents,
        merge_shards_not_adjacent,
        can_merge_adjacent,
        can_merge_not_adjacent,
        estimate_migration
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Clean up any existing test VDBs
    cleanup_test_vdbs(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    cleanup_test_vdbs(),
    ok.

%%====================================================================
%% Test Cases - Split
%%====================================================================

split_shard_basic(_Config) ->
    VdbName = <<"test_split_basic">>,

    %% Create VDB with 2 shards
    ok = barrel_vdb:create(VdbName, #{shard_count => 2}),

    %% Verify initial state
    {ok, Info1} = barrel_vdb:info(VdbName),
    ?assertEqual(2, maps:get(shard_count, Info1)),

    %% Split shard 0
    {ok, NewShardId} = barrel_shard_rebalance:split_shard(VdbName, 0),

    %% Verify new shard was created
    ?assert(NewShardId >= 2),

    %% Verify shard count increased
    {ok, Info2} = barrel_vdb:info(VdbName),
    ?assertEqual(3, maps:get(shard_count, Info2)),

    %% Verify both shards are active
    {ok, active} = barrel_shard_map:get_status(VdbName, 0),
    {ok, active} = barrel_shard_map:get_status(VdbName, NewShardId),

    %% Verify ranges don't overlap
    {ok, Ranges} = barrel_shard_map:get_ranges(VdbName),
    verify_no_range_overlap(Ranges),

    ok.

split_shard_with_documents(_Config) ->
    VdbName = <<"test_split_docs">>,

    %% Create VDB with 2 shards
    ok = barrel_vdb:create(VdbName, #{shard_count => 2}),

    %% Insert test documents
    Docs = [#{<<"id">> => <<"doc", (integer_to_binary(I))/binary>>,
              <<"value">> => I} || I <- lists:seq(1, 50)],
    lists:foreach(fun(Doc) ->
        {ok, _} = barrel_vdb:put_doc(VdbName, Doc)
    end, Docs),

    %% Verify initial doc count
    {ok, Info1} = barrel_vdb:info(VdbName),
    ?assertEqual(50, maps:get(total_docs, Info1)),

    %% Split shard 0
    {ok, _NewShardId} = barrel_shard_rebalance:split_shard(VdbName, 0),

    %% Verify all documents still accessible
    {ok, Info2} = barrel_vdb:info(VdbName),
    ?assertEqual(50, maps:get(total_docs, Info2)),

    %% Verify each document can be retrieved
    lists:foreach(fun(Doc) ->
        DocId = maps:get(<<"id">>, Doc),
        {ok, Retrieved} = barrel_vdb:get_doc(VdbName, DocId),
        ?assertEqual(maps:get(<<"value">>, Doc), maps:get(<<"value">>, Retrieved))
    end, Docs),

    ok.

split_shard_progress_callback(_Config) ->
    VdbName = <<"test_split_progress">>,

    %% Create VDB and add some docs
    ok = barrel_vdb:create(VdbName, #{shard_count => 2}),
    lists:foreach(fun(I) ->
        Doc = #{<<"id">> => <<"doc", (integer_to_binary(I))/binary>>, <<"v">> => I},
        barrel_vdb:put_doc(VdbName, Doc)
    end, lists:seq(1, 20)),

    %% Track progress via callback
    Self = self(),
    ProgressCb = fun(Info) ->
        Self ! {progress, Info},
        ok
    end,

    %% Split with progress callback
    {ok, _} = barrel_shard_rebalance:split_shard(VdbName, 0, #{
        progress_callback => ProgressCb,
        batch_size => 5
    }),

    %% Verify we received progress updates
    receive
        {progress, #{phase := preparing}} -> ok
    after 1000 ->
        ct:fail("No preparing phase received")
    end,

    ok.

split_shard_not_active(_Config) ->
    VdbName = <<"test_split_not_active">>,

    %% Create VDB
    ok = barrel_vdb:create(VdbName, #{shard_count => 2}),

    %% Manually set shard to splitting
    ok = barrel_shard_map:set_status(VdbName, 0, splitting),

    %% Try to split - should fail
    {error, {shard_not_active, splitting}} = barrel_shard_rebalance:split_shard(VdbName, 0),

    %% Reset for cleanup
    ok = barrel_shard_map:set_status(VdbName, 0, active),

    ok.

%%====================================================================
%% Test Cases - Merge
%%====================================================================

merge_shards_basic(_Config) ->
    VdbName = <<"test_merge_basic">>,

    %% Create VDB with 4 shards
    ok = barrel_vdb:create(VdbName, #{shard_count => 4}),

    %% Verify initial state
    {ok, Info1} = barrel_vdb:info(VdbName),
    ?assertEqual(4, maps:get(shard_count, Info1)),

    %% Find two adjacent shards
    {ok, Ranges} = barrel_shard_map:get_ranges(VdbName),
    {Shard1, Shard2} = find_adjacent_shards(Ranges),

    %% Merge them
    ok = barrel_shard_rebalance:merge_shards(VdbName, Shard1, Shard2),

    %% Verify shard count decreased
    {ok, Info2} = barrel_vdb:info(VdbName),
    ?assertEqual(3, maps:get(shard_count, Info2)),

    %% Verify merged shard is active
    {ok, active} = barrel_shard_map:get_status(VdbName, Shard1),

    %% Verify second shard no longer exists
    {error, not_found} = barrel_shard_map:get_status(VdbName, Shard2),

    ok.

merge_shards_with_documents(_Config) ->
    VdbName = <<"test_merge_docs">>,

    %% Create VDB with 4 shards
    ok = barrel_vdb:create(VdbName, #{shard_count => 4}),

    %% Insert test documents
    Docs = [#{<<"id">> => <<"doc", (integer_to_binary(I))/binary>>,
              <<"value">> => I} || I <- lists:seq(1, 50)],
    lists:foreach(fun(Doc) ->
        {ok, _} = barrel_vdb:put_doc(VdbName, Doc)
    end, Docs),

    %% Find adjacent shards
    {ok, Ranges} = barrel_shard_map:get_ranges(VdbName),
    {Shard1, Shard2} = find_adjacent_shards(Ranges),

    %% Merge them
    ok = barrel_shard_rebalance:merge_shards(VdbName, Shard1, Shard2),

    %% Verify all documents still accessible
    {ok, Info} = barrel_vdb:info(VdbName),
    ?assertEqual(50, maps:get(total_docs, Info)),

    %% Verify each document can be retrieved
    lists:foreach(fun(Doc) ->
        DocId = maps:get(<<"id">>, Doc),
        {ok, Retrieved} = barrel_vdb:get_doc(VdbName, DocId),
        ?assertEqual(maps:get(<<"value">>, Doc), maps:get(<<"value">>, Retrieved))
    end, Docs),

    ok.

merge_shards_not_adjacent(_Config) ->
    VdbName = <<"test_merge_not_adjacent">>,

    %% Create VDB with 4 shards
    ok = barrel_vdb:create(VdbName, #{shard_count => 4}),

    %% Try to merge non-adjacent shards (0 and 2)
    Result = barrel_shard_rebalance:merge_shards(VdbName, 0, 2),

    %% Should fail
    ?assertMatch({error, _}, Result),

    ok.

%%====================================================================
%% Test Cases - Can Merge
%%====================================================================

can_merge_adjacent(_Config) ->
    VdbName = <<"test_can_merge_adj">>,

    %% Create VDB with 4 shards
    ok = barrel_vdb:create(VdbName, #{shard_count => 4}),

    %% Find adjacent shards
    {ok, Ranges} = barrel_shard_map:get_ranges(VdbName),
    {Shard1, Shard2} = find_adjacent_shards(Ranges),

    %% Should be able to merge
    {ok, true} = barrel_shard_rebalance:can_merge(VdbName, Shard1, Shard2),

    ok.

can_merge_not_adjacent(_Config) ->
    VdbName = <<"test_can_merge_not_adj">>,

    %% Create VDB with 4 shards
    ok = barrel_vdb:create(VdbName, #{shard_count => 4}),

    %% Shards 0 and 2 are not adjacent in a 4-shard setup
    {ok, false} = barrel_shard_rebalance:can_merge(VdbName, 0, 2),

    ok.

%%====================================================================
%% Test Cases - Estimate Migration
%%====================================================================

estimate_migration(_Config) ->
    VdbName = <<"test_estimate">>,

    %% Create VDB with 4 shards
    ok = barrel_vdb:create(VdbName, #{shard_count => 4}),

    %% Insert documents to shard 0
    Shard0Db = barrel_shard_map:physical_db_name(VdbName, 0),
    lists:foreach(fun(I) ->
        Doc = #{<<"id">> => <<"est", (integer_to_binary(I))/binary>>, <<"v">> => I},
        barrel_docdb:put_doc(Shard0Db, Doc, #{})
    end, lists:seq(1, 10)),

    %% Estimate migration from shard 0 to shard 1
    {ok, Count} = barrel_shard_rebalance:estimate_migration(VdbName, 0, 1),

    %% Should be approximately 10 (the docs we inserted)
    ?assert(Count >= 10),

    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

cleanup_test_vdbs() ->
    {ok, Vdbs} = barrel_vdb:list(),
    lists:foreach(fun(VdbName) ->
        case binary:match(VdbName, <<"test_">>) of
            {0, _} -> barrel_vdb:delete(VdbName);
            _ -> ok
        end
    end, Vdbs).

verify_no_range_overlap(Ranges) ->
    %% Sort by start_hash
    Sorted = lists:sort(
        fun(#{start_hash := A}, #{start_hash := B}) -> A =< B end,
        Ranges
    ),
    verify_no_overlap(Sorted).

verify_no_overlap([]) -> ok;
verify_no_overlap([_]) -> ok;
verify_no_overlap([#{end_hash := End1} | [#{start_hash := Start2} | _] = Rest]) ->
    ?assert(End1 < Start2 orelse End1 + 1 =:= Start2,
            io_lib:format("Ranges overlap: end=~p, next_start=~p", [End1, Start2])),
    verify_no_overlap(Rest).

find_adjacent_shards(Ranges) ->
    %% Sort by start_hash
    Sorted = lists:sort(
        fun(#{start_hash := A}, #{start_hash := B}) -> A =< B end,
        Ranges
    ),
    %% Find first adjacent pair
    find_first_adjacent(Sorted).

find_first_adjacent([#{shard_id := Id1, end_hash := End1},
                     #{shard_id := Id2, start_hash := Start2} | _])
  when End1 + 1 =:= Start2 ->
    {Id1, Id2};
find_first_adjacent([_ | Rest]) ->
    find_first_adjacent(Rest);
find_first_adjacent([]) ->
    ct:fail("No adjacent shards found").
