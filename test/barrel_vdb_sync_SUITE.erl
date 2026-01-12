%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_vdb_sync module
%%%
%%% Tests VDB configuration synchronization across nodes.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb_sync_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Common Test callbacks
%%====================================================================

all() ->
    [
        {group, meta_database},
        {group, broadcast},
        {group, cluster_queries}
    ].

groups() ->
    [
        {meta_database, [], [
            meta_db_exists,
            store_in_meta,
            list_from_meta
        ]},
        {broadcast, [], [
            broadcast_config,
            broadcast_on_create
        ]},
        {cluster_queries, [], [
            list_cluster_vdbs,
            get_vdb_nodes
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
%% Test Cases - Meta Database
%%====================================================================

meta_db_exists(_Config) ->
    %% Meta database should be created by barrel_vdb_sync on startup
    {ok, Info} = barrel_docdb:db_info(<<"_barrel_vdb_meta">>),
    ?assert(is_map(Info)).

store_in_meta(_Config) ->
    %% Create a VDB and check it's stored in meta
    VdbName = <<"test_sync_store">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    %% Broadcast config (may already be done async)
    ?assertEqual(ok, barrel_vdb_sync:broadcast_config(VdbName)),
    %% Check it's in meta database
    DocId = <<"vdb:", VdbName/binary>>,
    case barrel_docdb:get_doc(<<"_barrel_vdb_meta">>, DocId, #{}) of
        {ok, Doc} ->
            ?assertEqual(VdbName, maps:get(<<"vdb_name">>, Doc));
        {error, not_found} ->
            %% Check with find
            {ok, Docs, _} = barrel_docdb:find(<<"_barrel_vdb_meta">>, #{where => []}, #{}),
            ct:pal("Meta docs: ~p", [Docs]),
            ?assert(false)
    end.

list_from_meta(_Config) ->
    %% Create VDBs and list from meta
    ?assertEqual(ok, barrel_vdb:create(<<"test_sync_list_a">>, #{shard_count => 2})),
    ?assertEqual(ok, barrel_vdb:create(<<"test_sync_list_b">>, #{shard_count => 2})),
    %% Broadcast configs
    barrel_vdb_sync:broadcast_config(<<"test_sync_list_a">>),
    barrel_vdb_sync:broadcast_config(<<"test_sync_list_b">>),
    %% List should include both
    {ok, ClusterVdbs} = barrel_vdb_sync:list_cluster_vdbs(),
    ?assert(lists:member(<<"test_sync_list_a">>, ClusterVdbs)),
    ?assert(lists:member(<<"test_sync_list_b">>, ClusterVdbs)).

%%====================================================================
%% Test Cases - Broadcast
%%====================================================================

broadcast_config(_Config) ->
    %% Create VDB
    VdbName = <<"test_sync_broadcast">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 3})),
    %% Broadcast config
    ?assertEqual(ok, barrel_vdb_sync:broadcast_config(VdbName)),
    %% Verify config is in meta database
    {ok, ClusterVdbs} = barrel_vdb_sync:list_cluster_vdbs(),
    ?assert(lists:member(VdbName, ClusterVdbs)).

broadcast_on_create(_Config) ->
    %% VDB creation should auto-broadcast config
    VdbName = <<"test_sync_autocreate">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    %% Give async broadcast time to complete
    timer:sleep(500),
    %% Should be in cluster list
    {ok, ClusterVdbs} = barrel_vdb_sync:list_cluster_vdbs(),
    ?assert(lists:member(VdbName, ClusterVdbs)).

%%====================================================================
%% Test Cases - Cluster Queries
%%====================================================================

list_cluster_vdbs(_Config) ->
    %% Create some VDBs
    ?assertEqual(ok, barrel_vdb:create(<<"test_cluster_a">>, #{shard_count => 2})),
    ?assertEqual(ok, barrel_vdb:create(<<"test_cluster_b">>, #{shard_count => 2})),
    %% List cluster VDBs should include local VDBs
    {ok, ClusterVdbs} = barrel_vdb_sync:list_cluster_vdbs(),
    ?assert(lists:member(<<"test_cluster_a">>, ClusterVdbs)),
    ?assert(lists:member(<<"test_cluster_b">>, ClusterVdbs)).

get_vdb_nodes(_Config) ->
    %% Without remote peers, should return empty list
    {ok, Nodes} = barrel_vdb_sync:get_vdb_nodes(<<"any_vdb">>),
    ?assert(is_list(Nodes)).
