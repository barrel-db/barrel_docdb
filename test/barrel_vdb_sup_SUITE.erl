%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_vdb_sup and barrel_vdb_registry modules
%%%
%%% Tests VDB supervisor and registry functionality.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb_sup_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Common Test callbacks
%%====================================================================

all() ->
    [
        {group, supervisor},
        {group, registry}
    ].

groups() ->
    [
        {supervisor, [], [
            supervisor_started,
            supervisor_children
        ]},
        {registry, [], [
            registry_started,
            register_vdb,
            unregister_vdb,
            list_registered,
            registry_refresh,
            registry_survives_vdb_lifecycle
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
    {ok, VDBs} = barrel_vdb:list(),
    lists:foreach(fun(Name) ->
        case binary:match(Name, <<"test_">>) of
            {0, _} -> barrel_vdb:delete(Name);
            _ -> ok
        end
    end, VDBs).

%%====================================================================
%% Test Cases - Supervisor
%%====================================================================

supervisor_started(_Config) ->
    %% VDB supervisor should be running
    ?assert(is_pid(whereis(barrel_vdb_sup))),
    %% Should be registered
    Pid = whereis(barrel_vdb_sup),
    ?assert(is_process_alive(Pid)).

supervisor_children(_Config) ->
    %% Get supervisor children
    Children = supervisor:which_children(barrel_vdb_sup),
    ?assert(is_list(Children)),
    %% Should have registry child
    ?assert(lists:keymember(barrel_vdb_registry, 1, Children)),
    %% Registry should be running
    {barrel_vdb_registry, RegistryPid, worker, _} = lists:keyfind(barrel_vdb_registry, 1, Children),
    ?assert(is_pid(RegistryPid)),
    ?assert(is_process_alive(RegistryPid)).

%%====================================================================
%% Test Cases - Registry
%%====================================================================

registry_started(_Config) ->
    %% Registry should be running
    ?assert(is_pid(whereis(barrel_vdb_registry))),
    Pid = whereis(barrel_vdb_registry),
    ?assert(is_process_alive(Pid)).

register_vdb(_Config) ->
    VdbName = <<"test_reg">>,
    %% Create VDB (should auto-register)
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    %% Should be registered
    ?assert(barrel_vdb_registry:is_registered(VdbName)),
    %% Should have cached info
    {ok, Info} = barrel_vdb_registry:get_vdb_info(VdbName),
    ?assert(is_map(Info)),
    ?assert(maps:is_key(config, Info)),
    ?assert(maps:is_key(registered_at, Info)).

unregister_vdb(_Config) ->
    VdbName = <<"test_unreg">>,
    %% Create and then delete
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    ?assert(barrel_vdb_registry:is_registered(VdbName)),
    ?assertEqual(ok, barrel_vdb:delete(VdbName)),
    %% Should no longer be registered
    ?assertNot(barrel_vdb_registry:is_registered(VdbName)),
    ?assertEqual({error, not_found}, barrel_vdb_registry:get_vdb_info(VdbName)).

list_registered(_Config) ->
    %% Create a few VDBs
    ?assertEqual(ok, barrel_vdb:create(<<"test_list_a">>, #{shard_count => 2})),
    ?assertEqual(ok, barrel_vdb:create(<<"test_list_b">>, #{shard_count => 2})),
    %% List should include both
    Registered = barrel_vdb_registry:list_registered(),
    ?assert(lists:member(<<"test_list_a">>, Registered)),
    ?assert(lists:member(<<"test_list_b">>, Registered)).

registry_refresh(_Config) ->
    VdbName = <<"test_refresh">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    ?assert(barrel_vdb_registry:is_registered(VdbName)),
    %% Refresh should reload from shard map
    ?assertEqual(ok, barrel_vdb_registry:refresh()),
    %% Should still be registered after refresh
    ?assert(barrel_vdb_registry:is_registered(VdbName)).

registry_survives_vdb_lifecycle(_Config) ->
    %% Test full lifecycle with registry tracking
    VdbName = <<"test_lifecycle">>,

    %% Create
    ?assertNot(barrel_vdb_registry:is_registered(VdbName)),
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    ?assert(barrel_vdb_registry:is_registered(VdbName)),

    %% Add some docs (registry should still work)
    {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => <<"doc1">>}),
    ?assert(barrel_vdb_registry:is_registered(VdbName)),

    %% Delete
    ?assertEqual(ok, barrel_vdb:delete(VdbName)),
    ?assertNot(barrel_vdb_registry:is_registered(VdbName)),

    %% Recreate
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 4})),
    ?assert(barrel_vdb_registry:is_registered(VdbName)),
    %% New config should be cached
    {ok, Info} = barrel_vdb_registry:get_vdb_info(VdbName),
    #{config := Config} = Info,
    ?assertEqual(4, maps:get(shard_count, Config)).
