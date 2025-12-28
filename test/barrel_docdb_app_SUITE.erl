%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb application scaffold
%%%
%%% Tests the basic application startup, supervision tree, and
%%% database server lifecycle.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_app_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    app_starts_and_stops/1,
    supervisor_starts/1,
    db_sup_starts/1,
    db_server_lifecycle/1,
    db_server_info/1,
    db_server_store_refs/1,
    multiple_db_servers/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, application}, {group, db_server}].

groups() ->
    [
        {application, [sequence], [
            app_starts_and_stops,
            supervisor_starts,
            db_sup_starts
        ]},
        {db_server, [sequence], [
            db_server_lifecycle,
            db_server_info,
            db_server_store_refs,
            multiple_db_servers
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Ensure application is stopped before each test
    _ = application:stop(barrel_docdb),
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% Stop application after each test
    _ = application:stop(barrel_docdb),
    ok.

%%====================================================================
%% Test Cases - Application
%%====================================================================

%% @doc Test that the application starts and stops correctly
app_starts_and_stops(_Config) ->
    %% Start the application
    {ok, _} = application:ensure_all_started(barrel_docdb),

    %% Verify it's running
    Apps = application:which_applications(),
    ?assert(lists:keymember(barrel_docdb, 1, Apps)),

    %% Stop the application
    ok = application:stop(barrel_docdb),

    %% Verify it's stopped
    Apps2 = application:which_applications(),
    ?assertNot(lists:keymember(barrel_docdb, 1, Apps2)),

    ok.

%% @doc Test that the main supervisor starts
supervisor_starts(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    %% Check supervisor is registered
    ?assertNotEqual(undefined, whereis(barrel_docdb_sup)),

    %% Check it's a supervisor
    Pid = whereis(barrel_docdb_sup),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    ok.

%% @doc Test that the database supervisor starts
db_sup_starts(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    %% Check db_sup is registered
    ?assertNotEqual(undefined, whereis(barrel_db_sup)),

    %% Check it's alive
    Pid = whereis(barrel_db_sup),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    ok.

%%====================================================================
%% Test Cases - DB Server
%%====================================================================

%% @doc Test database server lifecycle
db_server_lifecycle(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    %% Start a database server
    DbName = <<"test_db">>,
    TestDir = "/tmp/barrel_lifecycle_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    Config = #{data_dir => TestDir},
    {ok, Pid} = barrel_db_sup:start_db(DbName, Config),

    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Stop the database server
    ok = barrel_db_server:stop(Pid),

    %% Give it time to terminate
    timer:sleep(100),
    ?assertNot(is_process_alive(Pid)),

    %% Cleanup
    os:cmd("rm -rf " ++ TestDir),

    ok.

%% @doc Test database server info
db_server_info(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"info_test_db">>,
    TestDir = "/tmp/barrel_info_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    Config = #{data_dir => TestDir},
    {ok, Pid} = barrel_db_sup:start_db(DbName, Config),

    %% Get info
    {ok, Info} = barrel_db_server:info(Pid),

    ?assertEqual(DbName, maps:get(name, Info)),
    ?assertEqual(Config, maps:get(config, Info)),
    ?assertEqual(Pid, maps:get(pid, Info)),
    ?assert(is_list(maps:get(db_path, Info))),

    %% Cleanup
    ok = barrel_db_server:stop(Pid),
    os:cmd("rm -rf " ++ TestDir),

    ok.

%% @doc Test database server store references are accessible
db_server_store_refs(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"store_refs_test_db">>,
    TestDir = "/tmp/barrel_store_refs_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    Config = #{data_dir => TestDir},
    {ok, Pid} = barrel_db_sup:start_db(DbName, Config),

    %% Get document store ref
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
    ?assert(is_map(StoreRef)),
    ?assert(maps:is_key(ref, StoreRef)),
    ?assert(maps:is_key(path, StoreRef)),

    %% Get attachment store ref
    {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
    ?assert(is_map(AttRef)),
    ?assert(maps:is_key(ref, AttRef)),
    ?assert(maps:is_key(path, AttRef)),

    %% Verify stores are in expected paths
    DocPath = maps:get(path, StoreRef),
    AttPath = maps:get(path, AttRef),
    ?assert(string:find(DocPath, "docs") =/= nomatch),
    ?assert(string:find(AttPath, "attachments") =/= nomatch),

    %% Test document store works - write and read
    ok = barrel_store_rocksdb:put(StoreRef, <<"test_key">>, <<"test_value">>),
    {ok, <<"test_value">>} = barrel_store_rocksdb:get(StoreRef, <<"test_key">>),

    %% Test attachment store works - write and read
    {ok, _AttInfo} = barrel_att_store:put(AttRef, DbName, <<"doc1">>, <<"file.txt">>, <<"content">>),
    {ok, <<"content">>} = barrel_att_store:get(AttRef, DbName, <<"doc1">>, <<"file.txt">>),

    %% Cleanup
    ok = barrel_db_server:stop(Pid),
    os:cmd("rm -rf " ++ TestDir),

    ok.

%% @doc Test multiple database servers
multiple_db_servers(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    BaseDir = "/tmp/barrel_multi_" ++ integer_to_list(erlang:system_time(millisecond)),

    %% Start multiple databases
    Dbs = [
        {<<"db1">>, #{data_dir => BaseDir ++ "/db1"}},
        {<<"db2">>, #{data_dir => BaseDir ++ "/db2"}},
        {<<"db3">>, #{data_dir => BaseDir ++ "/db3"}}
    ],

    Pids = lists:map(
        fun({Name, Config}) ->
            {ok, Pid} = barrel_db_sup:start_db(Name, Config),
            {Name, Pid}
        end,
        Dbs
    ),

    %% Verify all are running and have both stores
    lists:foreach(
        fun({_Name, Pid}) ->
            ?assert(is_process_alive(Pid)),
            {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
            {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
            ?assert(is_map(StoreRef)),
            ?assert(is_map(AttRef))
        end,
        Pids
    ),

    %% Stop all
    lists:foreach(
        fun({_Name, Pid}) ->
            ok = barrel_db_server:stop(Pid)
        end,
        Pids
    ),

    %% Give time to terminate
    timer:sleep(100),

    %% Verify all are stopped
    lists:foreach(
        fun({_Name, Pid}) ->
            ?assertNot(is_process_alive(Pid))
        end,
        Pids
    ),

    %% Cleanup
    os:cmd("rm -rf " ++ BaseDir),

    ok.
