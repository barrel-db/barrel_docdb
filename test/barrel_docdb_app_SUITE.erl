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
    Config = #{path => "/tmp/barrel_test"},
    {ok, Pid} = barrel_db_sup:start_db(DbName, Config),

    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Stop the database server
    ok = barrel_db_server:stop(Pid),

    %% Give it time to terminate
    timer:sleep(100),
    ?assertNot(is_process_alive(Pid)),

    ok.

%% @doc Test database server info
db_server_info(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    DbName = <<"info_test_db">>,
    Config = #{path => "/tmp/barrel_info_test"},
    {ok, Pid} = barrel_db_sup:start_db(DbName, Config),

    %% Get info
    {ok, Info} = barrel_db_server:info(Pid),

    ?assertEqual(DbName, maps:get(name, Info)),
    ?assertEqual(Config, maps:get(config, Info)),
    ?assertEqual(Pid, maps:get(pid, Info)),

    %% Cleanup
    ok = barrel_db_server:stop(Pid),

    ok.

%% @doc Test multiple database servers
multiple_db_servers(_Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),

    %% Start multiple databases
    Dbs = [
        {<<"db1">>, #{path => "/tmp/barrel_db1"}},
        {<<"db2">>, #{path => "/tmp/barrel_db2"}},
        {<<"db3">>, #{path => "/tmp/barrel_db3"}}
    ],

    Pids = lists:map(
        fun({Name, Config}) ->
            {ok, Pid} = barrel_db_sup:start_db(Name, Config),
            {Name, Pid}
        end,
        Dbs
    ),

    %% Verify all are running
    lists:foreach(
        fun({_Name, Pid}) ->
            ?assert(is_process_alive(Pid))
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

    ok.
