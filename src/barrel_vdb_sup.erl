%%%-------------------------------------------------------------------
%%% @doc VDB (Virtual Database) Supervisor
%%%
%%% Supervises VDB-related processes:
%%% - VDB registry (tracks active VDBs)
%%% - Future: shard monitors, rebalancers, health checkers
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the VDB supervisor
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @doc Initialize the supervisor
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },

    %% VDB Registry - tracks active VDBs and provides lookup
    Registry = #{
        id => barrel_vdb_registry,
        start => {barrel_vdb_registry, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_vdb_registry]
    },

    {ok, {SupFlags, [Registry]}}.
