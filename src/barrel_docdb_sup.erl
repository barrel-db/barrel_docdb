%%%-------------------------------------------------------------------
%%% @doc barrel_docdb top-level supervisor
%%%
%%% Supervises all barrel_docdb processes.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the barrel_docdb supervisor
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @doc Initialize the supervisor with child specs
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },

    %% Global HLC clock for distributed time synchronization
    %% Registered as 'barrel_hlc_clock' for node-wide access
    HlcMaxOffset = application:get_env(barrel_docdb, hlc_max_offset, 0),
    Hlc = #{
        id => barrel_hlc_clock,
        start => {hlc, start_link, [barrel_hlc_clock, fun hlc:physical_clock/0, HlcMaxOffset]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [hlc]
    },

    %% Database supervisor for managing individual database processes
    DbSup = #{
        id => barrel_db_sup,
        start => {barrel_db_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [barrel_db_sup]
    },

    %% HLC must start before databases
    ChildSpecs = [Hlc, DbSup],

    {ok, {SupFlags, ChildSpecs}}.
