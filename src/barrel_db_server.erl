%%%-------------------------------------------------------------------
%%% @doc barrel_db_server - Individual database server process
%%%
%%% Manages a single database instance. Each database has its own
%%% gen_server process that handles all operations for that database.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_db_server).

-behaviour(gen_server).

%% API
-export([start_link/2]).
-export([info/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-record(state, {
    name :: binary(),
    config :: map(),
    store_ref :: term() | undefined
}).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the database server
-spec start_link(binary(), map()) -> {ok, pid()} | {error, term()}.
start_link(Name, Config) ->
    gen_server:start_link(?MODULE, [Name, Config], []).

%% @doc Get database info
-spec info(pid()) -> {ok, map()} | {error, term()}.
info(Pid) ->
    gen_server:call(Pid, info).

%% @doc Stop the database server
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @doc Initialize the database server
init([Name, Config]) ->
    process_flag(trap_exit, true),
    %% Register in persistent_term for lookup
    persistent_term:put({barrel_db, Name}, self()),
    logger:info("Database ~s started", [Name]),
    {ok, #state{name = Name, config = Config}}.

%% @doc Handle synchronous calls
handle_call(info, _From, #state{name = Name, config = Config} = State) ->
    Info = #{
        name => Name,
        config => Config,
        pid => self()
    },
    {reply, {ok, Info}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @doc Handle asynchronous casts
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Handle other messages
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc Clean up when terminating
terminate(_Reason, #state{name = Name}) ->
    persistent_term:erase({barrel_db, Name}),
    logger:info("Database ~s stopped", [Name]),
    ok.
