%%%-------------------------------------------------------------------
%%% @doc VDB Registry
%%%
%%% Tracks active Virtual Databases and provides fast lookup.
%%% Maintains an in-memory cache of VDB metadata for performance.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb_registry).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    register_vdb/1,
    unregister_vdb/1,
    is_registered/1,
    list_registered/0,
    get_vdb_info/1,
    refresh/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(TABLE, barrel_vdb_registry_tab).

-record(state, {}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the VDB registry
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Register a VDB (called after create)
-spec register_vdb(binary()) -> ok.
register_vdb(VdbName) when is_binary(VdbName) ->
    gen_server:call(?SERVER, {register, VdbName}).

%% @doc Unregister a VDB (called after delete)
-spec unregister_vdb(binary()) -> ok.
unregister_vdb(VdbName) when is_binary(VdbName) ->
    gen_server:call(?SERVER, {unregister, VdbName}).

%% @doc Check if a VDB is registered (fast lookup)
-spec is_registered(binary()) -> boolean().
is_registered(VdbName) when is_binary(VdbName) ->
    case ets:lookup(?TABLE, VdbName) of
        [{_, _}] -> true;
        [] -> false
    end.

%% @doc List all registered VDBs
-spec list_registered() -> [binary()].
list_registered() ->
    [Name || {Name, _Info} <- ets:tab2list(?TABLE)].

%% @doc Get cached VDB info
-spec get_vdb_info(binary()) -> {ok, map()} | {error, not_found}.
get_vdb_info(VdbName) when is_binary(VdbName) ->
    case ets:lookup(?TABLE, VdbName) of
        [{_, Info}] -> {ok, Info};
        [] -> {error, not_found}
    end.

%% @doc Refresh registry from shard map (reload all VDBs)
-spec refresh() -> ok.
refresh() ->
    gen_server:call(?SERVER, refresh).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS table for fast lookups
    ?TABLE = ets:new(?TABLE, [
        named_table,
        public,
        set,
        {read_concurrency, true}
    ]),

    %% Load existing VDBs from shard map
    load_existing_vdbs(),

    {ok, #state{}}.

handle_call({register, VdbName}, _From, State) ->
    %% Fetch config and store in ETS
    case barrel_shard_map:get_config(VdbName) of
        {ok, Config} ->
            Info = #{
                config => Config,
                registered_at => erlang:system_time(millisecond)
            },
            ets:insert(?TABLE, {VdbName, Info});
        {error, _} ->
            %% Store minimal info even without config
            ets:insert(?TABLE, {VdbName, #{registered_at => erlang:system_time(millisecond)}})
    end,
    {reply, ok, State};

handle_call({unregister, VdbName}, _From, State) ->
    ets:delete(?TABLE, VdbName),
    {reply, ok, State};

handle_call(refresh, _From, State) ->
    ets:delete_all_objects(?TABLE),
    load_existing_vdbs(),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Load existing VDBs from shard map at startup
load_existing_vdbs() ->
    {ok, VdbNames} = barrel_shard_map:list(),
    lists:foreach(
      fun(VdbName) ->
          case barrel_shard_map:get_config(VdbName) of
            {ok, Config} ->
              Info = #{
                       config => Config,
                       registered_at => erlang:system_time(millisecond)
                      },
              ets:insert(?TABLE, {VdbName, Info});
            {error, _} ->
              ok
          end
      end,
      VdbNames
     ).
