%%%-------------------------------------------------------------------
%%% @doc View supervisor for barrel_docdb
%%%
%%% A simple_one_for_one supervisor that manages view processes.
%%% Each view is a barrel_view gen_statem process.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_view_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/2,
    start_view/2,
    stop_view/2,
    which_views/1
]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the view supervisor
-spec start_link(binary(), barrel_store_rocksdb:db_ref()) -> {ok, pid()} | {error, term()}.
start_link(DbName, StoreRef) ->
    supervisor:start_link(?MODULE, [DbName, StoreRef]).

%% @doc Start a view process
-spec start_view(pid(), map()) -> {ok, pid()} | {error, term()}.
start_view(Sup, ViewConfig) ->
    supervisor:start_child(Sup, [ViewConfig]).

%% @doc Stop a view process
-spec stop_view(pid(), pid()) -> ok | {error, term()}.
stop_view(Sup, ViewPid) ->
    supervisor:terminate_child(Sup, ViewPid).

%% @doc Get list of running view processes
-spec which_views(pid()) -> [{binary(), pid()}].
which_views(Sup) ->
    Children = supervisor:which_children(Sup),
    lists:filtermap(
        fun({_, Pid, _, _}) when is_pid(Pid) ->
            case barrel_view:get_view_id(Pid) of
                {ok, ViewId} -> {true, {ViewId, Pid}};
                _ -> false
            end;
           (_) ->
            false
        end,
        Children
    ).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([DbName, StoreRef]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 60
    },

    ChildSpec = #{
        id => barrel_view,
        start => {barrel_view, start_link, [DbName, StoreRef]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [barrel_view]
    },

    {ok, {SupFlags, [ChildSpec]}}.
