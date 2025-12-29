%%%-------------------------------------------------------------------
%%% @doc barrel_db_server - Individual database server process
%%%
%%% Manages a single database instance. Each database has its own
%%% gen_server process that handles all operations for that database.
%%% Opens both a document store (regular RocksDB) and an attachment
%%% store (RocksDB with BlobDB enabled).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_db_server).

-behaviour(gen_server).

%% API
-export([start_link/2]).
-export([info/1, stop/1]).
-export([get_store_ref/1, get_att_ref/1]).

%% View API
-export([
    register_view/3,
    unregister_view/2,
    list_views/1,
    get_view_pid/2
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-record(state, {
    name :: binary(),
    config :: map(),
    db_path :: string(),
    store_ref :: barrel_store_rocksdb:db_ref() | undefined,
    att_ref :: barrel_att_store:att_ref() | undefined,
    view_sup :: pid() | undefined,
    views :: #{binary() => pid()}  %% ViewId => ViewPid
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

%% @doc Get the document store reference
-spec get_store_ref(pid()) -> {ok, barrel_store_rocksdb:db_ref()} | {error, term()}.
get_store_ref(Pid) ->
    gen_server:call(Pid, get_store_ref).

%% @doc Get the attachment store reference
-spec get_att_ref(pid()) -> {ok, barrel_att_store:att_ref()} | {error, term()}.
get_att_ref(Pid) ->
    gen_server:call(Pid, get_att_ref).

%% @doc Stop the database server
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%%====================================================================
%% View API functions
%%====================================================================

%% @doc Register a new view
-spec register_view(pid(), binary(), map()) -> ok | {error, term()}.
register_view(Pid, ViewId, Config) ->
    gen_server:call(Pid, {register_view, ViewId, Config}).

%% @doc Unregister a view
-spec unregister_view(pid(), binary()) -> ok | {error, term()}.
unregister_view(Pid, ViewId) ->
    gen_server:call(Pid, {unregister_view, ViewId}).

%% @doc List all registered views
-spec list_views(pid()) -> {ok, [map()]}.
list_views(Pid) ->
    gen_server:call(Pid, list_views).

%% @doc Get the pid of a view process
-spec get_view_pid(pid(), binary()) -> {ok, pid()} | {error, not_found}.
get_view_pid(Pid, ViewId) ->
    gen_server:call(Pid, {get_view_pid, ViewId}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

%% @doc Initialize the database server
init([Name, Config]) ->
    process_flag(trap_exit, true),

    %% Get data directory from config
    DataDir = maps:get(data_dir, Config, "/tmp/barrel_data"),
    DbPath = filename:join([DataDir, binary_to_list(Name)]),

    %% Open document store (regular RocksDB)
    DocStorePath = filename:join(DbPath, "docs"),
    StoreOpts = maps:get(store_opts, Config, #{}),
    case barrel_store_rocksdb:open(DocStorePath, StoreOpts) of
        {ok, StoreRef} ->
            %% Open attachment store (RocksDB with BlobDB)
            AttStorePath = filename:join(DbPath, "attachments"),
            AttOpts = maps:get(att_opts, Config, #{}),
            case barrel_att_store:open(AttStorePath, AttOpts) of
                {ok, AttRef} ->
                    %% Start view supervisor
                    {ok, ViewSup} = barrel_view_sup:start_link(Name, StoreRef),

                    %% Load and start registered views
                    Views = start_registered_views(Name, StoreRef, ViewSup),

                    %% Register in persistent_term for lookup
                    persistent_term:put({barrel_db, Name}, self()),
                    logger:info("Database ~s started at ~s", [Name, DbPath]),
                    {ok, #state{
                        name = Name,
                        config = Config,
                        db_path = DbPath,
                        store_ref = StoreRef,
                        att_ref = AttRef,
                        view_sup = ViewSup,
                        views = Views
                    }};
                {error, AttReason} ->
                    %% Close document store if attachment store fails
                    barrel_store_rocksdb:close(StoreRef),
                    {stop, {att_store_open_failed, AttReason}}
            end;
        {error, Reason} ->
            {stop, {store_open_failed, Reason}}
    end.

%% @doc Handle synchronous calls
handle_call(info, _From, #state{name = Name, config = Config, db_path = DbPath} = State) ->
    Info = #{
        name => Name,
        config => Config,
        db_path => DbPath,
        pid => self()
    },
    {reply, {ok, Info}, State};

handle_call(get_store_ref, _From, #state{store_ref = StoreRef} = State) ->
    {reply, {ok, StoreRef}, State};

handle_call(get_att_ref, _From, #state{att_ref = AttRef} = State) ->
    {reply, {ok, AttRef}, State};

%% View operations
handle_call({register_view, ViewId, Config}, _From,
            #state{view_sup = ViewSup, views = Views} = State) ->
    case maps:is_key(ViewId, Views) of
        true ->
            {reply, {error, already_registered}, State};
        false ->
            ViewConfig = Config#{id => ViewId},
            case barrel_view_sup:start_view(ViewSup, ViewConfig) of
                {ok, Pid} ->
                    NewViews = Views#{ViewId => Pid},
                    {reply, ok, State#state{views = NewViews}};
                {error, _} = Error ->
                    {reply, Error, State}
            end
    end;

handle_call({unregister_view, ViewId}, _From,
            #state{name = Name, store_ref = StoreRef, view_sup = ViewSup, views = Views} = State) ->
    case maps:get(ViewId, Views, undefined) of
        undefined ->
            {reply, {error, not_found}, State};
        Pid ->
            %% Stop the view process
            ok = barrel_view_sup:stop_view(ViewSup, Pid),
            %% Delete view metadata and index
            ok = barrel_view_index:delete_view_meta(StoreRef, Name, ViewId),
            ok = barrel_view_index:clear_all(StoreRef, Name, ViewId),
            NewViews = maps:remove(ViewId, Views),
            {reply, ok, State#state{views = NewViews}}
    end;

handle_call(list_views, _From, #state{name = Name, store_ref = StoreRef} = State) ->
    Views = barrel_view_index:list_views(StoreRef, Name),
    {reply, {ok, Views}, State};

handle_call({get_view_pid, ViewId}, _From, #state{views = Views} = State) ->
    case maps:get(ViewId, Views, undefined) of
        undefined ->
            {reply, {error, not_found}, State};
        Pid ->
            {reply, {ok, Pid}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @doc Handle asynchronous casts
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Handle other messages
handle_info({'EXIT', Pid, Reason}, #state{views = Views} = State) ->
    %% Check if it's a view process that exited
    case find_view_by_pid(Pid, Views) of
        {ok, ViewId} ->
            logger:warning("View ~s exited: ~p", [ViewId, Reason]),
            NewViews = maps:remove(ViewId, Views),
            {noreply, State#state{views = NewViews}};
        not_found ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%% @doc Clean up when terminating
terminate(_Reason, #state{name = Name, store_ref = StoreRef, att_ref = AttRef, view_sup = ViewSup}) ->
    %% Stop view supervisor (will stop all views)
    case ViewSup of
        undefined -> ok;
        _ -> catch exit(ViewSup, shutdown)
    end,
    %% Close attachment store
    case AttRef of
        undefined -> ok;
        _ -> barrel_att_store:close(AttRef)
    end,
    %% Close document store
    case StoreRef of
        undefined -> ok;
        _ -> barrel_store_rocksdb:close(StoreRef)
    end,
    %% Unregister
    persistent_term:erase({barrel_db, Name}),
    logger:info("Database ~s stopped", [Name]),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Start all registered views on database startup
start_registered_views(DbName, StoreRef, ViewSup) ->
    ViewMetas = barrel_view_index:list_views(StoreRef, DbName),
    lists:foldl(
        fun(#{id := ViewId, module := Mod}, Acc) ->
            ViewConfig = #{id => ViewId, module => Mod},
            case barrel_view_sup:start_view(ViewSup, ViewConfig) of
                {ok, Pid} ->
                    Acc#{ViewId => Pid};
                {error, Reason} ->
                    logger:error("Failed to start view ~s: ~p", [ViewId, Reason]),
                    Acc
            end
        end,
        #{},
        ViewMetas
    ).

%% @doc Find a view ID by its process pid
find_view_by_pid(Pid, Views) ->
    case maps:fold(
        fun(ViewId, ViewPid, Acc) ->
            case ViewPid of
                Pid -> {found, ViewId};
                _ -> Acc
            end
        end,
        not_found,
        Views
    ) of
        {found, ViewId} -> {ok, ViewId};
        not_found -> not_found
    end.
