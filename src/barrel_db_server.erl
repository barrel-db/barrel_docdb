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

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-record(state, {
    name :: binary(),
    config :: map(),
    db_path :: string(),
    store_ref :: barrel_store_rocksdb:db_ref() | undefined,
    att_ref :: barrel_att_store:att_ref() | undefined
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
                    %% Register in persistent_term for lookup
                    persistent_term:put({barrel_db, Name}, self()),
                    logger:info("Database ~s started at ~s", [Name, DbPath]),
                    {ok, #state{
                        name = Name,
                        config = Config,
                        db_path = DbPath,
                        store_ref = StoreRef,
                        att_ref = AttRef
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

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @doc Handle asynchronous casts
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Handle other messages
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc Clean up when terminating
terminate(_Reason, #state{name = Name, store_ref = StoreRef, att_ref = AttRef}) ->
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
