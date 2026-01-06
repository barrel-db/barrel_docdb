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

%% Document API
-export([
    put_doc/3,
    put_docs/3,
    get_doc/3,
    get_docs/3,
    delete_doc/3,
    fold_docs/3,
    resolve_conflict/4,
    get_conflicts/2
]).

%% Replication API
-export([
    put_rev/4,
    revsdiff/3
]).

%% Local document API (for checkpoints, not replicated)
-export([
    put_local_doc/3,
    get_local_doc/2,
    delete_local_doc/2,
    fold_local_docs/4
]).

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
    views :: #{binary() => pid()},  %% ViewId => ViewPid
    filter_pid :: pid() | undefined,  %% Compaction filter handler for this database
    compaction_timer :: reference() | undefined,  %% Timer for periodic compaction checks
    compaction_interval :: pos_integer(),  %% Interval between checks in ms (default: 1 hour)
    compaction_size_threshold :: pos_integer()  %% Size threshold in bytes to trigger compaction
}).

%% Default compaction settings
-define(DEFAULT_COMPACTION_INTERVAL, 3600000).  %% 1 hour in ms
-define(DEFAULT_COMPACTION_SIZE_THRESHOLD, 1073741824).  %% 1 GB in bytes

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
%% Document API functions
%%====================================================================

%% @doc Put a document (create or update)
-spec put_doc(pid(), map(), map()) -> {ok, map()} | {error, term()}.
put_doc(Pid, Doc, Opts) ->
    gen_server:call(Pid, {put_doc, Doc, Opts}).

%% @doc Put multiple documents (batch write)
-spec put_docs(pid(), [map()], map()) -> [{ok, map()} | {error, term()}].
put_docs(Pid, Docs, Opts) ->
    gen_server:call(Pid, {put_docs, Docs, Opts}).

%% @doc Get a document
-spec get_doc(pid(), binary(), map()) -> {ok, map()} | {error, not_found} | {error, term()}.
get_doc(Pid, DocId, Opts) ->
    gen_server:call(Pid, {get_doc, DocId, Opts}).

%% @doc Get multiple documents (batch read)
-spec get_docs(pid(), [binary()], map()) -> [{ok, map()} | {error, not_found} | {error, term()}].
get_docs(Pid, DocIds, Opts) ->
    gen_server:call(Pid, {get_docs, DocIds, Opts}).

%% @doc Delete a document
-spec delete_doc(pid(), binary(), map()) -> {ok, map()} | {error, term()}.
delete_doc(Pid, DocId, Opts) ->
    gen_server:call(Pid, {delete_doc, DocId, Opts}).

%% @doc Fold over all documents
-spec fold_docs(pid(), fun(), term()) -> {ok, term()}.
fold_docs(Pid, Fun, Acc) ->
    gen_server:call(Pid, {fold_docs, Fun, Acc}, infinity).

%% @doc Get conflicts for a document
%% Returns list of conflicting revision IDs (excluding the winner)
-spec get_conflicts(pid(), binary()) -> {ok, [binary()]} | {error, term()}.
get_conflicts(Pid, DocId) ->
    gen_server:call(Pid, {get_conflicts, DocId}).

%% @doc Resolve a conflict by choosing a winning revision or providing a merged doc
%% Resolution can be:
%%   - {choose, Rev} - keep this revision as winner, delete other branches
%%   - {merge, Doc} - create new revision merging all conflicts
-spec resolve_conflict(pid(), binary(), binary(), {choose, binary()} | {merge, map()}) ->
    {ok, map()} | {error, term()}.
resolve_conflict(Pid, DocId, BaseRev, Resolution) ->
    gen_server:call(Pid, {resolve_conflict, DocId, BaseRev, Resolution}).

%%====================================================================
%% Replication API functions
%%====================================================================

%% @doc Put a document with explicit revision history (for replication)
-spec put_rev(pid(), map(), [binary()], boolean()) -> {ok, map()} | {error, term()}.
put_rev(Pid, Doc, History, Deleted) ->
    gen_server:call(Pid, {put_rev, Doc, History, Deleted}).

%% @doc Get revisions difference (for replication)
%% Returns {ok, Missing, PossibleAncestors}
-spec revsdiff(pid(), binary(), [binary()]) -> {ok, [binary()], [binary()]}.
revsdiff(Pid, DocId, RevIds) ->
    gen_server:call(Pid, {revsdiff, DocId, RevIds}).

%%====================================================================
%% Local Document API functions
%%====================================================================

%% @doc Put a local document (not replicated)
-spec put_local_doc(pid(), binary(), map()) -> ok | {error, term()}.
put_local_doc(Pid, DocId, Doc) ->
    gen_server:call(Pid, {put_local_doc, DocId, Doc}).

%% @doc Get a local document
-spec get_local_doc(pid(), binary()) -> {ok, map()} | {error, not_found}.
get_local_doc(Pid, DocId) ->
    gen_server:call(Pid, {get_local_doc, DocId}).

%% @doc Delete a local document
-spec delete_local_doc(pid(), binary()) -> ok | {error, not_found}.
delete_local_doc(Pid, DocId) ->
    gen_server:call(Pid, {delete_local_doc, DocId}).

%% @doc Fold over local documents with a given prefix
-spec fold_local_docs(pid(), binary(), fun((binary(), map(), term()) -> term()), term()) ->
    {ok, term()} | {error, term()}.
fold_local_docs(Pid, Prefix, Fun, Acc) ->
    gen_server:call(Pid, {fold_local_docs, Prefix, Fun, Acc}).

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

    %% Start compaction filter handler BEFORE opening RocksDB
    %% (handler pid is passed to CF options)
    PruneDepth = maps:get(prune_depth, Config, 1000),
    {ok, FilterPid} = barrel_compaction_filter:start_link(#{
        db_name => Name,
        prune_depth => PruneDepth
    }),

    %% Open document store (RocksDB with column families including body CF with BlobDB)
    DocStorePath = filename:join(DbPath, "docs"),
    StoreOpts0 = maps:get(store_opts, Config, #{}),
    %% Pass the filter handler to the store options
    StoreOpts = StoreOpts0#{compaction_filter_handler => FilterPid},
    case barrel_store_rocksdb:open(DocStorePath, StoreOpts) of
        {ok, StoreRef} ->
            %% Open attachment store (separate RocksDB with BlobDB)
            AttStorePath = filename:join(DbPath, "attachments"),
            AttOpts = maps:get(att_opts, Config, #{}),
            case barrel_att_store:open(AttStorePath, AttOpts) of
                {ok, AttRef} ->
                    %% Start view supervisor
                    {ok, ViewSup} = barrel_view_sup:start_link(Name, StoreRef),

                    %% Load and start registered views
                    Views = start_registered_views(Name, StoreRef, ViewSup),

                    %% Register in persistent_term for lookup
                    %% barrel_store is used by barrel_doc_body_store for body CF access
                    %% and by compaction filter handler for body deletion
                    persistent_term:put({barrel_db, Name}, self()),
                    persistent_term:put({barrel_store, Name}, StoreRef),

                    %% Compaction settings from config (or defaults)
                    CompactionInterval = maps:get(compaction_interval, Config,
                                                  ?DEFAULT_COMPACTION_INTERVAL),
                    CompactionThreshold = maps:get(compaction_size_threshold, Config,
                                                   ?DEFAULT_COMPACTION_SIZE_THRESHOLD),

                    %% Start periodic compaction check timer
                    TimerRef = erlang:send_after(CompactionInterval, self(), compaction_check),

                    logger:info("Database ~s started at ~s", [Name, DbPath]),
                    {ok, #state{
                        name = Name,
                        config = Config,
                        db_path = DbPath,
                        store_ref = StoreRef,
                        att_ref = AttRef,
                        view_sup = ViewSup,
                        views = Views,
                        filter_pid = FilterPid,
                        compaction_timer = TimerRef,
                        compaction_interval = CompactionInterval,
                        compaction_size_threshold = CompactionThreshold
                    }};
                {error, AttReason} ->
                    %% Close document store if attachment store fails
                    barrel_store_rocksdb:close(StoreRef),
                    exit(FilterPid, shutdown),
                    {stop, {att_store_open_failed, AttReason}}
            end;
        {error, Reason} ->
            exit(FilterPid, shutdown),
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

%% Document operations
handle_call({put_doc, Doc, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_put_doc(StoreRef, DbName, Doc, Opts),
    {reply, Result, State};

handle_call({put_docs, Docs, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_put_docs(StoreRef, DbName, Docs, Opts),
    {reply, Result, State};

handle_call({get_doc, DocId, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_doc(StoreRef, DbName, DocId, Opts),
    {reply, Result, State};

handle_call({get_docs, DocIds, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_docs(StoreRef, DbName, DocIds, Opts),
    {reply, Result, State};

handle_call({delete_doc, DocId, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_delete_doc(StoreRef, DbName, DocId, Opts),
    {reply, Result, State};

handle_call({fold_docs, Fun, Acc}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_fold_docs(StoreRef, DbName, Fun, Acc),
    {reply, Result, State};

%% Conflict operations
handle_call({get_conflicts, DocId}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_conflicts(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call({resolve_conflict, DocId, BaseRev, Resolution}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_resolve_conflict(StoreRef, DbName, DocId, BaseRev, Resolution),
    {reply, Result, State};

%% Replication operations
handle_call({put_rev, Doc, History, Deleted}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_put_rev(StoreRef, DbName, Doc, History, Deleted),
    {reply, Result, State};

handle_call({revsdiff, DocId, RevIds}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_revsdiff(StoreRef, DbName, DocId, RevIds),
    {reply, Result, State};

%% Local document operations
handle_call({put_local_doc, DocId, Doc}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_put_local_doc(StoreRef, DbName, DocId, Doc),
    {reply, Result, State};

handle_call({get_local_doc, DocId}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_local_doc(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call({delete_local_doc, DocId}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_delete_local_doc(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call({fold_local_docs, Prefix, Fun, Acc}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_fold_local_docs(StoreRef, DbName, Prefix, Fun, Acc),
    {reply, Result, State};

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

%%--------------------------------------------------------------------
%% Tier/TTL Operations
%%--------------------------------------------------------------------

handle_call({set_doc_ttl, DocId, ExpiresAt, _Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_set_doc_ttl(StoreRef, DbName, DocId, ExpiresAt),
    {reply, Result, State};

handle_call({get_doc_ttl, DocId, _Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_doc_ttl(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call({get_doc_tier, DocId, _Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_doc_tier(StoreRef, DbName, DocId),
    {reply, Result, State};

handle_call({set_doc_tier, DocId, Tier, _Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_set_doc_tier(StoreRef, DbName, DocId, Tier),
    {reply, Result, State};

handle_call({classify_by_age, HotCutoff, WarmCutoff, _Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_classify_by_age(StoreRef, DbName, HotCutoff, WarmCutoff),
    {reply, Result, State};

handle_call({migrate_expired, _Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_migrate_expired(StoreRef, DbName),
    {reply, Result, State};

handle_call({migrate_to_tier, TargetTier, Filter, _Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_migrate_to_tier(StoreRef, DbName, TargetTier, Filter),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @doc Handle asynchronous casts
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @doc Handle other messages
handle_info(compaction_check, #state{store_ref = StoreRef,
                                      compaction_interval = Interval,
                                      compaction_size_threshold = Threshold} = State) ->
    %% Check database size and trigger compaction if needed
    NewState = do_check_compaction(StoreRef, Threshold, State),
    %% Restart timer for next check
    TimerRef = erlang:send_after(Interval, self(), compaction_check),
    {noreply, NewState#state{compaction_timer = TimerRef}};

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
terminate(_Reason, #state{name = Name, store_ref = StoreRef, att_ref = AttRef,
                          view_sup = ViewSup, filter_pid = FilterPid,
                          compaction_timer = CompactionTimer}) ->
    %% Cancel compaction timer
    case CompactionTimer of
        undefined -> ok;
        _ -> erlang:cancel_timer(CompactionTimer)
    end,
    %% Stop view supervisor (will stop all views)
    case ViewSup of
        undefined -> ok;
        _ -> catch exit(ViewSup, shutdown)
    end,
    %% Stop compaction filter handler
    case FilterPid of
        undefined -> ok;
        _ -> catch exit(FilterPid, shutdown)
    end,
    %% Close attachment store
    case AttRef of
        undefined -> ok;
        _ -> barrel_att_store:close(AttRef)
    end,
    %% Close document store (includes body CF)
    case StoreRef of
        undefined -> ok;
        _ -> barrel_store_rocksdb:close(StoreRef)
    end,
    %% Unregister
    persistent_term:erase({barrel_db, Name}),
    persistent_term:erase({barrel_store, Name}),
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

%% @doc Check database size and trigger compaction if threshold exceeded
%% Called periodically by the compaction_check timer.
do_check_compaction(StoreRef, Threshold, State) ->
    #state{name = Name} = State,
    case barrel_store_rocksdb:get_db_size(StoreRef) of
        {ok, Size} when Size >= Threshold ->
            logger:info("Database ~s size ~p bytes exceeds threshold ~p, triggering compaction",
                        [Name, Size, Threshold]),
            case barrel_store_rocksdb:compact_default_cf(StoreRef) of
                ok ->
                    logger:info("Database ~s compaction completed", [Name]),
                    State;
                {error, Reason} ->
                    logger:warning("Database ~s compaction failed: ~p", [Name, Reason]),
                    State
            end;
        {ok, Size} ->
            logger:debug("Database ~s size ~p bytes below threshold ~p, skipping compaction",
                         [Name, Size, Threshold]),
            State;
        {error, Reason} ->
            logger:warning("Failed to get database ~s size: ~p", [Name, Reason]),
            State
    end.

%%====================================================================
%% Document Operations
%%====================================================================

%% Wide column names for document entity
-define(COL_REV, <<"rev">>).
-define(COL_DELETED, <<"del">>).
-define(COL_HLC, <<"hlc">>).
-define(COL_REVTREE, <<"revtree">>).
%% Tier/TTL columns (for tiered storage)
-define(COL_CREATED_AT, <<"created_at">>).
-define(COL_EXPIRES_AT, <<"expires_at">>).
-define(COL_TIER, <<"tier">>).

%% @doc Put a document (create or update)
%% Accepts: Erlang map, indexed CBOR binary, or plain CBOR binary
%% Options:
%%   - sync: boolean() - if true, sync to disk before returning (default: false)
do_put_doc(StoreRef, DbName, Doc, Opts) ->
    %% Normalize input: map, indexed binary, or plain CBOR -> map for processing
    DocMap = barrel_doc:to_map(Doc),
    %% Build document record from input
    DocRecord = barrel_doc:make_doc_record(DocMap),
    #{id := DocId, revs := Revs, deleted := Deleted, doc := DocBody} = DocRecord,
    [NewRev | _] = Revs,

    %% Check for existing document (wide column entity)
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    {OldHlc, OldRev, OldRevTree, OldDocBody} = case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            ExistingRev = proplists:get_value(?COL_REV, Columns),
            ExistingHlc = barrel_hlc:decode(proplists:get_value(?COL_HLC, Columns)),
            OldTree = decode_revtree(proplists:get_value(?COL_REVTREE, Columns)),
            %% Get old doc body for path index diff from body CF (current body, no rev in key)
            OldBody = case barrel_store_rocksdb:body_get(StoreRef,
                            barrel_store_keys:doc_body(DbName, DocId)) of
                {ok, OldCborBin} -> OldCborBin;
                not_found -> undefined
            end,
            {ExistingHlc, ExistingRev, OldTree, OldBody};
        not_found ->
            {undefined, undefined, #{}, undefined}
    end,

    %% Build revision tree (merge with existing)
    RevTree = case length(Revs) of
        1 ->
            %% New document
            OldRevTree#{NewRev => #{id => NewRev, parent => undefined, deleted => Deleted}};
        _ ->
            %% Update - build tree with history
            [CurRev, ParentRev | _] = Revs,
            OldRevTree#{
                ParentRev => #{id => ParentRev, parent => undefined, deleted => false},
                CurRev => #{id => CurRev, parent => ParentRev, deleted => Deleted}
            }
    end,

    %% Generate new HLC timestamp for this change
    NextHlc = barrel_hlc:new_hlc(),

    %% Prepare entity columns for wide-column storage
    %% Always track created_at and tier metadata (tiered storage strategy is optional)
    {CreatedAt, ExistingTier, ExistingExpires} = case OldHlc of
        undefined ->
            %% New document - set created_at to now, tier to hot (0)
            {barrel_hlc:encode(NextHlc), 0, 0};
        _ ->
            %% Update - preserve existing tier metadata
            case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
                {ok, OldColumns} ->
                    {proplists:get_value(?COL_CREATED_AT, OldColumns, barrel_hlc:encode(OldHlc)),
                     proplists:get_value(?COL_TIER, OldColumns, 0),
                     proplists:get_value(?COL_EXPIRES_AT, OldColumns, 0)};
                _ ->
                    {barrel_hlc:encode(OldHlc), 0, 0}
            end
    end,
    DocColumns = [
        {?COL_REV, NewRev},
        {?COL_DELETED, deleted_to_bin(Deleted)},
        {?COL_HLC, barrel_hlc:encode(NextHlc)},
        {?COL_REVTREE, encode_revtree(RevTree)},
        {?COL_CREATED_AT, CreatedAt},
        {?COL_EXPIRES_AT, ExistingExpires},
        {?COL_TIER, ExistingTier}
    ],
    DocOps = [{entity_put, DocEntityKey, DocColumns}],

    %% Build DocInfo for change tracking (compatible with existing change feed)
    DocInfo = #{
        id => DocId,
        rev => NewRev,
        deleted => Deleted,
        revtree => RevTree,
        hlc => NextHlc
    },

    %% Delete old HLC entry if exists
    HlcDeleteOps = case OldHlc of
        undefined -> [];
        _ -> [{delete, barrel_store_keys:doc_hlc(DbName, OldHlc)}]
    end,

    %% Path index operations (if not deleted)
    PathIndexOps = case Deleted of
        true when OldDocBody =/= undefined ->
            %% Deleting a document - remove all paths
            case barrel_ars_index:get_doc_paths(StoreRef, DbName, DocId) of
                {ok, OldPaths} -> barrel_ars_index:remove_doc_ops(DbName, DocId, OldPaths);
                not_found -> []
            end;
        true ->
            %% New doc being created as deleted (edge case) - no paths to index
            [];
        false when OldDocBody =:= undefined ->
            %% New document - index all paths
            barrel_ars_index:index_doc_ops(DbName, DocId, DocBody);
        false ->
            %% Update document - compute diff and update paths
            barrel_ars_index:update_doc_ops(DbName, DocId, OldDocBody, DocBody)
    end,

    %% Change entry operations
    ChangeInfo = DocInfo#{doc => DocBody},
    ChangeOps = barrel_changes:write_change_ops(DbName, NextHlc, ChangeInfo),

    %% Path-indexed change operations (for efficient filtered queries)
    PathHlcOps = case OldHlc of
        undefined ->
            %% New document - create path index entries
            barrel_changes:write_path_index_ops(DbName, NextHlc, ChangeInfo);
        _ ->
            %% Update - remove old path entries, add new ones
            barrel_changes:update_path_index_ops(DbName, NextHlc, ChangeInfo,
                                                  OldHlc, OldDocBody)
    end,

    %% Write batch atomically (metadata + indexes + body)
    CborBody = barrel_docdb_codec_cbor:encode_cbor(DocBody),

    %% Archive old body to revision-keyed location if updating
    ArchiveOps = case {OldRev, OldDocBody} of
        {undefined, _} -> [];  %% New document, no archive needed
        {_, undefined} -> [];  %% No old body to archive
        {_, OldCbor} ->
            %% Move old body to revision-keyed location
            OldBodyKey = barrel_store_keys:doc_body_rev(DbName, DocId, OldRev),
            [{body_put, OldBodyKey, OldCbor}]
    end,

    %% Write new body to current location (no revision in key)
    BodyKey = barrel_store_keys:doc_body(DbName, DocId),
    BodyOp = {body_put, BodyKey, CborBody},
    AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps ++ ArchiveOps ++ [BodyOp],
    Sync = maps:get(sync, Opts, false),
    WriteOpts = #{sync => Sync},
    ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps, WriteOpts),

    %% Notify path subscribers
    notify_subscribers(DbName, DocId, NewRev, NextHlc, Deleted, DocBody),

    %% Return result
    Result = #{
        <<"id">> => DocId,
        <<"ok">> => true,
        <<"rev">> => NewRev
    },
    {ok, Result}.

%% @doc Put multiple documents in a single batch (batch write)
%% Options:
%%   - sync: boolean() - if true, sync to disk before returning (default: false)
do_put_docs(StoreRef, DbName, Docs, Opts) ->
    Sync = maps:get(sync, Opts, false),
    %% Process each document to build operations and metadata
    {AllOps, Notifications} = lists:foldl(
        fun(Doc, {OpsAcc, NotifyAcc}) ->
            case prepare_doc_ops(StoreRef, DbName, Doc) of
                {ok, Ops, NotifyInfo} ->
                    {OpsAcc ++ Ops, [NotifyInfo | NotifyAcc]};
                {error, _Reason} = Err ->
                    %% Skip failed docs, include error in notifications
                    {OpsAcc, [{error, Err} | NotifyAcc]}
            end
        end,
        {[], []},
        Docs
    ),

    %% Write all operations in a single batch (includes body CF writes)
    case AllOps of
        [] -> ok;
        _ ->
            WriteOpts = #{sync => Sync},
            ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps, WriteOpts)
    end,

    %% Notify subscribers and build results (reverse to maintain order)
    Results = lists:map(
        fun({error, Err}) ->
            Err;
           ({DocId, NewRev, NextHlc, Deleted, DocBody}) ->
            notify_subscribers(DbName, DocId, NewRev, NextHlc, Deleted, DocBody),
            {ok, #{<<"id">> => DocId, <<"ok">> => true, <<"rev">> => NewRev}}
        end,
        lists:reverse(Notifications)
    ),
    Results.

%% @doc Prepare document operations without writing (using wide column entity)
%% Accepts: Erlang map, indexed CBOR binary, or plain CBOR binary
%% Returns {ok, Ops, NotifyInfo} or {error, Reason}
prepare_doc_ops(StoreRef, DbName, Doc) ->
    try
        %% Normalize input: map, indexed binary, or plain CBOR -> map for processing
        DocMap = barrel_doc:to_map(Doc),
        %% Build document record from input
        DocRecord = barrel_doc:make_doc_record(DocMap),
        #{id := DocId, revs := Revs, deleted := Deleted, doc := DocBody} = DocRecord,
        [NewRev | _] = Revs,

        %% Check for existing document (wide column entity)
        DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
        {OldHlc, OldRev, OldRevTree, OldDocBody, OldCborBody, OldTierMeta} = case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
            {ok, Columns} ->
                ExistingRev = proplists:get_value(?COL_REV, Columns),
                ExistingHlc = barrel_hlc:decode(proplists:get_value(?COL_HLC, Columns)),
                OldTree = decode_revtree(proplists:get_value(?COL_REVTREE, Columns)),
                %% Extract tier metadata
                TierMeta = {
                    proplists:get_value(?COL_CREATED_AT, Columns, <<>>),
                    proplists:get_value(?COL_EXPIRES_AT, Columns, 0),
                    proplists:get_value(?COL_TIER, Columns, 0)
                },
                %% Get old doc body from body CF (current body, no rev in key)
                {OldBody, OldCbor} = case barrel_store_rocksdb:body_get(StoreRef,
                                barrel_store_keys:doc_body(DbName, DocId)) of
                    {ok, OldCborBin} -> {barrel_docdb_codec_cbor:decode_any(OldCborBin), OldCborBin};
                    not_found -> {#{}, undefined}
                end,
                {ExistingHlc, ExistingRev, OldTree, OldBody, OldCbor, TierMeta};
            not_found ->
                {undefined, undefined, #{}, undefined, undefined, {<<>>, 0, 0}}
        end,

        %% Build revision tree (merge with existing)
        RevTree = case length(Revs) of
            1 ->
                OldRevTree#{NewRev => #{id => NewRev, parent => undefined, deleted => Deleted}};
            _ ->
                [CurRev, ParentRev | _] = Revs,
                OldRevTree#{
                    ParentRev => #{id => ParentRev, parent => undefined, deleted => false},
                    CurRev => #{id => CurRev, parent => ParentRev, deleted => Deleted}
                }
        end,

        %% Generate new HLC timestamp
        NextHlc = barrel_hlc:new_hlc(),

        %% Prepare entity columns for wide-column storage (with tier metadata)
        {CreatedAt, ExpiresAt, Tier} = case OldHlc of
            undefined -> {barrel_hlc:encode(NextHlc), 0, 0};  %% New doc
            _ -> OldTierMeta  %% Preserve existing tier metadata
        end,
        DocColumns = [
            {?COL_REV, NewRev},
            {?COL_DELETED, deleted_to_bin(Deleted)},
            {?COL_HLC, barrel_hlc:encode(NextHlc)},
            {?COL_REVTREE, encode_revtree(RevTree)},
            {?COL_CREATED_AT, CreatedAt},
            {?COL_EXPIRES_AT, ExpiresAt},
            {?COL_TIER, Tier}
        ],
        DocOps = [{entity_put, DocEntityKey, DocColumns}],

        %% Build DocInfo for change tracking
        DocInfo = #{
            id => DocId,
            rev => NewRev,
            deleted => Deleted,
            revtree => RevTree,
            hlc => NextHlc
        },

        HlcDeleteOps = case OldHlc of
            undefined -> [];
            _ -> [{delete, barrel_store_keys:doc_hlc(DbName, OldHlc)}]
        end,

        PathIndexOps = case Deleted of
            true when OldDocBody =/= undefined ->
                case barrel_ars_index:get_doc_paths(StoreRef, DbName, DocId) of
                    {ok, OldPaths} -> barrel_ars_index:remove_doc_ops(DbName, DocId, OldPaths);
                    not_found -> []
                end;
            true -> [];
            false when OldDocBody =:= undefined ->
                barrel_ars_index:index_doc_ops(DbName, DocId, DocBody);
            false ->
                barrel_ars_index:update_doc_ops(DbName, DocId, OldDocBody, DocBody)
        end,

        ChangeInfo = DocInfo#{doc => DocBody},
        ChangeOps = barrel_changes:write_change_ops(DbName, NextHlc, ChangeInfo),

        PathHlcOps = case OldHlc of
            undefined ->
                barrel_changes:write_path_index_ops(DbName, NextHlc, ChangeInfo);
            _ ->
                barrel_changes:update_path_index_ops(DbName, NextHlc, ChangeInfo,
                                                      OldHlc, OldDocBody)
        end,

        CborBody = barrel_docdb_codec_cbor:encode_cbor(DocBody),

        %% Archive old body to revision-keyed location if updating
        ArchiveOps = case {OldRev, OldCborBody} of
            {undefined, _} -> [];
            {_, undefined} -> [];
            _ ->
                OldBodyKey = barrel_store_keys:doc_body_rev(DbName, DocId, OldRev),
                [{body_put, OldBodyKey, OldCborBody}]
        end,

        %% Write new body to current location (no revision in key)
        BodyKey = barrel_store_keys:doc_body(DbName, DocId),
        BodyOp = {body_put, BodyKey, CborBody},
        AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps ++ ArchiveOps ++ [BodyOp],
        NotifyInfo = {DocId, NewRev, NextHlc, Deleted, DocBody},
        {ok, AllOps, NotifyInfo}
    catch
        _:Reason ->
            {error, Reason}
    end.

%% @doc Get a document by ID (using wide column entity)
do_get_doc(StoreRef, DbName, DocId, Opts) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            %% Get the deterministic winner from revtree (not just stored rev)
            RevTreeBin = proplists:get_value(?COL_REVTREE, Columns),
            Rev = case RevTreeBin of
                undefined ->
                    proplists:get_value(?COL_REV, Columns);
                <<>> ->
                    proplists:get_value(?COL_REV, Columns);
                _ ->
                    #{winner := Winner} = barrel_revtree_bin:decode_winner_leaves(RevTreeBin),
                    case Winner of
                        undefined -> proplists:get_value(?COL_REV, Columns);
                        _ -> Winner
                    end
            end,
            Deleted = bin_to_deleted(proplists:get_value(?COL_DELETED, Columns, <<"false">>)),
            IncludeDeleted = maps:get(include_deleted, Opts, false),

            case {Deleted, IncludeDeleted} of
                {true, false} ->
                    {error, not_found};
                _ ->
                    %% Get document body from body CF (current body, no rev in key)
                    BodyKey = barrel_store_keys:doc_body(DbName, DocId),
                    case barrel_store_rocksdb:body_get(StoreRef, BodyKey) of
                        {ok, CborBin} ->
                            %% Decode CBOR to get document body
                            DocBody = barrel_docdb_codec_cbor:decode_any(CborBin),
                            %% Add metadata
                            Result = DocBody#{
                                <<"id">> => DocId,
                                <<"_rev">> => Rev
                            },
                            Result2 = case Deleted of
                                true -> Result#{<<"_deleted">> => true};
                                false -> Result
                            end,
                            %% Add conflicts if requested
                            Result3 = maybe_add_conflicts(Result2, Columns, Opts),
                            {ok, Result3};
                        not_found ->
                            {error, not_found}
                    end
            end;
        not_found ->
            {error, not_found}
    end.

%% @doc Get multiple documents by ID (batch read - parallel entity + body fetch)
do_get_docs(StoreRef, DbName, DocIds, Opts) ->
    IncludeDeleted = maps:get(include_deleted, Opts, false),

    %% Build keys for both entity and body (no rev needed for body)
    DocEntityKeys = [barrel_store_keys:doc_entity(DbName, DocId) || DocId <- DocIds],
    BodyKeys = [barrel_store_keys:doc_body(DbName, DocId) || DocId <- DocIds],

    %% Batch fetch entities and bodies in parallel (same batch)
    DocEntityResults = barrel_store_rocksdb:multi_get_entity(StoreRef, DocEntityKeys),
    DocBodyResults = barrel_store_rocksdb:body_multi_get(StoreRef, BodyKeys),

    %% Combine results
    DocBodyMap = lists:foldl(
        fun({DocId, EntityResult, BodyResult}, Map) ->
            case {EntityResult, BodyResult} of
                {{ok, Columns}, {ok, CborBin}} ->
                    Rev = proplists:get_value(?COL_REV, Columns),
                    Deleted = bin_to_deleted(proplists:get_value(?COL_DELETED, Columns, <<"false">>)),
                    case {Deleted, IncludeDeleted} of
                        {true, false} ->
                            Map;  %% Skip deleted
                        _ ->
                            DocBody = barrel_docdb_codec_cbor:decode_any(CborBin),
                            Result = DocBody#{<<"id">> => DocId, <<"_rev">> => Rev},
                            Result2 = case Deleted of
                                true -> Result#{<<"_deleted">> => true};
                                false -> Result
                            end,
                            Map#{DocId => {ok, Result2}}
                    end;
                _ -> Map
            end
        end,
        #{},
        lists:zip3(DocIds, DocEntityResults, DocBodyResults)
    ),
    %% Return results in original order
    [maps:get(DocId, DocBodyMap, {error, not_found}) || DocId <- DocIds].

%% @doc Delete a document (using wide column entity)
%% Options:
%%   - rev: binary() - expected revision (optional, for conflict detection)
%%   - sync: boolean() - if true, sync to disk before returning (default: false)
do_delete_doc(StoreRef, DbName, DocId, Opts) ->
    %% Get current state (wide column entity)
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            CurrentRev = proplists:get_value(?COL_REV, Columns),
            OldHlc = barrel_hlc:decode(proplists:get_value(?COL_HLC, Columns)),
            RevTree = decode_revtree(proplists:get_value(?COL_REVTREE, Columns)),

            %% Verify revision if provided
            ExpectedRev = maps:get(rev, Opts, undefined),
            case ExpectedRev of
                undefined -> ok;
                CurrentRev -> ok;
                _ -> throw({error, {conflict, CurrentRev}})
            end,

            %% Create delete revision
            {Gen, _Hash} = barrel_doc:parse_revision(CurrentRev),
            DeleteHash = barrel_doc:revision_hash(#{}, CurrentRev, true),
            NewRev = barrel_doc:make_revision(Gen + 1, DeleteHash),

            %% Generate new HLC
            NextHlc = barrel_hlc:new_hlc(),

            %% Update revision tree
            NewRevTree = RevTree#{NewRev => #{id => NewRev, parent => CurrentRev, deleted => true}},

            %% Prepare entity columns for wide-column storage
            DocColumns = [
                {?COL_REV, NewRev},
                {?COL_DELETED, <<"true">>},
                {?COL_HLC, barrel_hlc:encode(NextHlc)},
                {?COL_REVTREE, encode_revtree(NewRevTree)}
            ],
            DocOps = [{entity_put, DocEntityKey, DocColumns}],

            HlcDeleteOps = case OldHlc of
                undefined -> [];
                _ -> [{delete, barrel_store_keys:doc_hlc(DbName, OldHlc)}]
            end,

            %% Path index removal operations
            PathIndexOps = case barrel_ars_index:get_doc_paths(StoreRef, DbName, DocId) of
                {ok, Paths} -> barrel_ars_index:remove_doc_ops(DbName, DocId, Paths);
                not_found -> []
            end,

            %% Build DocInfo for change tracking
            NewDocInfo = #{
                id => DocId,
                rev => NewRev,
                deleted => true,
                revtree => NewRevTree,
                hlc => NextHlc
            },

            %% Change entry operations
            ChangeOps = barrel_changes:write_change_ops(DbName, NextHlc, NewDocInfo),

            %% Path-indexed change operations for delete
            %% Get old doc body from body CF (current body, no rev in key)
            OldDocBody = case barrel_store_rocksdb:body_get(StoreRef,
                                barrel_store_keys:doc_body(DbName, DocId)) of
                {ok, OldCborBin} -> barrel_docdb_codec_cbor:decode_any(OldCborBin);
                not_found -> undefined
            end,
            PathHlcOps = barrel_changes:update_path_index_ops(DbName, NextHlc, NewDocInfo,
                                                               OldHlc, OldDocBody),

            %% Write batch atomically
            AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps,
            WriteOpts = #{sync => maps:get(sync, Opts, false)},
            ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps, WriteOpts),

            %% Notify path subscribers
            notify_subscribers(DbName, DocId, NewRev, NextHlc, true, #{}),

            {ok, #{<<"id">> => DocId, <<"ok">> => true, <<"rev">> => NewRev}};

        not_found ->
            {error, not_found}
    end.

%% @doc Fold over all documents (using wide column entity)
%% Note: Uses regular key iteration since wide columns are stored per-key
do_fold_docs(StoreRef, DbName, Fun, Acc) ->
    StartKey = barrel_store_keys:doc_entity_prefix(DbName),
    EndKey = barrel_store_keys:doc_entity_end(DbName),
    PrefixLen = byte_size(StartKey),

    FoldFun = fun(Key, _Value, AccIn) ->
        %% Extract DocId from key (after prefix)
        DocId = binary:part(Key, PrefixLen, byte_size(Key) - PrefixLen),
        %% Get entity to access columns
        case barrel_store_rocksdb:get_entity(StoreRef, Key) of
            {ok, Columns} ->
                Rev = proplists:get_value(?COL_REV, Columns),
                Deleted = bin_to_deleted(proplists:get_value(?COL_DELETED, Columns, <<"false">>)),
                case Deleted of
                    true ->
                        %% Skip deleted documents
                        {ok, AccIn};
                    false ->
                        %% Get document body from body CF (current body, no rev in key)
                        BodyKey = barrel_store_keys:doc_body(DbName, DocId),
                        DocBody = case barrel_store_rocksdb:body_get(StoreRef, BodyKey) of
                            {ok, CborBin} -> barrel_docdb_codec_cbor:decode_any(CborBin);
                            not_found -> #{}
                        end,
                        Doc = DocBody#{
                            <<"id">> => DocId,
                            <<"_rev">> => Rev
                        },
                        case Fun(Doc, AccIn) of
                            {ok, AccOut} -> {ok, AccOut};
                            {stop, AccOut} -> {stop, AccOut};
                            stop -> {stop, AccIn}
                        end
                end;
            not_found ->
                {ok, AccIn}
        end
    end,

    FinalAcc = barrel_store_rocksdb:fold_range(StoreRef, StartKey, EndKey, FoldFun, Acc),
    {ok, FinalAcc}.

%%====================================================================
%% Replication Operations
%%====================================================================

%% @doc Put a document with explicit revision history (for replication, using wide column entity)
do_put_rev(StoreRef, DbName, Doc, History, Deleted) ->
    DocId = maps:get(<<"id">>, Doc),
    DocBody = barrel_doc:doc_without_meta(Doc),
    [NewRev | _] = History,

    %% Check for existing document (wide column entity)
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    {ExistingRevTree, OldRev, OldHlc, OldDocBody, OldCborBody} = case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            ExistingRev = proplists:get_value(?COL_REV, Columns),
            ExistingHlc = barrel_hlc:decode(proplists:get_value(?COL_HLC, Columns)),
            OldTree = decode_revtree(proplists:get_value(?COL_REVTREE, Columns)),
            %% Get old doc body from body CF (current body, no rev in key)
            {OldBody, OldCbor} = case barrel_store_rocksdb:body_get(StoreRef,
                            barrel_store_keys:doc_body(DbName, DocId)) of
                {ok, OldCborBin} -> {barrel_docdb_codec_cbor:decode_any(OldCborBin), OldCborBin};
                not_found -> {#{}, undefined}
            end,
            {OldTree, ExistingRev, ExistingHlc, OldBody, OldCbor};
        not_found ->
            {#{}, undefined, undefined, undefined, undefined}
    end,

    %% Build revision tree from history
    NewRevTree = build_revtree_from_history(History, Deleted, ExistingRevTree),

    %% Generate new HLC timestamp for this change
    NextHlc = barrel_hlc:new_hlc(),

    %% Prepare entity columns for wide-column storage
    DocColumns = [
        {?COL_REV, NewRev},
        {?COL_DELETED, deleted_to_bin(Deleted)},
        {?COL_HLC, barrel_hlc:encode(NextHlc)},
        {?COL_REVTREE, encode_revtree(NewRevTree)}
    ],
    DocOps = [{entity_put, DocEntityKey, DocColumns}],

    %% Build DocInfo for change tracking
    DocInfo = #{
        id => DocId,
        rev => NewRev,
        deleted => Deleted,
        revtree => NewRevTree,
        hlc => NextHlc
    },

    %% Delete old HLC entry if exists
    HlcDeleteOps = case OldHlc of
        undefined -> [];
        _ -> [{delete, barrel_store_keys:doc_hlc(DbName, OldHlc)}]
    end,

    %% Path index operations
    PathIndexOps = case Deleted of
        true when OldDocBody =/= undefined ->
            %% Deleting a document - remove all paths
            case barrel_ars_index:get_doc_paths(StoreRef, DbName, DocId) of
                {ok, OldPaths} -> barrel_ars_index:remove_doc_ops(DbName, DocId, OldPaths);
                not_found -> []
            end;
        true ->
            %% New doc being created as deleted - no paths to index
            [];
        false when OldDocBody =:= undefined ->
            %% New document - index all paths
            barrel_ars_index:index_doc_ops(DbName, DocId, DocBody);
        false ->
            %% Update document - compute diff and update paths
            barrel_ars_index:update_doc_ops(DbName, DocId, OldDocBody, DocBody)
    end,

    %% Change entry operations
    ChangeInfo = DocInfo#{doc => DocBody},
    ChangeOps = barrel_changes:write_change_ops(DbName, NextHlc, ChangeInfo),

    %% Path-indexed change operations (for efficient filtered queries)
    PathHlcOps = case OldHlc of
        undefined ->
            %% New document - create path index entries
            barrel_changes:write_path_index_ops(DbName, NextHlc, ChangeInfo);
        _ ->
            %% Update - remove old path entries, add new ones
            barrel_changes:update_path_index_ops(DbName, NextHlc, ChangeInfo,
                                                  OldHlc, OldDocBody)
    end,

    %% Write batch atomically (metadata + indexes + body)
    CborBody = barrel_docdb_codec_cbor:encode_cbor(DocBody),

    %% Archive old body to revision-keyed location if updating
    ArchiveOps = case {OldRev, OldCborBody} of
        {undefined, _} -> [];
        {_, undefined} -> [];
        _ ->
            OldBodyKey = barrel_store_keys:doc_body_rev(DbName, DocId, OldRev),
            [{body_put, OldBodyKey, OldCborBody}]
    end,

    %% Write new body to current location (no revision in key)
    BodyKey = barrel_store_keys:doc_body(DbName, DocId),
    BodyOp = {body_put, BodyKey, CborBody},
    AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps ++ ArchiveOps ++ [BodyOp],
    ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps),

    %% Notify path subscribers
    notify_subscribers(DbName, DocId, NewRev, NextHlc, Deleted, DocBody),

    {ok, DocId, NewRev}.

%% @doc Build revision tree from history
build_revtree_from_history(History, Deleted, ExistingTree) ->
    build_revtree_from_history(lists:reverse(History), Deleted, ExistingTree, undefined).

build_revtree_from_history([], _Deleted, Tree, _Parent) ->
    Tree;
build_revtree_from_history([Rev], Deleted, Tree, Parent) ->
    %% Last revision (the newest one)
    Tree#{Rev => #{id => Rev, parent => Parent, deleted => Deleted}};
build_revtree_from_history([Rev | Rest], Deleted, Tree, Parent) ->
    %% Intermediate revisions (not deleted)
    NewTree = Tree#{Rev => #{id => Rev, parent => Parent, deleted => false}},
    build_revtree_from_history(Rest, Deleted, NewTree, Rev).

%% @doc Get revisions difference (using wide column entity)
do_revsdiff(StoreRef, DbName, DocId, RevIds) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            RevTree = decode_revtree(proplists:get_value(?COL_REVTREE, Columns)),

            %% Find missing revisions and possible ancestors
            {Missing, PossibleAncestors} = lists:foldl(
                fun(RevId, {M, A} = Acc) ->
                    case maps:is_key(RevId, RevTree) of
                        true ->
                            %% Revision exists, not missing
                            Acc;
                        false ->
                            %% Revision is missing
                            M2 = [RevId | M],
                            %% Find possible ancestors in our tree
                            {Gen, _} = barrel_doc:parse_revision(RevId),
                            A2 = maps:fold(
                                fun(LocalRev, _RevInfo, AccA) ->
                                    {LocalGen, _} = barrel_doc:parse_revision(LocalRev),
                                    case LocalGen < Gen of
                                        true -> [LocalRev | AccA];
                                        false -> AccA
                                    end
                                end,
                                A,
                                RevTree
                            ),
                            {M2, A2}
                    end
                end,
                {[], []},
                RevIds
            ),
            {ok, lists:reverse(Missing), lists:usort(PossibleAncestors)};

        not_found ->
            %% Document doesn't exist - all revisions are missing
            {ok, RevIds, []}
    end.

%%====================================================================
%% Local Document Operations
%%====================================================================
%% Local documents are stored in the dedicated local_cf column family.
%% They use a simple key format: DbName + 0 + DocId (no prefix needed).

%% @doc Put a local document (per-database)
do_put_local_doc(StoreRef, DbName, DocId, Doc) ->
    Key = barrel_store_keys:local_doc_key(DbName, DocId),
    Value = term_to_binary(Doc),
    ok = barrel_store_rocksdb:local_put(StoreRef, Key, Value),
    ok.

%% @doc Get a local document (per-database)
do_get_local_doc(StoreRef, DbName, DocId) ->
    Key = barrel_store_keys:local_doc_key(DbName, DocId),
    case barrel_store_rocksdb:local_get(StoreRef, Key) of
        {ok, Value} ->
            {ok, binary_to_term(Value)};
        not_found ->
            {error, not_found}
    end.

%% @doc Delete a local document (per-database)
do_delete_local_doc(StoreRef, DbName, DocId) ->
    Key = barrel_store_keys:local_doc_key(DbName, DocId),
    case barrel_store_rocksdb:local_get(StoreRef, Key) of
        {ok, _} ->
            ok = barrel_store_rocksdb:local_delete(StoreRef, Key),
            ok;
        not_found ->
            {error, not_found}
    end.

%% @doc Fold over local documents matching a prefix
%% The prefix is applied to the DocId portion (after DbName).
do_fold_local_docs(StoreRef, DbName, DocIdPrefix, Fun, Acc0) ->
    %% Build full key prefix: DbName + 0 + DocIdPrefix
    KeyPrefix = barrel_store_keys:local_doc_key(DbName, DocIdPrefix),
    %% Fold over matching keys, decode values, extract DocId
    DbNameLen = byte_size(DbName),
    Result = barrel_store_rocksdb:local_fold(
        StoreRef,
        KeyPrefix,
        fun(Key, Value, Acc) ->
            %% Extract DocId from key (skip DbName + 0)
            <<_:DbNameLen/binary, 0, DocId/binary>> = Key,
            Doc = binary_to_term(Value),
            Fun(DocId, Doc, Acc)
        end,
        Acc0
    ),
    {ok, Result}.

%%====================================================================
%% Subscription Notifications
%%====================================================================

%% @doc Notify subscribers of document changes
%% Extracts paths from document, matches against subscriptions,
%% and sends notifications to matching subscribers.
notify_subscribers(DbName, DocId, Rev, Hlc, Deleted, DocBody) ->
    %% Extract paths from document body
    Topics = case Deleted of
        true ->
            %% For deleted docs, just use the doc ID as a path
            [DocId];
        false ->
            Paths = barrel_ars:analyze(DocBody),
            barrel_ars:paths_to_topics(Paths)
    end,

    %% Find matching path subscribers
    Pids = barrel_sub:match(DbName, Topics),

    %% Build notification
    Notification = {barrel_change, DbName, #{
        id => DocId,
        rev => Rev,
        hlc => Hlc,
        deleted => Deleted,
        paths => Topics
    }},

    %% Send to each path subscriber
    [Pid ! Notification || Pid <- Pids],

    %% Notify query subscribers (only for non-deleted docs)
    case Deleted of
        true ->
            ok;
        false ->
            barrel_query_sub:notify_change(DbName, DocId, Rev, DocBody)
    end,
    ok.

%%====================================================================
%% Wide Column Helpers
%%====================================================================

%% @doc Convert boolean to binary for wide column storage
-spec deleted_to_bin(boolean()) -> binary().
deleted_to_bin(true) -> <<"true">>;
deleted_to_bin(false) -> <<"false">>.

%% @doc Convert binary back to boolean from wide column storage
-spec bin_to_deleted(binary()) -> boolean().
bin_to_deleted(<<"true">>) -> true;
bin_to_deleted(<<"false">>) -> false;
bin_to_deleted(_) -> false.

%% @doc Encode revision tree to compact binary format
-spec encode_revtree(map()) -> binary().
encode_revtree(RevTree) when is_map(RevTree) ->
    RT = barrel_revtree_bin:from_map(RevTree),
    barrel_revtree_bin:encode(RT).

%% @doc Decode revision tree from compact binary format
-spec decode_revtree(binary() | undefined) -> map().
decode_revtree(undefined) -> #{};
decode_revtree(<<>>) -> #{};
decode_revtree(Bin) ->
    RT = barrel_revtree_bin:decode(Bin),
    barrel_revtree_bin:to_map(RT).

%% @doc Add conflicts to document if requested and conflicts exist
maybe_add_conflicts(Doc, Columns, Opts) ->
    case maps:get(conflicts, Opts, false) of
        true ->
            RevTreeBin = proplists:get_value(?COL_REVTREE, Columns),
            case RevTreeBin of
                undefined -> Doc;
                <<>> -> Doc;
                _ ->
                    %% Use fast path to get conflicts directly from binary
                    #{conflicts := Conflicts} = barrel_revtree_bin:decode_winner_leaves(RevTreeBin),
                    case Conflicts of
                        [] -> Doc;
                        _ -> Doc#{<<"_conflicts">> => Conflicts}
                    end
            end;
        false ->
            Doc
    end.

%%====================================================================
%% Conflict Operations
%%====================================================================

%% @doc Get list of conflicting revisions for a document
do_get_conflicts(StoreRef, DbName, DocId) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            RevTreeBin = proplists:get_value(?COL_REVTREE, Columns),
            case RevTreeBin of
                undefined -> {ok, []};
                <<>> -> {ok, []};
                _ ->
                    #{conflicts := Conflicts} = barrel_revtree_bin:decode_winner_leaves(RevTreeBin),
                    {ok, Conflicts}
            end;
        not_found ->
            {error, not_found}
    end.

%% @doc Resolve a conflict
%% Resolution types:
%%   {choose, Rev} - Mark the chosen rev as winner, delete all other leaf branches
%%   {merge, Doc} - Create a new revision that supersedes all conflicting branches
do_resolve_conflict(StoreRef, DbName, DocId, BaseRev, Resolution) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            CurrentRev = proplists:get_value(?COL_REV, Columns),
            RevTreeBin = proplists:get_value(?COL_REVTREE, Columns),
            case RevTreeBin of
                undefined ->
                    {error, no_conflicts};
                <<>> ->
                    {error, no_conflicts};
                _ ->
                    RevTree = decode_revtree(RevTreeBin),
                    #{winner := Winner, conflicts := Conflicts} =
                        barrel_revtree_bin:decode_winner_leaves(RevTreeBin),
                    case Conflicts of
                        [] ->
                            {error, no_conflicts};
                        _ ->
                            %% Verify base rev matches current winner
                            case BaseRev =:= Winner of
                                false ->
                                    {error, {conflict, CurrentRev}};
                                true ->
                                    do_apply_resolution(StoreRef, DbName, DocId,
                                                       DocEntityKey, Columns,
                                                       RevTree, Winner, Conflicts,
                                                       Resolution)
                            end
                    end
            end;
        not_found ->
            {error, not_found}
    end.

%% Apply the resolution to the document
do_apply_resolution(StoreRef, DbName, DocId, DocEntityKey, Columns,
                    RevTree, Winner, Conflicts, {choose, ChosenRev}) ->
    %% Verify chosen rev is either the winner or one of the conflicts
    AllLeaves = [Winner | Conflicts],
    case lists:member(ChosenRev, AllLeaves) of
        false ->
            {error, {invalid_rev, ChosenRev}};
        true ->
            %% Delete all other branches by marking them as deleted
            RevsToDelete = [R || R <- AllLeaves, R =/= ChosenRev],
            NewRevTree0 = lists:foldl(
                fun(Rev, Tree) ->
                    case maps:get(Rev, Tree, undefined) of
                        undefined -> Tree;
                        Info -> Tree#{Rev => Info#{deleted => true}}
                    end
                end,
                RevTree,
                RevsToDelete
            ),
            %% Delete bodies of resolved conflicting revisions
            DeleteOps = [
                {body_delete, barrel_store_keys:doc_body_rev(DbName, DocId, Rev)}
                || Rev <- RevsToDelete
            ],
            %% If chosen rev is not the current winner, we need to create a new
            %% revision as child of chosen (to make it the deterministic winner)
            case ChosenRev =:= Winner of
                true ->
                    %% Just update the revtree + delete loser bodies
                    UpdatedColumns = lists:keyreplace(?COL_REVTREE, 1, Columns,
                                                      {?COL_REVTREE, encode_revtree(NewRevTree0)}),
                    AllOps = [{entity_put, DocEntityKey, UpdatedColumns} | DeleteOps],
                    case barrel_store_rocksdb:write_batch(StoreRef, AllOps) of
                        ok ->
                            {ok, #{id => DocId, rev => Winner, conflicts_resolved => length(RevsToDelete)}};
                        {error, _} = Err ->
                            Err
                    end;
                false ->
                    %% Create a new rev as child of chosen to make it the winner
                    ChosenGen = case binary:split(ChosenRev, <<"-">>) of
                        [GenBin, _] -> binary_to_integer(GenBin);
                        _ -> 1
                    end,
                    NewGen = ChosenGen + 1,
                    %% Generate a hash that will be higher than any competing branch
                    Hash = crypto:hash(md5, term_to_binary({DocId, ChosenRev, erlang:system_time()})),
                    HashHex = string:lowercase(binary:encode_hex(Hash)),
                    NewRev = <<(integer_to_binary(NewGen))/binary, "-", HashHex/binary>>,

                    %% Add new rev as child of chosen
                    NewRevTree = NewRevTree0#{
                        NewRev => #{id => NewRev, parent => ChosenRev, deleted => false}
                    },

                    UpdatedColumns = lists:keyreplace(?COL_REVTREE, 1, Columns,
                                                      {?COL_REVTREE, encode_revtree(NewRevTree)}),
                    UpdatedColumns2 = lists:keyreplace(?COL_REV, 1, UpdatedColumns,
                                                       {?COL_REV, NewRev}),
                    AllOps = [{entity_put, DocEntityKey, UpdatedColumns2} | DeleteOps],
                    case barrel_store_rocksdb:write_batch(StoreRef, AllOps) of
                        ok ->
                            {ok, #{id => DocId, rev => NewRev, conflicts_resolved => length(RevsToDelete)}};
                        {error, _} = Err ->
                            Err
                    end
            end
    end;

do_apply_resolution(StoreRef, DbName, DocId, DocEntityKey, Columns,
                    RevTree, Winner, Conflicts, {merge, MergedDoc}) ->
    %% Create a new revision that has all conflicting branches as parents
    %% This effectively merges all branches into one
    AllLeaves = [Winner | Conflicts],

    %% Generate new revision - use winner's generation + 1
    WinnerGen = case binary:split(Winner, <<"-">>) of
        [GenBin, _] -> binary_to_integer(GenBin);
        _ -> 1
    end,
    NewGen = WinnerGen + 1,

    %% Create new rev hash from merged content
    DocWithoutMeta = maps:without([<<"id">>, <<"_id">>, <<"_rev">>, <<"_deleted">>], MergedDoc),
    Hash = crypto:hash(md5, term_to_binary({DocWithoutMeta, AllLeaves})),
    HashHex = string:lowercase(binary:encode_hex(Hash)),
    NewRev = <<(integer_to_binary(NewGen))/binary, "-", HashHex/binary>>,

    %% Build new revtree: add new rev as child of winner, mark all conflicts as deleted
    NewRevTree0 = lists:foldl(
        fun(Rev, Tree) ->
            case maps:get(Rev, Tree, undefined) of
                undefined -> Tree;
                Info -> Tree#{Rev => Info#{deleted => true}}
            end
        end,
        RevTree,
        Conflicts
    ),
    %% Add the merged revision as child of winner
    NewRevTree = NewRevTree0#{
        NewRev => #{id => NewRev, parent => Winner, deleted => false}
    },

    %% Generate new HLC timestamp
    NextHlc = barrel_hlc:new_hlc(),

    %% Preserve existing tier metadata
    CreatedAt = proplists:get_value(?COL_CREATED_AT, Columns, barrel_hlc:encode(NextHlc)),
    ExistingTier = proplists:get_value(?COL_TIER, Columns, 0),
    ExistingExpires = proplists:get_value(?COL_EXPIRES_AT, Columns, 0),

    %% Update entity columns with new rev and revtree
    NewColumns = [
        {?COL_REV, NewRev},
        {?COL_DELETED, <<"false">>},
        {?COL_HLC, barrel_hlc:encode(NextHlc)},
        {?COL_REVTREE, encode_revtree(NewRevTree)},
        {?COL_CREATED_AT, CreatedAt},
        {?COL_EXPIRES_AT, ExistingExpires},
        {?COL_TIER, ExistingTier}
    ],

    %% Encode the merged document body
    CborBin = barrel_docdb_codec_cbor:encode(DocWithoutMeta),
    BodyKey = barrel_store_keys:doc_body(DbName, DocId),

    %% Delete bodies of resolved conflict revisions
    DeleteOps = [
        {body_delete, barrel_store_keys:doc_body_rev(DbName, DocId, Rev)}
        || Rev <- Conflicts
    ],

    %% Build batch operations
    EntityOp = {entity_put, DocEntityKey, NewColumns},
    BodyOp = {body_put, BodyKey, CborBin},

    %% Write batch (entity + new body + delete conflict bodies)
    case barrel_store_rocksdb:write_batch(StoreRef, [EntityOp, BodyOp | DeleteOps]) of
        ok ->
            {ok, #{id => DocId, rev => NewRev, conflicts_resolved => length(Conflicts)}};
        {error, _} = Err ->
            Err
    end.

%%====================================================================
%% Tier/TTL Operations
%%====================================================================

%% @doc Set TTL (expires_at timestamp) for a document
do_set_doc_ttl(StoreRef, DbName, DocId, ExpiresAt) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            %% Update only the expires_at column
            UpdatedColumns = lists:keyreplace(?COL_EXPIRES_AT, 1, Columns, {?COL_EXPIRES_AT, ExpiresAt}),
            case barrel_store_rocksdb:write_batch(StoreRef, [{entity_put, DocEntityKey, UpdatedColumns}]) of
                ok -> ok;
                Error -> Error
            end;
        not_found ->
            {error, not_found}
    end.

%% @doc Get TTL info for a document
do_get_doc_ttl(StoreRef, DbName, DocId) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            ExpiresAt = proplists:get_value(?COL_EXPIRES_AT, Columns, 0),
            case ExpiresAt of
                0 ->
                    {ok, undefined};
                _ ->
                    Now = erlang:system_time(millisecond),
                    Remaining = max(0, (ExpiresAt - Now) div 1000),
                    {ok, #{expires_at => ExpiresAt, remaining => Remaining}}
            end;
        not_found ->
            {error, not_found}
    end.

%% @doc Get the current tier of a document
do_get_doc_tier(StoreRef, DbName, DocId) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            %% decode_entity already converts tier to atom (hot, warm, cold)
            Tier = proplists:get_value(?COL_TIER, Columns, hot),
            {ok, Tier};
        not_found ->
            {error, not_found}
    end.

%% @doc Set the tier for a document
do_set_doc_tier(StoreRef, DbName, DocId, Tier) ->
    DocEntityKey = barrel_store_keys:doc_entity(DbName, DocId),
    case barrel_store_rocksdb:get_entity(StoreRef, DocEntityKey) of
        {ok, Columns} ->
            TierByte = barrel_tier:tier_to_byte(Tier),
            UpdatedColumns = lists:keyreplace(?COL_TIER, 1, Columns, {?COL_TIER, TierByte}),
            case barrel_store_rocksdb:write_batch(StoreRef, [{entity_put, DocEntityKey, UpdatedColumns}]) of
                ok -> ok;
                Error -> Error
            end;
        not_found ->
            {error, not_found}
    end.

%% @doc Classify documents by age into tiers
%% Scans all documents and updates their tier based on created_at timestamp.
do_classify_by_age(StoreRef, DbName, HotCutoff, WarmCutoff) ->
    %% Get all document entities in this database
    StartKey = barrel_store_keys:doc_entity(DbName, <<>>),
    EndKey = barrel_store_keys:doc_entity_end(DbName),

    Stats = #{classified => 0, errors => 0},

    FoldFun = fun(Key, Value, AccStats) ->
        case barrel_store_rocksdb:decode_entity(Value) of
            Columns when is_list(Columns) ->
                CreatedAtBin = proplists:get_value(?COL_CREATED_AT, Columns, <<>>),
                %% decode_entity returns tier as atom (hot, warm, cold)
                CurrentTier = proplists:get_value(?COL_TIER, Columns, hot),

                %% Decode created_at HLC to get timestamp (wall_time in ms)
                CreatedMs = case CreatedAtBin of
                    <<>> -> 0;
                    _ ->
                        CreatedHlc = barrel_hlc:decode(CreatedAtBin),
                        barrel_hlc:wall_time(CreatedHlc)
                end,

                %% Determine new tier based on age (use atoms)
                NewTier = if
                    CreatedMs >= HotCutoff -> hot;
                    CreatedMs >= WarmCutoff -> warm;
                    true -> cold
                end,

                %% Update if tier changed
                case NewTier =/= CurrentTier of
                    true ->
                        %% Store as byte value for encoding
                        NewTierByte = barrel_tier:tier_to_byte(NewTier),
                        UpdatedColumns = lists:keyreplace(?COL_TIER, 1, Columns, {?COL_TIER, NewTierByte}),
                        case barrel_store_rocksdb:write_batch(StoreRef, [{entity_put, Key, UpdatedColumns}]) of
                            ok ->
                                {ok, AccStats#{classified := maps:get(classified, AccStats) + 1}};
                            _ ->
                                {ok, AccStats#{errors := maps:get(errors, AccStats) + 1}}
                        end;
                    false ->
                        {ok, AccStats}
                end;
            _ ->
                {ok, AccStats#{errors := maps:get(errors, AccStats) + 1}}
        end
    end,

    FinalStats = barrel_store_rocksdb:fold_range(StoreRef, StartKey, EndKey, FoldFun, Stats),
    {ok, FinalStats}.

%% @doc Migrate expired documents (delete or archive)
do_migrate_expired(StoreRef, DbName) ->
    Now = erlang:system_time(millisecond),

    StartKey = barrel_store_keys:doc_entity(DbName, <<>>),
    EndKey = barrel_store_keys:doc_entity_end(DbName),

    Stats = #{deleted => 0, errors => 0},

    FoldFun = fun(Key, Value, AccStats) ->
        case barrel_store_rocksdb:decode_entity(Value) of
            Columns when is_list(Columns) ->
                ExpiresAt = proplists:get_value(?COL_EXPIRES_AT, Columns, 0),

                case ExpiresAt > 0 andalso Now >= ExpiresAt of
                    true ->
                        %% Document has expired - mark as deleted
                        UpdatedColumns = lists:keyreplace(?COL_DELETED, 1, Columns, {?COL_DELETED, <<"true">>}),
                        case barrel_store_rocksdb:write_batch(StoreRef, [{entity_put, Key, UpdatedColumns}]) of
                            ok ->
                                {ok, AccStats#{deleted := maps:get(deleted, AccStats) + 1}};
                            _ ->
                                {ok, AccStats#{errors := maps:get(errors, AccStats) + 1}}
                        end;
                    false ->
                        {ok, AccStats}
                end;
            _ ->
                {ok, AccStats}
        end
    end,

    FinalStats = barrel_store_rocksdb:fold_range(StoreRef, StartKey, EndKey, FoldFun, Stats),
    {ok, FinalStats}.

%% @doc Migrate documents matching filter to a target tier
%% Filter options:
%%   - min_age: minimum age in milliseconds
%%   - tier: only documents in this tier
do_migrate_to_tier(StoreRef, DbName, TargetTier, Filter) ->
    Now = erlang:system_time(millisecond),
    MinAge = maps:get(min_age, Filter, 0),
    SourceTier = maps:get(tier, Filter, undefined),
    %% Convert target tier to byte for storage
    TargetTierByte = barrel_tier:tier_to_byte(TargetTier),

    StartKey = barrel_store_keys:doc_entity(DbName, <<>>),
    EndKey = barrel_store_keys:doc_entity_end(DbName),

    Stats = #{migrated => 0, skipped => 0, errors => 0},

    FoldFun = fun(Key, Value, AccStats) ->
        case barrel_store_rocksdb:decode_entity(Value) of
            Columns when is_list(Columns) ->
                %% decode_entity returns tier as atom (hot, warm, cold)
                CurrentTier = proplists:get_value(?COL_TIER, Columns, hot),
                CreatedAtBin = proplists:get_value(?COL_CREATED_AT, Columns, <<>>),

                %% Check source tier filter (compare atoms)
                TierMatch = (SourceTier =:= undefined) orelse (CurrentTier =:= SourceTier),

                %% Check age filter (calculate document age in ms)
                AgeMs = case CreatedAtBin of
                    <<>> -> 0;
                    _ ->
                        CreatedHlc = barrel_hlc:decode(CreatedAtBin),
                        Now - barrel_hlc:wall_time(CreatedHlc)
                end,
                AgeMatch = AgeMs >= MinAge,

                case TierMatch andalso AgeMatch andalso CurrentTier =/= TargetTier of
                    true ->
                        %% Store as byte value
                        UpdatedColumns = lists:keyreplace(?COL_TIER, 1, Columns, {?COL_TIER, TargetTierByte}),
                        case barrel_store_rocksdb:write_batch(StoreRef, [{entity_put, Key, UpdatedColumns}]) of
                            ok ->
                                {ok, AccStats#{migrated := maps:get(migrated, AccStats) + 1}};
                            _ ->
                                {ok, AccStats#{errors := maps:get(errors, AccStats) + 1}}
                        end;
                    false ->
                        {ok, AccStats#{skipped := maps:get(skipped, AccStats) + 1}}
                end;
            _ ->
                {ok, AccStats#{errors := maps:get(errors, AccStats) + 1}}
        end
    end,

    FinalStats = barrel_store_rocksdb:fold_range(StoreRef, StartKey, EndKey, FoldFun, Stats),
    {ok, FinalStats}.
