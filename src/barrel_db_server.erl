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
    get_doc/3,
    delete_doc/3,
    fold_docs/3
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
    delete_local_doc/2
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
%% Document API functions
%%====================================================================

%% @doc Put a document (create or update)
-spec put_doc(pid(), map(), map()) -> {ok, map()} | {error, term()}.
put_doc(Pid, Doc, Opts) ->
    gen_server:call(Pid, {put_doc, Doc, Opts}).

%% @doc Get a document
-spec get_doc(pid(), binary(), map()) -> {ok, map()} | {error, not_found} | {error, term()}.
get_doc(Pid, DocId, Opts) ->
    gen_server:call(Pid, {get_doc, DocId, Opts}).

%% @doc Delete a document
-spec delete_doc(pid(), binary(), map()) -> {ok, map()} | {error, term()}.
delete_doc(Pid, DocId, Opts) ->
    gen_server:call(Pid, {delete_doc, DocId, Opts}).

%% @doc Fold over all documents
-spec fold_docs(pid(), fun(), term()) -> {ok, term()}.
fold_docs(Pid, Fun, Acc) ->
    gen_server:call(Pid, {fold_docs, Fun, Acc}, infinity).

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

%% Document operations
handle_call({put_doc, Doc, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_put_doc(StoreRef, DbName, Doc, Opts),
    {reply, Result, State};

handle_call({get_doc, DocId, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_get_doc(StoreRef, DbName, DocId, Opts),
    {reply, Result, State};

handle_call({delete_doc, DocId, Opts}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_delete_doc(StoreRef, DbName, DocId, Opts),
    {reply, Result, State};

handle_call({fold_docs, Fun, Acc}, _From,
            #state{name = DbName, store_ref = StoreRef} = State) ->
    Result = do_fold_docs(StoreRef, DbName, Fun, Acc),
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

%%====================================================================
%% Document Operations
%%====================================================================

%% @doc Put a document (create or update)
do_put_doc(StoreRef, DbName, Doc, _Opts) ->
    %% Build document record from input
    DocRecord = barrel_doc:make_doc_record(Doc),
    #{id := DocId, revs := Revs, deleted := Deleted, doc := DocBody} = DocRecord,
    [NewRev | _] = Revs,

    %% Check for existing document and get old doc body for path index update
    DocInfoKey = barrel_store_keys:doc_info(DbName, DocId),
    {OldHlc, OldDocBody} = case barrel_store_rocksdb:get(StoreRef, DocInfoKey) of
        {ok, ExistingBin} ->
            ExistingDocInfo = binary_to_term(ExistingBin),
            ExistingRev = maps:get(rev, ExistingDocInfo),
            %% Get old doc body for path index diff
            OldBody = case barrel_store_rocksdb:get(StoreRef,
                            barrel_store_keys:doc_rev(DbName, DocId, ExistingRev)) of
                {ok, OldBodyBin} -> binary_to_term(OldBodyBin);
                not_found -> #{}
            end,
            {maps:get(hlc, ExistingDocInfo, undefined), OldBody};
        not_found ->
            {undefined, undefined}
    end,

    %% Build revision tree
    RevTree = case length(Revs) of
        1 ->
            %% New document
            #{NewRev => #{id => NewRev, parent => undefined, deleted => Deleted}};
        _ ->
            %% Update - build tree with history
            [CurRev, ParentRev | _] = Revs,
            #{
                ParentRev => #{id => ParentRev, parent => undefined, deleted => false},
                CurRev => #{id => CurRev, parent => ParentRev, deleted => Deleted}
            }
    end,

    %% Generate new HLC timestamp for this change
    NextHlc = barrel_hlc:new_hlc(),

    %% Build doc_info
    DocInfo = #{
        id => DocId,
        rev => NewRev,
        deleted => Deleted,
        revtree => RevTree,
        hlc => NextHlc
    },

    %% Prepare batch operations for document
    DocOps = [
        %% Write doc_info
        {put, DocInfoKey, term_to_binary(DocInfo)},
        %% Write doc body for revision
        {put, barrel_store_keys:doc_rev(DbName, DocId, NewRev), term_to_binary(DocBody)}
    ],

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

    %% Write batch atomically (doc + path index + change + path_hlc in single batch)
    AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps,
    ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps),

    %% Notify path subscribers
    notify_subscribers(DbName, DocId, NewRev, NextHlc, Deleted, DocBody),

    %% Return result
    Result = #{
        <<"id">> => DocId,
        <<"ok">> => true,
        <<"rev">> => NewRev
    },
    {ok, Result}.

%% @doc Get a document by ID
do_get_doc(StoreRef, DbName, DocId, Opts) ->
    DocInfoKey = barrel_store_keys:doc_info(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, DocInfoKey) of
        {ok, DocInfoBin} ->
            DocInfo = binary_to_term(DocInfoBin),
            Rev = maps:get(rev, DocInfo),
            Deleted = maps:get(deleted, DocInfo, false),
            IncludeDeleted = maps:get(include_deleted, Opts, false),

            case {Deleted, IncludeDeleted} of
                {true, false} ->
                    {error, not_found};
                _ ->
                    %% Get document body
                    DocRevKey = barrel_store_keys:doc_rev(DbName, DocId, Rev),
                    case barrel_store_rocksdb:get(StoreRef, DocRevKey) of
                        {ok, DocBin} ->
                            DocBody = binary_to_term(DocBin),
                            %% Add metadata
                            Result = DocBody#{
                                <<"id">> => DocId,
                                <<"_rev">> => Rev
                            },
                            Result2 = case Deleted of
                                true -> Result#{<<"_deleted">> => true};
                                false -> Result
                            end,
                            {ok, Result2};
                        not_found ->
                            {error, not_found}
                    end
            end;
        not_found ->
            {error, not_found}
    end.

%% @doc Delete a document
do_delete_doc(StoreRef, DbName, DocId, Opts) ->
    %% Get current doc info
    DocInfoKey = barrel_store_keys:doc_info(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, DocInfoKey) of
        {ok, DocInfoBin} ->
            DocInfo = binary_to_term(DocInfoBin),
            CurrentRev = maps:get(rev, DocInfo),

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

            %% Get old HLC and generate new one
            OldHlc = maps:get(hlc, DocInfo, undefined),
            NextHlc = barrel_hlc:new_hlc(),

            %% Update revision tree
            RevTree = maps:get(revtree, DocInfo, #{}),
            NewRevTree = RevTree#{NewRev => #{id => NewRev, parent => CurrentRev, deleted => true}},

            %% Build new doc_info
            NewDocInfo = DocInfo#{
                rev => NewRev,
                deleted => true,
                revtree => NewRevTree,
                hlc => NextHlc
            },

            %% Prepare doc operations
            DocOps = [
                {put, DocInfoKey, term_to_binary(NewDocInfo)}
            ],

            HlcDeleteOps = case OldHlc of
                undefined -> [];
                _ -> [{delete, barrel_store_keys:doc_hlc(DbName, OldHlc)}]
            end,

            %% Path index removal operations
            PathIndexOps = case barrel_ars_index:get_doc_paths(StoreRef, DbName, DocId) of
                {ok, Paths} -> barrel_ars_index:remove_doc_ops(DbName, DocId, Paths);
                not_found -> []
            end,

            %% Change entry operations
            ChangeOps = barrel_changes:write_change_ops(DbName, NextHlc, NewDocInfo),

            %% Path-indexed change operations for delete
            %% Get old doc body to remove old path entries
            OldDocBody = case barrel_store_rocksdb:get(StoreRef,
                              barrel_store_keys:doc_rev(DbName, DocId, CurrentRev)) of
                {ok, OldBodyBin} -> binary_to_term(OldBodyBin);
                not_found -> undefined
            end,
            PathHlcOps = barrel_changes:update_path_index_ops(DbName, NextHlc, NewDocInfo,
                                                               OldHlc, OldDocBody),

            %% Write batch atomically (doc + path index + change + path_hlc in single batch)
            AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps,
            ok = barrel_store_rocksdb:write_batch(StoreRef, AllOps),

            %% Notify path subscribers
            notify_subscribers(DbName, DocId, NewRev, NextHlc, true, #{}),

            {ok, #{<<"id">> => DocId, <<"ok">> => true, <<"rev">> => NewRev}};

        not_found ->
            {error, not_found}
    end.

%% @doc Fold over all documents
do_fold_docs(StoreRef, DbName, Fun, Acc) ->
    StartKey = barrel_store_keys:doc_info_prefix(DbName),
    EndKey = barrel_store_keys:doc_info_end(DbName),

    FoldFun = fun(_Key, Value, AccIn) ->
        DocInfo = binary_to_term(Value),
        Deleted = maps:get(deleted, DocInfo, false),
        case Deleted of
            true ->
                %% Skip deleted documents
                {ok, AccIn};
            false ->
                DocId = maps:get(id, DocInfo),
                Rev = maps:get(rev, DocInfo),
                %% Get document body
                DocRevKey = barrel_store_keys:doc_rev(DbName, DocId, Rev),
                DocBody = case barrel_store_rocksdb:get(StoreRef, DocRevKey) of
                    {ok, DocBin} -> binary_to_term(DocBin);
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
        end
    end,

    FinalAcc = barrel_store_rocksdb:fold_range(StoreRef, StartKey, EndKey, FoldFun, Acc),
    {ok, FinalAcc}.

%%====================================================================
%% Replication Operations
%%====================================================================

%% @doc Put a document with explicit revision history (for replication)
do_put_rev(StoreRef, DbName, Doc, History, Deleted) ->
    DocId = maps:get(<<"id">>, Doc),
    DocBody = barrel_doc:doc_without_meta(Doc),
    [NewRev | _] = History,

    %% Check for existing document and get old doc body for path index update
    DocInfoKey = barrel_store_keys:doc_info(DbName, DocId),
    {ExistingRevTree, OldHlc, OldDocBody} = case barrel_store_rocksdb:get(StoreRef, DocInfoKey) of
        {ok, ExistingBin} ->
            ExistingDocInfo = binary_to_term(ExistingBin),
            ExistingRev = maps:get(rev, ExistingDocInfo),
            %% Get old doc body for path index diff
            OldBody = case barrel_store_rocksdb:get(StoreRef,
                            barrel_store_keys:doc_rev(DbName, DocId, ExistingRev)) of
                {ok, OldBodyBin} -> binary_to_term(OldBodyBin);
                not_found -> #{}
            end,
            {maps:get(revtree, ExistingDocInfo, #{}),
             maps:get(hlc, ExistingDocInfo, undefined),
             OldBody};
        not_found ->
            {#{}, undefined, undefined}
    end,

    %% Build revision tree from history
    NewRevTree = build_revtree_from_history(History, Deleted, ExistingRevTree),

    %% Generate new HLC timestamp for this change
    NextHlc = barrel_hlc:new_hlc(),

    %% Build doc_info
    DocInfo = #{
        id => DocId,
        rev => NewRev,
        deleted => Deleted,
        revtree => NewRevTree,
        hlc => NextHlc
    },

    %% Prepare doc operations
    DocOps = [
        {put, DocInfoKey, term_to_binary(DocInfo)},
        {put, barrel_store_keys:doc_rev(DbName, DocId, NewRev), term_to_binary(DocBody)}
    ],

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

    %% Write batch atomically (doc + path index + change + path_hlc in single batch)
    AllOps = DocOps ++ HlcDeleteOps ++ PathIndexOps ++ ChangeOps ++ PathHlcOps,
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

%% @doc Get revisions difference
do_revsdiff(StoreRef, DbName, DocId, RevIds) ->
    DocInfoKey = barrel_store_keys:doc_info(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, DocInfoKey) of
        {ok, DocInfoBin} ->
            DocInfo = binary_to_term(DocInfoBin),
            RevTree = maps:get(revtree, DocInfo, #{}),

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

%% @doc Put a local document
do_put_local_doc(StoreRef, DbName, DocId, Doc) ->
    Key = barrel_store_keys:local_doc(DbName, DocId),
    Value = term_to_binary(Doc),
    ok = barrel_store_rocksdb:put(StoreRef, Key, Value),
    ok.

%% @doc Get a local document
do_get_local_doc(StoreRef, DbName, DocId) ->
    Key = barrel_store_keys:local_doc(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, Value} ->
            {ok, binary_to_term(Value)};
        not_found ->
            {error, not_found}
    end.

%% @doc Delete a local document
do_delete_local_doc(StoreRef, DbName, DocId) ->
    Key = barrel_store_keys:local_doc(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, _} ->
            ok = barrel_store_rocksdb:delete(StoreRef, Key),
            ok;
        not_found ->
            {error, not_found}
    end.

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
