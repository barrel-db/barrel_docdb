%%%-------------------------------------------------------------------
%%% @doc barrel_docdb - Public API for barrel_docdb
%%%
%%% This module provides the main public API for interacting with
%%% barrel_docdb databases. It wraps all lower-level operations into
%%% a clean, user-friendly interface.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb).

-include("barrel_docdb.hrl").

%% Database lifecycle
-export([
    create_db/1,
    create_db/2,
    open_db/1,
    close_db/1,
    delete_db/1,
    db_info/1,
    list_dbs/0
]).

%% Document CRUD
-export([
    put_doc/2,
    put_doc/3,
    get_doc/2,
    get_doc/3,
    delete_doc/2,
    delete_doc/3,
    fold_docs/3
]).

%% Attachments
-export([
    put_attachment/4,
    get_attachment/3,
    delete_attachment/3,
    list_attachments/2
]).

%% Views
-export([
    register_view/3,
    unregister_view/2,
    query_view/3,
    list_views/1,
    refresh_view/2
]).

%% Changes
-export([
    get_changes/2,
    get_changes/3,
    subscribe_changes/2,
    subscribe_changes/3
]).

%%====================================================================
%% Database Lifecycle
%%====================================================================

%% @doc Create a new database with default options
-spec create_db(binary()) -> {ok, pid()} | {error, term()}.
create_db(Name) ->
    create_db(Name, #{}).

%% @doc Create a new database with options
-spec create_db(binary(), map()) -> {ok, pid()} | {error, term()}.
create_db(Name, Opts) when is_binary(Name) ->
    case get_db(Name) of
        {ok, _Pid} ->
            {error, already_exists};
        {error, not_found} ->
            barrel_db_sup:start_db(Name, Opts)
    end.

%% @doc Open an existing database
-spec open_db(binary()) -> {ok, pid()} | {error, term()}.
open_db(Name) when is_binary(Name) ->
    get_db(Name).

%% @doc Close a database
-spec close_db(binary() | pid()) -> ok | {error, term()}.
close_db(Name) when is_binary(Name) ->
    case get_db(Name) of
        {ok, Pid} ->
            barrel_db_server:stop(Pid),
            ok;
        {error, _} = Error ->
            Error
    end;
close_db(Pid) when is_pid(Pid) ->
    barrel_db_server:stop(Pid),
    ok.

%% @doc Delete a database
-spec delete_db(binary()) -> ok | {error, term()}.
delete_db(Name) when is_binary(Name) ->
    case get_db(Name) of
        {ok, Pid} ->
            {ok, Info} = barrel_db_server:info(Pid),
            DbPath = maps:get(db_path, Info),
            barrel_db_server:stop(Pid),
            %% Remove data directory
            os:cmd("rm -rf " ++ DbPath),
            ok;
        {error, not_found} ->
            ok
    end.

%% @doc Get database info
-spec db_info(binary() | pid()) -> {ok, map()} | {error, term()}.
db_info(Name) when is_binary(Name) ->
    case get_db(Name) of
        {ok, Pid} ->
            barrel_db_server:info(Pid);
        {error, _} = Error ->
            Error
    end;
db_info(Pid) when is_pid(Pid) ->
    barrel_db_server:info(Pid).

%% @doc List all open databases
-spec list_dbs() -> [binary()].
list_dbs() ->
    %% Get all database names from persistent_term
    lists:filtermap(
        fun({Key, Value}) ->
            case Key of
                {barrel_db, Name} when is_pid(Value), is_binary(Name) ->
                    case is_process_alive(Value) of
                        true -> {true, Name};
                        false -> false
                    end;
                _ -> false
            end
        end,
        persistent_term:get()
    ).

%%====================================================================
%% Document CRUD
%%====================================================================

%% @doc Put a document (create or update)
-spec put_doc(binary() | pid(), map()) -> {ok, map()} | {error, term()}.
put_doc(Db, Doc) ->
    put_doc(Db, Doc, #{}).

%% @doc Put a document with options
-spec put_doc(binary() | pid(), map(), map()) -> {ok, map()} | {error, term()}.
put_doc(Db, Doc, Opts) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:put_doc(Pid, Doc, Opts)
    end).

%% @doc Get a document by ID
-spec get_doc(binary() | pid(), binary()) -> {ok, map()} | {error, term()}.
get_doc(Db, DocId) ->
    get_doc(Db, DocId, #{}).

%% @doc Get a document with options
-spec get_doc(binary() | pid(), binary(), map()) -> {ok, map()} | {error, term()}.
get_doc(Db, DocId, Opts) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:get_doc(Pid, DocId, Opts)
    end).

%% @doc Delete a document
-spec delete_doc(binary() | pid(), binary()) -> {ok, map()} | {error, term()}.
delete_doc(Db, DocId) ->
    delete_doc(Db, DocId, #{}).

%% @doc Delete a document with options
-spec delete_doc(binary() | pid(), binary(), map()) -> {ok, map()} | {error, term()}.
delete_doc(Db, DocId, Opts) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:delete_doc(Pid, DocId, Opts)
    end).

%% @doc Fold over all documents
-spec fold_docs(binary() | pid(), fun((map(), term()) -> {ok, term()} | {stop, term()} | stop), term()) ->
    {ok, term()}.
fold_docs(Db, Fun, Acc) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:fold_docs(Pid, Fun, Acc)
    end).

%%====================================================================
%% Attachments
%%====================================================================

%% @doc Put an attachment
-spec put_attachment(binary() | pid(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put_attachment(Db, DocId, AttName, Data) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:put_attachment(AttRef, DbName, DocId, AttName, Data)
    end).

%% @doc Get an attachment
-spec get_attachment(binary() | pid(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
get_attachment(Db, DocId, AttName) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:get_attachment(AttRef, DbName, DocId, AttName)
    end).

%% @doc Delete an attachment
-spec delete_attachment(binary() | pid(), binary(), binary()) -> ok | {error, term()}.
delete_attachment(Db, DocId, AttName) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:delete_attachment(AttRef, DbName, DocId, AttName)
    end).

%% @doc List attachments for a document
-spec list_attachments(binary() | pid(), binary()) -> [binary()].
list_attachments(Db, DocId) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:list_attachments(AttRef, DbName, DocId)
    end).

%%====================================================================
%% Views
%%====================================================================

%% @doc Register a view
-spec register_view(binary() | pid(), binary(), map()) -> ok | {error, term()}.
register_view(Db, ViewId, Config) ->
    with_db(Db, fun(Pid) ->
        barrel_view:register(Pid, ViewId, Config)
    end).

%% @doc Unregister a view
-spec unregister_view(binary() | pid(), binary()) -> ok | {error, term()}.
unregister_view(Db, ViewId) ->
    with_db(Db, fun(Pid) ->
        barrel_view:unregister(Pid, ViewId)
    end).

%% @doc Query a view
-spec query_view(binary() | pid(), binary(), map()) -> {ok, [map()]} | {error, term()}.
query_view(Db, ViewId, Opts) ->
    with_db(Db, fun(Pid) ->
        barrel_view:query(Pid, ViewId, Opts)
    end).

%% @doc List all views
-spec list_views(binary() | pid()) -> {ok, [map()]} | {error, term()}.
list_views(Db) ->
    with_db(Db, fun(Pid) ->
        barrel_view:list(Pid)
    end).

%% @doc Refresh a view (wait for it to be up-to-date)
-spec refresh_view(binary() | pid(), binary()) -> {ok, seq()} | {error, term()}.
refresh_view(Db, ViewId) ->
    with_db(Db, fun(Pid) ->
        barrel_view:refresh(Pid, ViewId)
    end).

%%====================================================================
%% Changes
%%====================================================================

%% @doc Get changes since a sequence
-spec get_changes(binary() | pid(), seq() | first) -> {ok, [map()], seq()}.
get_changes(Db, Since) ->
    get_changes(Db, Since, #{}).

%% @doc Get changes with options
-spec get_changes(binary() | pid(), seq() | first, map()) -> {ok, [map()], seq()}.
get_changes(Db, Since, Opts) ->
    with_db(Db, fun(Pid) ->
        {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_changes:get_changes(StoreRef, DbName, Since, Opts)
    end).

%% @doc Subscribe to changes stream (iterate mode)
-spec subscribe_changes(binary() | pid(), seq() | first) -> {ok, pid()} | {error, term()}.
subscribe_changes(Db, Since) ->
    subscribe_changes(Db, Since, #{}).

%% @doc Subscribe to changes stream with options
-spec subscribe_changes(binary() | pid(), seq() | first, map()) -> {ok, pid()} | {error, term()}.
subscribe_changes(Db, Since, Opts) ->
    with_db(Db, fun(Pid) ->
        {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        StreamOpts = Opts#{since => Since},
        barrel_changes_stream:start_link(StoreRef, DbName, StreamOpts)
    end).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Get database pid by name
-spec get_db(binary()) -> {ok, pid()} | {error, not_found}.
get_db(Name) when is_binary(Name) ->
    case persistent_term:get({barrel_db, Name}, undefined) of
        undefined ->
            {error, not_found};
        Pid when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true -> {ok, Pid};
                false ->
                    %% Cleanup stale entry
                    persistent_term:erase({barrel_db, Name}),
                    {error, not_found}
            end
    end.

%% @private Execute function with database pid
-spec with_db(binary() | pid(), fun((pid()) -> term())) -> term().
with_db(Pid, Fun) when is_pid(Pid) ->
    Fun(Pid);
with_db(Name, Fun) when is_binary(Name) ->
    case get_db(Name) of
        {ok, Pid} ->
            Fun(Pid);
        {error, _} = Error ->
            Error
    end.
