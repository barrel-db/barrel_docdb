%%%-------------------------------------------------------------------
%%% @author Benoit Chesneau
%%% @copyright (C) 2024, Benoit Chesneau
%%% @doc barrel_docdb - Public API for barrel_docdb
%%%
%%% This module provides the main public API for interacting with
%%% barrel_docdb databases. It supports:
%%%
%%% <ul>
%%%   <li>Database lifecycle management (create, open, close, delete)</li>
%%%   <li>Document CRUD operations with MVCC revision control</li>
%%%   <li>Binary attachments stored efficiently with BlobDB</li>
%%%   <li>Secondary indexes (views) with automatic updates</li>
%%%   <li>Changes feed for tracking document modifications</li>
%%%   <li>Replication primitives for syncing databases</li>
%%% </ul>
%%%
%%% == Quick Start ==
%%%
%%% ```
%%% %% Create a database
%%% {ok, _} = barrel_docdb:create_db(<<"mydb">>),
%%%
%%% %% Store a document
%%% {ok, #{<<"id">> := DocId, <<"rev">> := Rev}} =
%%%     barrel_docdb:put_doc(<<"mydb">>, #{
%%%         <<"id">> => <<"doc1">>,
%%%         <<"type">> => <<"user">>,
%%%         <<"name">> => <<"Alice">>
%%%     }),
%%%
%%% %% Retrieve the document
%%% {ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"doc1">>),
%%%
%%% %% Update the document (must include _rev)
%%% {ok, _} = barrel_docdb:put_doc(<<"mydb">>, Doc#{<<"name">> => <<"Bob">>}),
%%%
%%% %% Delete the document
%%% {ok, _} = barrel_docdb:delete_doc(<<"mydb">>, DocId).
%%% '''
%%%
%%% == Document Structure ==
%%%
%%% Documents are Erlang maps with the following special keys:
%%%
%%% <ul>
%%%   <li>`<<"id">>' - Document identifier (auto-generated if not provided)</li>
%%%   <li>`<<"_rev">>' - Revision identifier (managed by the system)</li>
%%%   <li>`<<"_deleted">>' - Set to `true' for deleted documents</li>
%%% </ul>
%%%
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

%% Query (declarative queries using path index)
-export([
    find/2,
    find/3,
    explain/2
]).

%% Changes
-export([
    get_changes/2,
    get_changes/3,
    subscribe_changes/2,
    subscribe_changes/3
]).

%% Replication primitives
-export([
    put_rev/4,
    revsdiff/3
]).

%% Local documents (for checkpoints, not replicated)
-export([
    put_local_doc/3,
    get_local_doc/2,
    delete_local_doc/2
]).

%% HLC (Hybrid Logical Clock) for distributed time synchronization
-export([
    get_hlc/0,
    sync_hlc/1,
    new_hlc/0
]).

%% Path Subscriptions (real-time document change notifications)
-export([
    subscribe/2,
    subscribe/3,
    unsubscribe/1
]).

%% Query Subscriptions (real-time query-based change notifications)
-export([
    subscribe_query/2,
    subscribe_query/3,
    unsubscribe_query/1
]).

%%====================================================================
%% Database Lifecycle
%%====================================================================

%% @doc Create a new database with default options.
%%
%% Creates a new database with the given name using default settings.
%% The database will be stored in the default data directory.
%%
%% == Example ==
%% ```
%% {ok, Pid} = barrel_docdb:create_db(<<"mydb">>).
%% '''
%%
%% @param Name The database name as a binary
%% @returns `{ok, Pid}' on success, `{error, already_exists}' if database exists
%% @see create_db/2
-spec create_db(binary()) -> {ok, pid()} | {error, term()}.
create_db(Name) ->
    create_db(Name, #{}).

%% @doc Create a new database with options.
%%
%% Creates a new database with custom configuration options.
%%
%% == Options ==
%% <ul>
%%   <li>`data_dir' - Directory to store database files (default: `/tmp/barrel_data')</li>
%%   <li>`store_opts' - RocksDB options for document store</li>
%%   <li>`att_opts' - RocksDB options for attachment store</li>
%% </ul>
%%
%% == Example ==
%% ```
%% {ok, Pid} = barrel_docdb:create_db(<<"mydb">>, #{
%%     data_dir => "/var/lib/barrel"
%% }).
%% '''
%%
%% @param Name The database name as a binary
%% @param Opts Configuration options map
%% @returns `{ok, Pid}' on success, `{error, already_exists}' if database exists
-spec create_db(binary(), map()) -> {ok, pid()} | {error, term()}.
create_db(Name, Opts) when is_binary(Name) ->
    case get_db(Name) of
        {ok, _Pid} ->
            {error, already_exists};
        {error, not_found} ->
            barrel_db_sup:start_db(Name, Opts)
    end.

%% @doc Open an existing database.
%%
%% Returns the pid of an already running database. Databases are
%% automatically opened when created and remain open until explicitly
%% closed or the application stops.
%%
%% == Example ==
%% ```
%% {ok, Pid} = barrel_docdb:open_db(<<"mydb">>).
%% '''
%%
%% @param Name The database name as a binary
%% @returns `{ok, Pid}' if database is open, `{error, not_found}' otherwise
-spec open_db(binary()) -> {ok, pid()} | {error, term()}.
open_db(Name) when is_binary(Name) ->
    get_db(Name).

%% @doc Close a database.
%%
%% Stops the database process and releases resources. The database
%% can be reopened by calling `create_db/1' again.
%%
%% == Example ==
%% ```
%% ok = barrel_docdb:close_db(<<"mydb">>).
%% '''
%%
%% @param Db Database name or pid
%% @returns `ok' on success, `{error, not_found}' if database doesn't exist
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

%% @doc Delete a database and all its data.
%%
%% Permanently removes the database, including all documents, attachments,
%% and indexes. This operation cannot be undone.
%%
%% == Example ==
%% ```
%% ok = barrel_docdb:delete_db(<<"mydb">>).
%% '''
%%
%% @param Name The database name as a binary
%% @returns `ok' on success (also returns `ok' if database doesn't exist)
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

%% @doc Get database information.
%%
%% Returns metadata about the database including its name, path, and pid.
%%
%% == Example ==
%% ```
%% {ok, Info} = barrel_docdb:db_info(<<"mydb">>),
%% Name = maps:get(name, Info).
%% '''
%%
%% @param Db Database name or pid
%% @returns `{ok, InfoMap}' with database metadata
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

%% @doc List all open databases.
%%
%% Returns the names of all currently open databases.
%%
%% == Example ==
%% ```
%% DbNames = barrel_docdb:list_dbs().
%% %% Returns [<<"db1">>, <<"db2">>, ...]
%% '''
%%
%% @returns List of database names
-spec list_dbs() -> [binary()].
list_dbs() ->
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

%% @doc Create or update a document.
%%
%% Stores a document in the database. If the document has an `<<"id">>'
%% key, that ID is used; otherwise, a unique ID is generated.
%%
%% For updates, the document must include the current `<<"_rev">>' value.
%% This ensures optimistic concurrency control.
%%
%% == Example ==
%% ```
%% %% Create a new document
%% {ok, Result} = barrel_docdb:put_doc(<<"mydb">>, #{
%%     <<"type">> => <<"user">>,
%%     <<"name">> => <<"Alice">>
%% }),
%% DocId = maps:get(<<"id">>, Result),
%% Rev = maps:get(<<"rev">>, Result).
%% '''
%%
%% @param Db Database name or pid
%% @param Doc Document map to store
%% @returns `{ok, #{<<"id">> => DocId, <<"rev">> => Rev, <<"ok">> => true}}'
%% @see put_doc/3
-spec put_doc(binary() | pid(), map()) -> {ok, map()} | {error, term()}.
put_doc(Db, Doc) ->
    put_doc(Db, Doc, #{}).

%% @doc Create or update a document with options.
%%
%% Same as `put_doc/2' but accepts additional options.
%%
%% @param Db Database name or pid
%% @param Doc Document map to store
%% @param Opts Options map (currently unused, reserved for future use)
%% @returns `{ok, #{<<"id">> => DocId, <<"rev">> => Rev, <<"ok">> => true}}'
-spec put_doc(binary() | pid(), map(), map()) -> {ok, map()} | {error, term()}.
put_doc(Db, Doc, Opts) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:put_doc(Pid, Doc, Opts)
    end).

%% @doc Get a document by ID.
%%
%% Retrieves a document from the database. Returns `{error, not_found}'
%% if the document doesn't exist or has been deleted.
%%
%% == Example ==
%% ```
%% {ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"doc1">>),
%% Name = maps:get(<<"name">>, Doc).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @returns `{ok, Document}' or `{error, not_found}'
%% @see get_doc/3
-spec get_doc(binary() | pid(), binary()) -> {ok, map()} | {error, term()}.
get_doc(Db, DocId) ->
    get_doc(Db, DocId, #{}).

%% @doc Get a document with options.
%%
%% Retrieves a document with additional options.
%%
%% == Options ==
%% <ul>
%%   <li>`include_deleted' - If `true', returns deleted documents</li>
%%   <li>`rev' - Specific revision to retrieve</li>
%% </ul>
%%
%% == Example ==
%% ```
%% %% Get a deleted document
%% {ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"doc1">>, #{
%%     include_deleted => true
%% }).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param Opts Options map
%% @returns `{ok, Document}' or `{error, not_found}'
-spec get_doc(binary() | pid(), binary(), map()) -> {ok, map()} | {error, term()}.
get_doc(Db, DocId, Opts) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:get_doc(Pid, DocId, Opts)
    end).

%% @doc Delete a document.
%%
%% Marks a document as deleted. The document's revision history is
%% preserved for conflict resolution and replication.
%%
%% == Example ==
%% ```
%% {ok, Result} = barrel_docdb:delete_doc(<<"mydb">>, <<"doc1">>).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @returns `{ok, #{<<"id">> => DocId, <<"rev">> => NewRev, <<"ok">> => true}}'
%% @see delete_doc/3
-spec delete_doc(binary() | pid(), binary()) -> {ok, map()} | {error, term()}.
delete_doc(Db, DocId) ->
    delete_doc(Db, DocId, #{}).

%% @doc Delete a document with options.
%%
%% == Options ==
%% <ul>
%%   <li>`rev' - Expected current revision (for conflict detection)</li>
%% </ul>
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param Opts Options map
%% @returns `{ok, #{<<"id">> => DocId, <<"rev">> => NewRev, <<"ok">> => true}}'
-spec delete_doc(binary() | pid(), binary(), map()) -> {ok, map()} | {error, term()}.
delete_doc(Db, DocId, Opts) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:delete_doc(Pid, DocId, Opts)
    end).

%% @doc Fold over all documents in the database.
%%
%% Iterates over all non-deleted documents, calling the provided function
%% for each document. The function receives the document and an accumulator.
%%
%% == Callback Return Values ==
%% <ul>
%%   <li>`{ok, NewAcc}' - Continue with new accumulator</li>
%%   <li>`{stop, FinalAcc}' - Stop iteration with final accumulator</li>
%%   <li>`stop' - Stop iteration with current accumulator</li>
%% </ul>
%%
%% == Example ==
%% ```
%% %% Count all documents
%% {ok, Count} = barrel_docdb:fold_docs(<<"mydb">>,
%%     fun(_Doc, Acc) -> {ok, Acc + 1} end,
%%     0
%% ).
%% '''
%%
%% @param Db Database name or pid
%% @param Fun Callback function `fun((Doc, Acc) -> {ok, Acc} | {stop, Acc} | stop)'
%% @param Acc Initial accumulator value
%% @returns `{ok, FinalAcc}'
-spec fold_docs(binary() | pid(), fun((map(), term()) -> {ok, term()} | {stop, term()} | stop), term()) ->
    {ok, term()}.
fold_docs(Db, Fun, Acc) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:fold_docs(Pid, Fun, Acc)
    end).

%%====================================================================
%% Attachments
%%====================================================================

%% @doc Attach binary data to a document.
%%
%% Stores a binary attachment associated with a document. Attachments
%% are stored in a separate BlobDB-enabled RocksDB instance optimized
%% for large binary data.
%%
%% == Example ==
%% ```
%% Data = <<"Hello, World!">>,
%% {ok, Info} = barrel_docdb:put_attachment(<<"mydb">>, <<"doc1">>,
%%     <<"greeting.txt">>, Data).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param AttName Attachment name
%% @param Data Binary data to store
%% @returns `{ok, AttachmentInfo}'
-spec put_attachment(binary() | pid(), binary(), binary(), binary()) ->
    {ok, map()} | {error, term()}.
put_attachment(Db, DocId, AttName, Data) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:put_attachment(AttRef, DbName, DocId, AttName, Data)
    end).

%% @doc Retrieve an attachment.
%%
%% Gets the binary data of an attachment.
%%
%% == Example ==
%% ```
%% {ok, Data} = barrel_docdb:get_attachment(<<"mydb">>, <<"doc1">>,
%%     <<"greeting.txt">>).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param AttName Attachment name
%% @returns `{ok, BinaryData}' or `{error, not_found}'
-spec get_attachment(binary() | pid(), binary(), binary()) ->
    {ok, binary()} | {error, term()}.
get_attachment(Db, DocId, AttName) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:get_attachment(AttRef, DbName, DocId, AttName)
    end).

%% @doc Delete an attachment.
%%
%% Removes an attachment from a document.
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param AttName Attachment name
%% @returns `ok' or `{error, not_found}'
-spec delete_attachment(binary() | pid(), binary(), binary()) -> ok | {error, term()}.
delete_attachment(Db, DocId, AttName) ->
    with_db(Db, fun(Pid) ->
        {ok, AttRef} = barrel_db_server:get_att_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_att:delete_attachment(AttRef, DbName, DocId, AttName)
    end).

%% @doc List all attachments for a document.
%%
%% Returns the names of all attachments associated with a document.
%%
%% == Example ==
%% ```
%% AttNames = barrel_docdb:list_attachments(<<"mydb">>, <<"doc1">>).
%% %% Returns [<<"file1.txt">>, <<"image.png">>]
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @returns List of attachment names
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

%% @doc Register a secondary index (view).
%%
%% Creates a view that maintains a secondary index over documents.
%% Views can be module-based or query-based.
%%
%% == Module-based View ==
%% The view module must implement the `barrel_view' behaviour.
%% ```
%% -module(by_type_view).
%% -behaviour(barrel_view).
%% -export([version/0, map/1]).
%%
%% version() -> 1.
%%
%% map(#{<<"type">> := Type}) -> [{Type, 1}];
%% map(_) -> [].
%%
%% %% Registration:
%% ok = barrel_docdb:register_view(<<"mydb">>, <<"by_type">>, #{
%%     module => by_type_view
%% }).
%% '''
%%
%% == Query-based View ==
%% Query-based views use declarative queries instead of module callbacks.
%% They automatically index documents matching the query conditions.
%% ```
%% %% Create a materialized view from a query
%% ok = barrel_docdb:register_view(<<"mydb">>, <<"users_by_org">>, #{
%%     query => #{
%%         where => [
%%             {path, [<<"type">>], <<"user">>},
%%             {path, [<<"org_id">>], '?Org'}
%%         ],
%%         key => '?Org'       %% Use org_id as the index key
%%     },
%%     reduce => '_count'      %% Optional: count users per org
%% }).
%%
%% %% Query the materialized view
%% {ok, Results} = barrel_docdb:query_view(<<"mydb">>, <<"users_by_org">>, #{
%%     reduce => true          %% Apply reduce
%% }).
%% '''
%%
%% == Configuration Options ==
%% <ul>
%%   <li>`module' - Module implementing barrel_view behaviour (module-based)</li>
%%   <li>`query' - Query specification with `where' and `key' (query-based)</li>
%%   <li>`reduce' - Optional reduce: '_count', '_sum', '_stats' (both types)</li>
%%   <li>`refresh' - Refresh mode: on_change (default) or manual</li>
%% </ul>
%%
%% @param Db Database name or pid
%% @param ViewId View identifier
%% @param Config View configuration
%% @returns `ok' or `{error, Reason}'
-spec register_view(binary() | pid(), binary(), map()) -> ok | {error, term()}.
register_view(Db, ViewId, Config) ->
    with_db(Db, fun(Pid) ->
        barrel_view:register(Pid, ViewId, Config)
    end).

%% @doc Unregister a view.
%%
%% Removes a view and deletes its index data.
%%
%% @param Db Database name or pid
%% @param ViewId View identifier
%% @returns `ok' or `{error, not_found}'
-spec unregister_view(binary() | pid(), binary()) -> ok | {error, term()}.
unregister_view(Db, ViewId) ->
    with_db(Db, fun(Pid) ->
        barrel_view:unregister(Pid, ViewId)
    end).

%% @doc Query a view.
%%
%% Retrieves index entries from a view.
%%
%% == Options ==
%% <ul>
%%   <li>`start_key' - Start key for range query</li>
%%   <li>`end_key' - End key for range query</li>
%%   <li>`limit' - Maximum number of results</li>
%%   <li>`include_docs' - Include full documents in results</li>
%%   <li>`descending' - Reverse order</li>
%% </ul>
%%
%% == Example ==
%% ```
%% {ok, Results} = barrel_docdb:query_view(<<"mydb">>, <<"by_type">>, #{
%%     start_key => <<"user">>,
%%     end_key => <<"user">>,
%%     limit => 100
%% }).
%% '''
%%
%% @param Db Database name or pid
%% @param ViewId View identifier
%% @param Opts Query options
%% @returns `{ok, [Result]}' where each result is a map with key, value, id
-spec query_view(binary() | pid(), binary(), map()) -> {ok, [map()]} | {error, term()}.
query_view(Db, ViewId, Opts) ->
    with_db(Db, fun(Pid) ->
        barrel_view:query(Pid, ViewId, Opts)
    end).

%% @doc List all registered views.
%%
%% Returns metadata about all views in the database.
%%
%% @param Db Database name or pid
%% @returns `{ok, [ViewInfo]}'
-spec list_views(binary() | pid()) -> {ok, [map()]} | {error, term()}.
list_views(Db) ->
    with_db(Db, fun(Pid) ->
        barrel_view:list(Pid)
    end).

%% @doc Refresh a view and wait for it to be up-to-date.
%%
%% Triggers index update and waits for it to complete. Returns the
%% HLC timestamp up to which the view is indexed.
%%
%% @param Db Database name or pid
%% @param ViewId View identifier
%% @returns `{ok, HlcTimestamp}' or `{error, Reason}'
-spec refresh_view(binary() | pid(), binary()) -> {ok, barrel_hlc:timestamp()} | {error, term()}.
refresh_view(Db, ViewId) ->
    with_db(Db, fun(Pid) ->
        barrel_view:refresh(Pid, ViewId)
    end).

%%====================================================================
%% Query
%%====================================================================

%% @doc Find documents matching a query specification.
%%
%% Executes a declarative query against the path index. All document
%% paths are automatically indexed, enabling ad-hoc queries without
%% predefined views.
%%
%% == Query Specification ==
%% <ul>
%%   <li>`where' - List of conditions (required)</li>
%%   <li>`select' - Fields to return (optional, defaults to full doc)</li>
%%   <li>`order_by' - Field or variable to sort by (optional)</li>
%%   <li>`limit' - Maximum results (optional)</li>
%%   <li>`offset' - Skip first N results (optional)</li>
%%   <li>`include_docs' - Include full documents (optional, default true)</li>
%% </ul>
%%
%% == Conditions ==
%% <ul>
%%   <li>`{path, Path, Value}' - Equality match on path</li>
%%   <li>`{compare, Path, Op, Value}' - Comparison (Op: '&gt;', '&lt;', '&gt;=', '=&lt;', '=/=')</li>
%%   <li>`{'and', [Clauses]}' - All conditions must match</li>
%%   <li>`{'or', [Clauses]}' - Any condition must match</li>
%%   <li>`{'not', Clause}' - Negation</li>
%%   <li>`{in, Path, Values}' - Value in list</li>
%%   <li>`{contains, Path, Value}' - Array contains value</li>
%%   <li>`{exists, Path}' - Path exists</li>
%%   <li>`{missing, Path}' - Path does not exist</li>
%%   <li>`{regex, Path, Pattern}' - Regex match</li>
%%   <li>`{prefix, Path, Prefix}' - String prefix match</li>
%% </ul>
%%
%% == Example ==
%% ```
%% %% Find all active users in org1
%% {ok, Results} = barrel_docdb:find(<<"mydb">>, #{
%%     where => [
%%         {path, [<<"type">>], <<"user">>},
%%         {path, [<<"org_id">>], <<"org1">>},
%%         {path, [<<"status">>], <<"active">>}
%%     ],
%%     limit => 100
%% }).
%% '''
%%
%% @param Db Database name or pid
%% @param QuerySpec Query specification map
%% @returns `{ok, [Document]}' or `{error, Reason}'
%% @see find/3
%% @see explain/2
-spec find(binary() | pid(), map()) -> {ok, [map()]} | {error, term()}.
find(Db, QuerySpec) ->
    find(Db, QuerySpec, #{}).

%% @doc Find documents with additional options.
%%
%% Same as `find/2' but allows merging additional options into the query.
%%
%% == Example ==
%% ```
%% {ok, Results} = barrel_docdb:find(<<"mydb">>,
%%     #{where => [{path, [<<"type">>], <<"user">>}]},
%%     #{limit => 10, include_docs => false}
%% ).
%% '''
%%
%% @param Db Database name or pid
%% @param QuerySpec Query specification map
%% @param Opts Additional options to merge
%% @returns `{ok, [Document]}' or `{error, Reason}'
-spec find(binary() | pid(), map(), map()) -> {ok, [map()]} | {error, term()}.
find(Db, QuerySpec, Opts) ->
    with_db(Db, fun(Pid) ->
        {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        %% Default include_docs to true for find API
        DefaultOpts = #{include_docs => true},
        MergedSpec = maps:merge(maps:merge(DefaultOpts, QuerySpec), Opts),
        case barrel_query:compile(MergedSpec) of
            {ok, Plan} ->
                {ok, Results, _LastSeq} = barrel_query:execute(StoreRef, DbName, Plan),
                {ok, Results};
            {error, _} = Error ->
                Error
        end
    end).

%% @doc Explain a query execution plan.
%%
%% Returns information about how a query would be executed without
%% actually running it. Useful for understanding query performance
%% and optimization.
%%
%% == Example ==
%% ```
%% {ok, Explanation} = barrel_docdb:explain(<<"mydb">>, #{
%%     where => [{path, [<<"type">>], <<"user">>}]
%% }),
%% Strategy = maps:get(strategy, Explanation).
%% %% Returns: index_seek | index_scan | multi_index | full_scan
%% '''
%%
%% @param Db Database name or pid (unused, for API consistency)
%% @param QuerySpec Query specification map
%% @returns `{ok, ExplanationMap}' or `{error, Reason}'
-spec explain(binary() | pid(), map()) -> {ok, map()} | {error, term()}.
explain(_Db, QuerySpec) ->
    case barrel_query:compile(QuerySpec) of
        {ok, Plan} ->
            {ok, barrel_query:explain(Plan)};
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Changes
%%====================================================================

%% @doc Get changes since an HLC timestamp.
%%
%% Returns all document changes since the given HLC timestamp. Use `first'
%% to get all changes from the beginning.
%%
%% == Example ==
%% ```
%% %% Get all changes
%% {ok, Changes, LastHlc} = barrel_docdb:get_changes(<<"mydb">>, first),
%%
%% %% Get incremental changes
%% {ok, NewChanges, NewHlc} = barrel_docdb:get_changes(<<"mydb">>, LastHlc).
%% '''
%%
%% @param Db Database name or pid
%% @param Since HLC timestamp or `first'
%% @returns `{ok, [Change], LastHlc}' where each change has id, hlc, rev, changes
%% @see get_changes/3
-spec get_changes(binary() | pid(), barrel_hlc:timestamp() | first) ->
    {ok, [map()], barrel_hlc:timestamp()}.
get_changes(Db, Since) ->
    get_changes(Db, Since, #{}).

%% @doc Get changes with options.
%%
%% == Options ==
%% <ul>
%%   <li>`limit' - Maximum number of changes to return</li>
%%   <li>`include_docs' - Include full documents in results</li>
%%   <li>`descending' - Reverse order</li>
%%   <li>`doc_ids' - Filter to specific document IDs</li>
%% </ul>
%%
%% @param Db Database name or pid
%% @param Since HLC timestamp or `first'
%% @param Opts Query options
%% @returns `{ok, [Change], LastHlc}'
-spec get_changes(binary() | pid(), barrel_hlc:timestamp() | first, map()) ->
    {ok, [map()], barrel_hlc:timestamp()}.
get_changes(Db, Since, Opts) ->
    with_db(Db, fun(Pid) ->
        {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        barrel_changes:get_changes(StoreRef, DbName, Since, Opts)
    end).

%% @doc Subscribe to a changes stream.
%%
%% Returns a stream pid that can be used to iterate over changes
%% as they occur.
%%
%% == Example ==
%% ```
%% {ok, Stream} = barrel_docdb:subscribe_changes(<<"mydb">>, first),
%% %% Use barrel_changes_stream:next/1 to get changes
%% '''
%%
%% @param Db Database name or pid
%% @param Since Starting HLC timestamp
%% @returns `{ok, StreamPid}'
%% @see subscribe_changes/3
-spec subscribe_changes(binary() | pid(), barrel_hlc:timestamp() | first) ->
    {ok, pid()} | {error, term()}.
subscribe_changes(Db, Since) ->
    subscribe_changes(Db, Since, #{}).

%% @doc Subscribe to a changes stream with options.
%%
%% @param Db Database name or pid
%% @param Since Starting HLC timestamp
%% @param Opts Stream options
%% @returns `{ok, StreamPid}'
-spec subscribe_changes(binary() | pid(), barrel_hlc:timestamp() | first, map()) ->
    {ok, pid()} | {error, term()}.
subscribe_changes(Db, Since, Opts) ->
    with_db(Db, fun(Pid) ->
        {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),
        {ok, Info} = barrel_db_server:info(Pid),
        DbName = maps:get(name, Info),
        StreamOpts = Opts#{since => Since},
        barrel_changes_stream:start_link(StoreRef, DbName, StreamOpts)
    end).

%%====================================================================
%% Replication Primitives
%%====================================================================

%% @doc Put a document with explicit revision history.
%%
%% This function is used by replication to store documents with their
%% full revision history. Unlike `put_doc/2', this allows specifying
%% the exact revision chain.
%%
%% == Example ==
%% ```
%% Doc = #{<<"id">> => <<"doc1">>, <<"value">> => <<"replicated">>},
%% History = [<<"2-abc123">>, <<"1-def456">>],
%% {ok, DocId, Rev} = barrel_docdb:put_rev(<<"mydb">>, Doc, History, false).
%% '''
%%
%% @param Db Database name or pid
%% @param Doc Document map (must include `<<"id">>')
%% @param History List of revision IDs, newest first
%% @param Deleted Whether this is a deletion tombstone
%% @returns `{ok, DocId, RevId}'
%% @see barrel_rep
-spec put_rev(binary() | pid(), map(), [binary()], boolean()) ->
    {ok, binary(), binary()} | {error, term()}.
put_rev(Db, Doc, History, Deleted) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:put_rev(Pid, Doc, History, Deleted)
    end).

%% @doc Find missing revisions for replication.
%%
%% Compares a list of revision IDs against those stored locally and
%% returns which revisions are missing. Used by replication to determine
%% what needs to be transferred.
%%
%% == Example ==
%% ```
%% {ok, Missing, Ancestors} = barrel_docdb:revsdiff(<<"mydb">>,
%%     <<"doc1">>,
%%     [<<"3-abc">>, <<"2-def">>, <<"1-ghi">>]
%% ),
%% %% Missing = revisions we don't have
%% %% Ancestors = our revisions that could be ancestors
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Document ID
%% @param RevIds List of revision IDs to check
%% @returns `{ok, MissingRevs, PossibleAncestors}'
%% @see barrel_rep
-spec revsdiff(binary() | pid(), binary(), [binary()]) ->
    {ok, [binary()], [binary()]} | {error, term()}.
revsdiff(Db, DocId, RevIds) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:revsdiff(Pid, DocId, RevIds)
    end).

%%====================================================================
%% Local Documents
%%====================================================================

%% @doc Store a local document.
%%
%% Local documents are stored in the database but are NOT replicated.
%% They are typically used for storing replication checkpoints and
%% other metadata.
%%
%% == Example ==
%% ```
%% ok = barrel_docdb:put_local_doc(<<"mydb">>, <<"_local/checkpoint">>, #{
%%     <<"last_seq">> => <<"100">>
%% }).
%% '''
%%
%% @param Db Database name or pid
%% @param DocId Local document ID
%% @param Doc Document content
%% @returns `ok'
-spec put_local_doc(binary() | pid(), binary(), map()) -> ok | {error, term()}.
put_local_doc(Db, DocId, Doc) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:put_local_doc(Pid, DocId, Doc)
    end).

%% @doc Get a local document.
%%
%% @param Db Database name or pid
%% @param DocId Local document ID
%% @returns `{ok, Document}' or `{error, not_found}'
-spec get_local_doc(binary() | pid(), binary()) -> {ok, map()} | {error, not_found}.
get_local_doc(Db, DocId) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:get_local_doc(Pid, DocId)
    end).

%% @doc Delete a local document.
%%
%% @param Db Database name or pid
%% @param DocId Local document ID
%% @returns `ok' or `{error, not_found}'
-spec delete_local_doc(binary() | pid(), binary()) -> ok | {error, not_found}.
delete_local_doc(Db, DocId) ->
    with_db(Db, fun(Pid) ->
        barrel_db_server:delete_local_doc(Pid, DocId)
    end).

%%====================================================================
%% HLC (Hybrid Logical Clock)
%%====================================================================

%% @doc Get the current global HLC timestamp.
%%
%% Returns the current Hybrid Logical Clock timestamp without advancing
%% the clock. The HLC is node-global and used for ordering events across
%% distributed machines.
%%
%% == Example ==
%% ```
%% TS = barrel_docdb:get_hlc().
%% %% TS is a #timestamp{wall_time, logical} record
%% '''
%%
%% @returns The current HLC timestamp
-spec get_hlc() -> barrel_hlc:timestamp().
get_hlc() ->
    barrel_hlc:get_hlc().

%% @doc Synchronize with a remote HLC timestamp.
%%
%% Call this when receiving data from another node to maintain causality.
%% The local clock is updated to reflect the remote timestamp, ensuring
%% that subsequent events are ordered after the received data.
%%
%% == Example ==
%% ```
%% %% When receiving data from another node:
%% RemoteHlc = ... %% HLC from remote node
%% {ok, NewHlc} = barrel_docdb:sync_hlc(RemoteHlc).
%% '''
%%
%% @param RemoteHlc The HLC timestamp from the remote node
%% @returns `{ok, UpdatedHlc}' or `{error, clock_skew}'
-spec sync_hlc(barrel_hlc:timestamp()) -> {ok, barrel_hlc:timestamp()} | {error, clock_skew}.
sync_hlc(RemoteHlc) ->
    barrel_hlc:sync_hlc(RemoteHlc).

%% @doc Generate a new HLC timestamp.
%%
%% Creates a new timestamp and advances the clock. Use this when creating
%% events that will be sent to other nodes or need to be ordered.
%%
%% == Example ==
%% ```
%% TS = barrel_docdb:new_hlc().
%% %% Use TS for ordering the event
%% '''
%%
%% @returns A new HLC timestamp
-spec new_hlc() -> barrel_hlc:timestamp().
new_hlc() ->
    barrel_hlc:new_hlc().

%%====================================================================
%% Path Subscriptions
%%====================================================================

%% @doc Subscribe to document changes matching a path pattern.
%%
%% Subscribes the calling process to receive notifications for document
%% changes that match the given MQTT-style path pattern. Notifications
%% are sent as messages of the form:
%%
%% `{barrel_change, DbName, #{id => DocId, rev => Rev, hlc => Hlc,
%%                            deleted => boolean(), paths => [binary()]}}'
%%
%% == Pattern Syntax ==
%%
%% Patterns use MQTT-style wildcards:
%% <ul>
%%   <li>`+' matches exactly one path level (e.g., `<<"users/+/profile">>')</li>
%%   <li>`#' matches zero or more levels (e.g., `<<"orders/#">>')</li>
%% </ul>
%%
%% Paths are derived from document field structure:
%% `#{<<"users">> => #{<<"123">> => #{<<"name">> => <<"Alice">>}}}'
%% produces paths like `<<"users/123/name/Alice">>'.
%%
%% == Example ==
%% ```
%% %% Subscribe to all user profile changes
%% {ok, SubRef} = barrel_docdb:subscribe(<<"mydb">>, <<"users/+/profile/#">>),
%%
%% %% Receive notifications
%% receive
%%     {barrel_change, <<"mydb">>, Change} ->
%%         io:format("Document ~s changed~n", [maps:get(id, Change)])
%% end,
%%
%% %% Unsubscribe when done
%% ok = barrel_docdb:unsubscribe(SubRef).
%% '''
%%
%% @param DbName The database name
%% @param Pattern MQTT-style path pattern to match
%% @returns `{ok, SubRef}' on success, `{error, invalid_pattern}' if pattern is invalid
%% @see unsubscribe/1
-spec subscribe(db_name(), binary()) -> {ok, reference()} | {error, term()}.
subscribe(DbName, Pattern) ->
    subscribe(DbName, Pattern, #{}).

%% @doc Subscribe to document changes with options.
%%
%% Same as {@link subscribe/2} but with additional options.
%%
%% == Options ==
%% Currently no options are supported (reserved for future use).
%%
%% @param DbName The database name
%% @param Pattern MQTT-style path pattern to match
%% @param Opts Options map (reserved for future use)
%% @returns `{ok, SubRef}' on success, `{error, invalid_pattern}' if pattern is invalid
%% @see subscribe/2
%% @see unsubscribe/1
-spec subscribe(db_name(), binary(), map()) -> {ok, reference()} | {error, term()}.
subscribe(DbName, Pattern, _Opts) ->
    barrel_sub:subscribe(DbName, Pattern, self()).

%% @doc Unsubscribe from document change notifications.
%%
%% Removes a subscription previously created with {@link subscribe/2} or
%% {@link subscribe/3}. After calling this function, no more notifications
%% will be received for the given subscription.
%%
%% == Example ==
%% ```
%% {ok, SubRef} = barrel_docdb:subscribe(<<"mydb">>, <<"users/#">>),
%% %% ... receive some notifications ...
%% ok = barrel_docdb:unsubscribe(SubRef).
%% '''
%%
%% @param SubRef The subscription reference returned by subscribe/2,3
%% @returns `ok'
%% @see subscribe/2
-spec unsubscribe(reference()) -> ok.
unsubscribe(SubRef) ->
    barrel_sub:unsubscribe(SubRef, self()).

%%====================================================================
%% Query Subscriptions
%%====================================================================

%% @doc Subscribe to document changes matching a query.
%%
%% Subscribes to document change notifications for documents that match
%% the specified query. Only documents that match the query conditions
%% will trigger notifications.
%%
%% Query subscriptions are optimized using path extraction - the full
%% query is only evaluated when a change affects paths referenced by
%% the query.
%%
%% == Example ==
%% ```
%% %% Subscribe to changes for user documents
%% Query = #{where => [{path, [<<"type">>], <<"user">>}]},
%% {ok, SubRef} = barrel_docdb:subscribe_query(<<"mydb">>, Query),
%%
%% %% Receive notifications
%% receive
%%     {barrel_query_change, <<"mydb">>, #{id := DocId, rev := Rev}} ->
%%         io:format("Document ~s changed~n", [DocId])
%% end,
%%
%% %% Unsubscribe when done
%% ok = barrel_docdb:unsubscribe_query(SubRef).
%% '''
%%
%% @param DbName The database name
%% @param Query Query specification (same format as barrel_query)
%% @returns `{ok, SubRef}' on success, `{error, Reason}' on failure
%% @see subscribe_query/3
%% @see unsubscribe_query/1
-spec subscribe_query(db_name(), barrel_query:query_spec()) ->
    {ok, reference()} | {error, term()}.
subscribe_query(DbName, Query) ->
    subscribe_query(DbName, Query, #{}).

%% @doc Subscribe to document changes matching a query with options.
%%
%% Same as {@link subscribe_query/2} but with additional options.
%%
%% @param DbName The database name
%% @param Query Query specification
%% @param Opts Options (reserved for future use)
%% @returns `{ok, SubRef}' on success, `{error, Reason}' on failure
%% @see subscribe_query/2
%% @see unsubscribe_query/1
-spec subscribe_query(db_name(), barrel_query:query_spec(), map()) ->
    {ok, reference()} | {error, term()}.
subscribe_query(DbName, Query, _Opts) ->
    barrel_query_sub:subscribe(DbName, Query, self()).

%% @doc Unsubscribe from query-based change notifications.
%%
%% Removes a query subscription previously created with
%% {@link subscribe_query/2} or {@link subscribe_query/3}.
%%
%% == Example ==
%% ```
%% {ok, SubRef} = barrel_docdb:subscribe_query(<<"mydb">>, Query),
%% %% ... receive some notifications ...
%% ok = barrel_docdb:unsubscribe_query(SubRef).
%% '''
%%
%% @param SubRef The subscription reference returned by subscribe_query/2,3
%% @returns `ok'
%% @see subscribe_query/2
-spec unsubscribe_query(reference()) -> ok.
unsubscribe_query(SubRef) ->
    barrel_query_sub:unsubscribe(SubRef).

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
