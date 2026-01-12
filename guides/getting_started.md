# Getting Started with barrel_docdb

barrel_docdb is an embeddable document database for Erlang applications. It provides:

- **Document storage** with automatic revision management (MVCC)
- **Binary attachments** stored efficiently with RocksDB BlobDB
- **Secondary indexes** (views) for querying documents by custom keys
- **Changes feed** for tracking all document modifications
- **Replication** for syncing databases

## Installation

Add barrel_docdb to your `rebar.config` dependencies:

```erlang
{deps, [
    {barrel_docdb, "0.1.0"}
]}.
```

## Quick Start

### Starting the Application

```erlang
%% Start the application
application:ensure_all_started(barrel_docdb).
```

### Creating a Database

```erlang
%% Create a database with default settings
{ok, _Pid} = barrel_docdb:create_db(<<"mydb">>).

%% Create with custom data directory
{ok, _Pid} = barrel_docdb:create_db(<<"mydb">>, #{
    data_dir => "/var/lib/barrel"
}).
```

### Working with Documents

Documents are Erlang maps with automatic ID and revision management:

```erlang
%% Create a document (ID auto-generated)
{ok, Result} = barrel_docdb:put_doc(<<"mydb">>, #{
    <<"type">> => <<"user">>,
    <<"name">> => <<"Alice">>,
    <<"email">> => <<"alice@example.com">>
}),
DocId = maps:get(<<"id">>, Result),
Rev = maps:get(<<"rev">>, Result).

%% Create a document with specific ID
{ok, _} = barrel_docdb:put_doc(<<"mydb">>, #{
    <<"id">> => <<"user:alice">>,
    <<"name">> => <<"Alice">>
}).

%% Retrieve a document
{ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"user:alice">>).

%% Update a document (must include _rev)
{ok, _} = barrel_docdb:put_doc(<<"mydb">>, Doc#{
    <<"name">> => <<"Alice Smith">>
}).

%% Delete a document
{ok, _} = barrel_docdb:delete_doc(<<"mydb">>, DocId).
```

### Document IDs and Revisions

- `<<"id">>` - The document identifier (string). Auto-generated if not provided.
- `<<"_rev">>` - The revision identifier. Required for updates to prevent conflicts.
- `<<"_deleted">>` - Set to `true` for deleted documents.

### Iterating Over Documents

```erlang
%% Count all documents
{ok, Count} = barrel_docdb:fold_docs(<<"mydb">>,
    fun(_Doc, Acc) -> {ok, Acc + 1} end,
    0
).

%% Collect all user documents
{ok, Users} = barrel_docdb:fold_docs(<<"mydb">>,
    fun(Doc, Acc) ->
        case maps:get(<<"type">>, Doc, undefined) of
            <<"user">> -> {ok, [Doc | Acc]};
            _ -> {ok, Acc}
        end
    end,
    []
).
```

## Working with Attachments

Attachments let you store binary data (files, images, etc.) associated with documents:

```erlang
%% Store an attachment
Data = <<"Hello, World!">>,
{ok, _} = barrel_docdb:put_attachment(<<"mydb">>, <<"doc1">>,
    <<"greeting.txt">>, Data).

%% Retrieve an attachment
{ok, Binary} = barrel_docdb:get_attachment(<<"mydb">>, <<"doc1">>,
    <<"greeting.txt">>).

%% List attachments for a document
AttNames = barrel_docdb:list_attachments(<<"mydb">>, <<"doc1">>).
%% Returns [<<"greeting.txt">>]

%% Delete an attachment
ok = barrel_docdb:delete_attachment(<<"mydb">>, <<"doc1">>,
    <<"greeting.txt">>).
```

## Creating Views (Secondary Indexes)

Views allow you to query documents by custom keys.

### Define a View Module

```erlang
-module(by_type_view).
-behaviour(barrel_view).
-export([version/0, map/1]).

%% Increment version when changing the map function
version() -> 1.

%% Emit {Key, Value} pairs for each document
map(#{<<"type">> := Type, <<"name">> := Name}) ->
    [{Type, Name}];
map(_) ->
    [].
```

### Register and Query the View

```erlang
%% Register the view
ok = barrel_docdb:register_view(<<"mydb">>, <<"by_type">>, #{
    module => by_type_view
}).

%% Wait for the view to be up-to-date
{ok, _Seq} = barrel_docdb:refresh_view(<<"mydb">>, <<"by_type">>).

%% Query the view
{ok, Results} = barrel_docdb:query_view(<<"mydb">>, <<"by_type">>, #{
    start_key => <<"user">>,
    end_key => <<"user">>
}).

%% Each result has: key, value, id
lists:foreach(fun(#{key := Key, value := Value, id := DocId}) ->
    io:format("~s: ~s (doc: ~s)~n", [Key, Value, DocId])
end, Results).
```

## Tracking Changes

The changes feed tracks all document modifications using HLC timestamps:

```erlang
%% Get all changes since the beginning
{ok, Changes, LastHlc} = barrel_docdb:get_changes(<<"mydb">>, first).

%% Get changes with options
{ok, Changes2, _} = barrel_docdb:get_changes(<<"mydb">>, LastHlc, #{
    limit => 100,
    include_docs => true
}).

%% Process each change
lists:foreach(fun(Change) ->
    DocId = maps:get(id, Change),
    Hlc = maps:get(hlc, Change),
    IsDeleted = maps:get(deleted, Change, false),
    io:format("Change at ~p: ~s (~s)~n",
        [Hlc, DocId, if IsDeleted -> "deleted"; true -> "updated" end])
end, Changes).
```

See the [Changes & Subscriptions Guide](changes.md) for more details on real-time notifications.

## Database Management

```erlang
%% List all open databases
DbNames = barrel_docdb:list_dbs().

%% Get database info
{ok, Info} = barrel_docdb:db_info(<<"mydb">>).

%% Close a database (can be reopened with create_db)
ok = barrel_docdb:close_db(<<"mydb">>).

%% Delete a database and all its data
ok = barrel_docdb:delete_db(<<"mydb">>).
```

## Next Steps

- [Query Guide](queries.md) - Declarative queries for finding documents
- [Changes & Subscriptions](changes.md) - Real-time change notifications
- [Replication Guide](replication.md) - Sync databases with filtering
- [Erlang API Reference](api/erlang.md) - Complete API documentation
