# barrel_docdb

An embeddable document database for Erlang with MVCC, declarative queries, real-time subscriptions, and replication.

## Features

- **Document CRUD**: Create, read, update, delete JSON documents
- **MVCC**: Multi-Version Concurrency Control with revision trees
- **Declarative Queries**: Ad-hoc queries with automatic path indexing
- **Real-time Subscriptions**: MQTT-style path patterns and query subscriptions
- **HLC Ordering**: Hybrid Logical Clocks for distributed event ordering
- **Views**: Secondary indexes with map-reduce style queries
- **Changes Feed**: HLC-ordered change notifications
- **Filtered Replication**: Sync by path patterns or queries
- **Attachments**: Efficient binary storage with RocksDB BlobDB
- **RocksDB Backend**: High-performance persistent storage

## Requirements

- Erlang/OTP 27+
- RocksDB (via rocksdb hex package 2.0.0)

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {barrel_docdb, "0.1.0"}
]}.
```

## Quick Start

```erlang
%% Start the application
application:ensure_all_started(barrel_docdb).

%% Create a database
{ok, _} = barrel_docdb:create_db(<<"mydb">>).

%% Save a document
{ok, #{<<"id">> := DocId, <<"rev">> := Rev}} = barrel_docdb:put_doc(<<"mydb">>, #{
    <<"type">> => <<"user">>,
    <<"name">> => <<"Alice">>
}).

%% Fetch the document
{ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, DocId).

%% Update the document (include _rev for optimistic concurrency)
{ok, _} = barrel_docdb:put_doc(<<"mydb">>, Doc#{<<"name">> => <<"Bob">>}).

%% Query documents (returns results with continuation metadata)
{ok, Users, _Meta} = barrel_docdb:find(<<"mydb">>, #{
    where => [{path, [<<"type">>], <<"user">>}]
}).

%% Subscribe to changes
{ok, SubRef} = barrel_docdb:subscribe(<<"mydb">>, <<"type/user/#">>),
receive {barrel_change, _, Change} -> io:format("Change: ~p~n", [Change]) end.

%% Delete the document
{ok, _} = barrel_docdb:delete_doc(<<"mydb">>, DocId).

%% Delete the database
ok = barrel_docdb:delete_db(<<"mydb">>).
```

## Configuration

In your `sys.config`:

```erlang
{barrel_docdb, [
    {data_dir, "data/barrel_docdb"},
    {default_store, barrel_store_rocksdb}
]}.
```

## API Overview

### Database Operations

- `create_db/1,2` - Create a new database
- `open_db/1` - Open an existing database
- `close_db/1` - Close a database
- `delete_db/1` - Delete a database
- `db_info/1` - Get database information

### Document Operations

- `put_doc/2,3` - Create or update a document
- `get_doc/2,3` - Fetch a document by ID
- `delete_doc/2,3` - Delete a document
- `fold_docs/3` - Iterate over documents

### Queries

- `find/2,3` - Query documents with declarative syntax
- `explain/2` - Explain query execution plan

### Subscriptions

- `subscribe/2,3` - Subscribe to path pattern changes
- `unsubscribe/1` - Unsubscribe
- `subscribe_query/2,3` - Subscribe to query-matching changes
- `unsubscribe_query/1` - Unsubscribe

### Changes Feed

- `get_changes/2,3` - Get changes since HLC timestamp
- `subscribe_changes/2,3` - Subscribe to changes stream

### HLC (Distributed Ordering)

- `get_hlc/0` - Get current HLC timestamp
- `new_hlc/0` - Generate new HLC timestamp
- `sync_hlc/1` - Synchronize with remote HLC

### Views

- `register_view/3` - Register a secondary index
- `query_view/3` - Query a view
- `refresh_view/2` - Refresh and wait for view

### Replication

- `barrel_rep:replicate/2,3` - Replicate between databases

## Architecture

barrel_docdb is designed as a single, embeddable OTP application:

```
barrel_docdb_sup
├── barrel_db_sup (simple_one_for_one)
│   └── barrel_db_server (per database)
└── (other services)
```

### Storage Abstraction

The storage layer uses a behaviour pattern (`barrel_store`) allowing different backends:

- `barrel_store_rocksdb` - Default RocksDB backend

### Replication Transport

Replication uses a pluggable transport behaviour (`barrel_rep_transport`):

- `barrel_rep_transport_local` - Same Erlang VM
- Custom transports can be implemented for HTTP, TCP, etc.

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
