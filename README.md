# barrel_docdb

An Erlang document database library with MVCC, views, and replication support.

## Features

- **Document CRUD**: Create, read, update, delete JSON documents
- **MVCC**: Multi-Version Concurrency Control with revision trees
- **Attachments**: Store binary attachments with documents
- **Views**: Secondary indexes with map-reduce style queries
- **Changes Feed**: Real-time change notifications for replication
- **Replication**: Pluggable transport layer for data synchronization
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
{ok, _} = barrel_docdb:create_db(<<"mydb">>, #{}).

%% Save a document
Doc = #{<<"id">> => <<"doc1">>, <<"name">> => <<"Hello">>},
{ok, DocId, RevId} = barrel_docdb:save_doc(<<"mydb">>, Doc).

%% Fetch the document
{ok, Doc2} = barrel_docdb:fetch_doc(<<"mydb">>, <<"doc1">>, #{}).

%% Update the document
Doc3 = Doc2#{<<"name">> => <<"World">>},
{ok, _, NewRevId} = barrel_docdb:save_doc(<<"mydb">>, Doc3).

%% Delete the document
ok = barrel_docdb:delete_doc(<<"mydb">>, <<"doc1">>, NewRevId).

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

- `create_db/2` - Create a new database
- `open_db/1` - Open an existing database
- `close_db/1` - Close a database
- `delete_db/1` - Delete a database
- `db_info/1` - Get database information

### Document Operations

- `save_doc/2,3` - Save a document (create or update)
- `fetch_doc/3` - Fetch a document by ID
- `delete_doc/3` - Delete a document
- `fold_docs/4` - Iterate over documents

### Changes Feed

- `changes_since/4` - Get changes since a sequence
- `subscribe_changes/3` - Subscribe to real-time changes

### Views

- `query_view/4` - Query a view
- `fold_view/5` - Iterate over view results

### Replication

- `start_replication/3` - Start replication between databases
- `stop_replication/1` - Stop a replication

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
