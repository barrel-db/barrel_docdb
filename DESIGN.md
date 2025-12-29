# barrel_docdb Design Document

## Overview

barrel_docdb is an embeddable document database for Erlang applications. It provides document storage with MVCC (Multi-Version Concurrency Control), binary attachments, secondary indexes, a changes feed, and replication primitives.

## Design Goals

1. **Embeddable**: Run as part of your Erlang application, no external services
2. **Reliable**: ACID transactions via RocksDB, crash-safe operations
3. **Replication-ready**: CouchDB-compatible revision model for sync
4. **Efficient**: Optimized storage for documents and large attachments
5. **Simple API**: Clean, intuitive public interface

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application                               │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     barrel_docdb (Public API)                   │
│  create_db, put_doc, get_doc, query_view, replicate, ...       │
└─────────────────────────────────────────────────────────────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│  barrel_db_server │ │   barrel_view    │ │   barrel_rep     │
│  (per-database)   │ │  (per-view)      │ │  (replication)   │
└──────────────────┘ └──────────────────┘ └──────────────────┘
          │                    │                    │
          ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                   barrel_store_rocksdb                          │
│                   (storage abstraction)                         │
└─────────────────────────────────────────────────────────────────┘
          │                                        │
          ▼                                        ▼
┌──────────────────────────┐         ┌──────────────────────────┐
│    Document Store        │         │   Attachment Store       │
│    (RocksDB)             │         │   (RocksDB + BlobDB)     │
└──────────────────────────┘         └──────────────────────────┘
```

## Core Components

### 1. Database Server (barrel_db_server)

Each database runs as a separate `gen_server` process:

```erlang
-record(state, {
    name      :: binary(),           %% Database name
    db_path   :: string(),           %% Data directory path
    store_ref :: rocksdb:db_handle(),%% Document store reference
    att_ref   :: rocksdb:db_handle(),%% Attachment store reference
    view_sup  :: pid()               %% View supervisor
}).
```

**Responsibilities:**
- Document CRUD operations
- Revision management
- Sequence number generation
- Coordination with views and changes

**Process Registry:**
Databases are registered in `persistent_term` for fast lookup:
```erlang
persistent_term:put({barrel_db, DbName}, Pid)
```

### 2. Document Model

Documents use a CouchDB-compatible revision model:

```
┌─────────────────────────────────────────────────────────────────┐
│                         Document                                 │
├─────────────────────────────────────────────────────────────────┤
│ id          : <<"user:alice">>                                  │
│ body        : #{<<"name">> => <<"Alice">>, ...}                 │
├─────────────────────────────────────────────────────────────────┤
│                       Revision Tree                              │
│                                                                  │
│         1-aaa (root)                                            │
│            │                                                     │
│         2-bbb                                                    │
│        ╱     ╲                                                   │
│     3-ccc   3-ddd  (conflict)                                   │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│ Current Rev : 3-ccc (winning)                                   │
│ Deleted     : false                                             │
│ Sequence    : {0, 42}                                           │
└─────────────────────────────────────────────────────────────────┘
```

**Revision Format:** `<generation>-<sha256_hex>`

The hash is computed from:
```erlang
crypto:hash(sha256, term_to_binary({DocBody, ParentRev, Deleted}))
```

### 3. Storage Layer

#### Dual-Database Architecture

Each barrel database uses two separate RocksDB instances:

| Store | Purpose | Optimization |
|-------|---------|--------------|
| Document Store | Docs, metadata, sequences, views | Standard RocksDB, optimized for small values |
| Attachment Store | Binary attachments | BlobDB enabled, optimized for large values |

**Rationale:** Separating large attachments from small documents avoids write amplification during compaction. RocksDB's BlobDB stores large values in separate blob files.

#### Key Schema

```
Document Store Keys:
├── doc_info/{db}/{docid}           → DocInfo (metadata + revtree)
├── doc_rev/{db}/{docid}/{rev}      → Document body
├── doc_seq/{db}/{seq}              → Change entry
├── local/{db}/{docid}              → Local document (not replicated)
├── view_meta/{db}/{viewid}         → View metadata
├── view_seq/{db}/{viewid}          → View indexed sequence
├── view_index/{db}/{viewid}:{key}:{docid} → View index entry
└── view_by_docid/{db}/{viewid}:{docid}    → Reverse index

Attachment Store Keys:
└── att/{db}/{docid}/{attname}      → Attachment binary data
```

Keys are designed for efficient range scans and prefix matching.

### 4. Changes Feed

Every document modification generates a sequence number and change entry:

```
Sequence: {Epoch, Counter}
├── Epoch   : Increments on database recovery/compaction
└── Counter : Monotonically increasing within epoch
```

Changes are stored by sequence for efficient streaming:

```erlang
%% Get changes since sequence 42
barrel_changes:fold_changes(StoreRef, DbName, {0, 42}, Fun, Acc)
```

**Use Cases:**
- Real-time notifications
- Incremental view updates
- Replication source

### 5. Views (Secondary Indexes)

Views are incremental map-reduce indexes:

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Changes    │──────│  View       │──────│  Index      │
│  Feed       │      │  gen_statem │      │  Storage    │
└─────────────┘      └─────────────┘      └─────────────┘
```

**View Lifecycle:**
1. Register view with module implementing `barrel_view` behaviour
2. View process subscribes to changes feed
3. For each change, call `Module:map(Doc)` to get key-value pairs
4. Store/update index entries
5. Track indexed sequence for incremental updates

**Automatic Rebuild:**
When `Module:version()` changes, the view clears and rebuilds from scratch.

### 6. Replication

Replication follows the CouchDB protocol:

```
┌──────────────────────────────────────────────────────────────┐
│                    Replication Flow                           │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Read checkpoint (last_seq from local doc)                │
│                     │                                         │
│                     ▼                                         │
│  2. Get changes from source since last_seq                   │
│                     │                                         │
│                     ▼                                         │
│  3. For each change:                                         │
│     ├── Call revsdiff(target, docid, revs)                   │
│     ├── Get missing revisions from source with history       │
│     └── Put to target using put_rev(doc, history, deleted)   │
│                     │                                         │
│                     ▼                                         │
│  4. Write checkpoint with new last_seq                       │
│                     │                                         │
│                     ▼                                         │
│  5. Repeat until no more changes                             │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

**Transport Abstraction:**
The `barrel_rep_transport` behaviour allows pluggable transports:
- `barrel_rep_transport_local` - Same Erlang VM
- Custom HTTP, TCP, or other transports

**Checkpoints:**
Stored as local documents (not replicated):
```erlang
Key: <<"replication-checkpoint-{rep_id}">>
Value: #{<<"history">> => [#{<<"source_last_seq">> => ...}]}
```

## Design Decisions

### Why RocksDB?

1. **Embedded**: No external service dependencies
2. **LSM-tree**: Optimized for write-heavy workloads
3. **Atomic batches**: Multiple operations in one atomic write
4. **BlobDB**: Efficient large value storage
5. **Snapshots**: Consistent reads during iteration

### Why Revision Trees?

1. **Conflict detection**: Multiple concurrent updates create branches
2. **Replication**: Only transfer missing revisions
3. **History**: Track document evolution
4. **Deterministic winners**: Same data = same winning revision

### Why Separate Attachment Store?

RocksDB stores values inline in SST files. Large values cause:
- High write amplification during compaction
- Wasted space in block cache
- Slower reads due to large block sizes

BlobDB stores large values in separate blob files, solving these issues.

### Why Local Documents?

Local documents:
- Are not replicated
- Don't have revision history
- Are used for per-database metadata (checkpoints, config)

This separates replication state from user data.

## Data Flow Examples

### Put Document

```
put_doc(Db, Doc)
    │
    ├── Validate document
    ├── Generate/validate ID
    ├── Compute new revision hash
    │
    ├── If update:
    │   ├── Check existing doc exists
    │   ├── Verify _rev matches current
    │   └── Extend revision tree
    │
    ├── Get next sequence number
    │
    ├── Atomic batch write:
    │   ├── doc_info (metadata + revtree)
    │   ├── doc_rev (body at new revision)
    │   └── doc_seq (change entry)
    │
    └── Return {ok, #{id, rev, ok}}
```

### Query View

```
query_view(Db, ViewId, Opts)
    │
    ├── Get view process
    ├── Ensure view is up-to-date (refresh if needed)
    │
    ├── Build key range from start_key/end_key
    │
    ├── Iterate view_index entries:
    │   ├── Decode key and value
    │   ├── Apply limit
    │   └── Optionally fetch full document
    │
    └── Return {ok, Results}
```

### Replicate

```
replicate(Source, Target)
    │
    ├── Generate replication ID
    ├── Read checkpoint (get last_seq)
    │
    ├── Loop:
    │   ├── Get changes batch from source
    │   │
    │   ├── For each change:
    │   │   ├── revsdiff(target, docid, revs)
    │   │   ├── If missing revs:
    │   │   │   ├── get_doc(source, docid, {history: true})
    │   │   │   └── put_rev(target, doc, history, deleted)
    │   │
    │   ├── Write checkpoint
    │   └── Continue until no changes
    │
    └── Return {ok, stats}
```

## Supervision Tree

```
barrel_docdb_sup (one_for_one)
├── barrel_db_sup (simple_one_for_one)
│   ├── barrel_db_server (db1)
│   │   └── barrel_view_sup (simple_one_for_one)
│   │       ├── barrel_view (view1)
│   │       └── barrel_view (view2)
│   ├── barrel_db_server (db2)
│   │   └── ...
│   └── ...
└── (future: replication manager)
```

## Performance Considerations

### Write Path
- Batch operations reduce disk I/O
- Sequence numbers enable efficient change tracking
- Revision computation is CPU-bound (SHA-256)

### Read Path
- Document lookup is O(1) key access
- View queries use RocksDB iterators
- Snapshots provide consistent reads

### Memory Usage
- RocksDB block cache for hot data
- Views process changes incrementally
- Large attachments stored in blob files

### Disk Usage
- RocksDB compaction reclaims space
- Old revisions can be pruned
- Attachments use content-addressable storage

## Future Considerations

### Planned Features
- Continuous replication
- HTTP transport for replication
- Reduce functions for views
- Conflict resolution helpers

### Extension Points
- Custom storage backends (via behaviour)
- Custom replication transports
- View behaviours for different index types

## File Structure

```
src/
├── barrel_docdb_app.erl      # Application callbacks
├── barrel_docdb_sup.erl      # Top-level supervisor
├── barrel_docdb.erl          # Public API
│
├── barrel_db_server.erl      # Per-database gen_server
├── barrel_db_sup.erl         # Database supervisor
│
├── barrel_doc.erl            # Document utilities
├── barrel_revtree.erl        # Revision tree operations
│
├── barrel_att.erl            # Attachment API
├── barrel_att_store.erl      # Attachment storage
│
├── barrel_changes.erl        # Changes feed API
├── barrel_changes_stream.erl # Streaming changes
├── barrel_sequence.erl       # Sequence number operations
│
├── barrel_view.erl           # View gen_statem
├── barrel_view_index.erl     # View index storage
├── barrel_view_sup.erl       # View supervisor
│
├── barrel_store_rocksdb.erl  # RocksDB storage
├── barrel_store_keys.erl     # Key encoding
│
├── barrel_rep.erl            # Replication API
├── barrel_rep_alg.erl        # Replication algorithm
├── barrel_rep_checkpoint.erl # Checkpoint management
├── barrel_rep_transport.erl  # Transport behaviour
└── barrel_rep_transport_local.erl  # Local transport
```

## References

- [CouchDB Replication Protocol](https://docs.couchdb.org/en/stable/replication/protocol.html)
- [RocksDB Documentation](https://rocksdb.org/docs/)
- [RocksDB BlobDB](https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html)
