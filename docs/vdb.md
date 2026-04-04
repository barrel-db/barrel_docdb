# Virtual Databases (VDB)

Virtual Databases (VDBs) provide automatic sharding for horizontal scalability. Documents are distributed across multiple physical databases (shards) based on consistent hashing of document IDs.

VDB is an **optional layer** - the existing `/db/:db` API remains unchanged. Use VDBs when you need to scale beyond a single database instance.

## Architecture

```
                         ┌─────────────────────────────────────┐
                         │           HTTP API                   │
                         ├──────────────────┬──────────────────┤
                         │  /db/:db         │  /vdb/:vdb       │
                         │  (direct)        │  (sharded)       │
                         └────────┬─────────┴────────┬─────────┘
                                  │                  │
                                  │         ┌───────▼────────┐
                                  │         │  barrel_vdb    │
                                  │         │  (router)      │
                                  │         └───────┬────────┘
                                  │                 │
                         ┌────────▼─────────────────▼─────────┐
                         │         barrel_docdb (unchanged)    │
                         │  ┌─────────┐ ┌─────────┐ ┌────────┐│
                         │  │users_s0 │ │users_s1 │ │ mydb   ││
                         │  └─────────┘ └─────────┘ └────────┘│
                         └─────────────────────────────────────┘
```

**Key principles:**

- VDB is optional - existing `/db/:db` API unchanged
- No changes to barrel_docdb core modules
- Physical shard DBs are regular barrel databases
- Documents routed by consistent hash of document ID

---

## Quick Start

### Create a VDB

=== "HTTP API"

    ```bash
    # Create a VDB with 4 shards
    curl -X POST "http://localhost:8080/vdb" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $API_KEY" \
      -d '{"name": "users", "shard_count": 4}'
    ```

=== "Erlang API"

    ```erlang
    %% Create a VDB with 4 shards
    ok = barrel_vdb:create(<<"users">>, #{shard_count => 4}).
    ```

### Store and Retrieve Documents

Documents are automatically routed to the correct shard based on their ID:

=== "HTTP API"

    ```bash
    # Put a document (routed to correct shard)
    curl -X PUT "http://localhost:8080/vdb/users/user123" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $API_KEY" \
      -d '{"name": "Alice", "email": "alice@example.com"}'

    # Get a document (routed to correct shard)
    curl "http://localhost:8080/vdb/users/user123" \
      -H "Authorization: Bearer $API_KEY"

    # Delete a document
    curl -X DELETE "http://localhost:8080/vdb/users/user123" \
      -H "Authorization: Bearer $API_KEY"
    ```

=== "Erlang API"

    ```erlang
    %% Put a document
    {ok, #{<<"id">> := DocId, <<"rev">> := Rev}} = barrel_vdb:put_doc(<<"users">>, #{
        <<"id">> => <<"user123">>,
        <<"name">> => <<"Alice">>,
        <<"email">> => <<"alice@example.com">>
    }).

    %% Get a document
    {ok, Doc} = barrel_vdb:get_doc(<<"users">>, <<"user123">>).

    %% Delete a document
    {ok, _} = barrel_vdb:delete_doc(<<"users">>, <<"user123">>).
    ```

### Query Across Shards

Queries use scatter-gather to search all shards and merge results:

=== "HTTP API"

    ```bash
    # Find all active users
    curl -X POST "http://localhost:8080/vdb/users/_find" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $API_KEY" \
      -d '{
        "where": [{"path": ["active"], "value": true}],
        "limit": 100
      }'
    ```

=== "Erlang API"

    ```erlang
    %% Find all active users
    {ok, Results, Meta} = barrel_vdb:find(<<"users">>, #{
        where => [{path, [<<"active">>], true}],
        limit => 100
    }).
    ```

---

## HTTP API Reference

### VDB Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/vdb` | Create a new VDB |
| `GET` | `/vdb` | List all VDBs |
| `GET` | `/vdb/:vdb` | Get VDB info |
| `DELETE` | `/vdb/:vdb` | Delete a VDB |
| `GET` | `/vdb/:vdb/_shards` | Get shard assignments |
| `GET` | `/vdb/:vdb/_replication` | Get replication status |
| `POST` | `/vdb/:vdb/_import` | Import from regular database |
| `POST` | `/vdb/:vdb/_shards/:shard/_split` | Split a shard |
| `POST` | `/vdb/:vdb/_shards/:shard/_merge` | Merge two shards |

### Document Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/vdb/:vdb/:doc_id` | Get document (routed) |
| `PUT` | `/vdb/:vdb/:doc_id` | Put document (routed) |
| `DELETE` | `/vdb/:vdb/:doc_id` | Delete document (routed) |
| `POST` | `/vdb/:vdb/_bulk_docs` | Bulk operations |

### Query Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/vdb/:vdb/_find` | Query across all shards |
| `GET` | `/vdb/:vdb/_changes` | Merged changes feed |

---

## Creating a VDB

### Basic Creation

```bash
curl -X POST "http://localhost:8080/vdb" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"name": "orders", "shard_count": 8}'
```

### With Replication

Create a VDB with automatic replication across zones:

```bash
curl -X POST "http://localhost:8080/vdb" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "users",
    "shard_count": 4,
    "placement": {
      "replica_factor": 2,
      "zones": ["us-east", "eu-west"]
    }
  }'
```

### Creation Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | string | (required) | VDB name |
| `shard_count` | integer | 4 | Number of shards |
| `hash_function` | string | "phash2" | Hash function (only "phash2" supported) |
| `placement.replica_factor` | integer | 1 | Number of replicas per shard |
| `placement.zones` | array | [] | Preferred zones for placement |

---

## VDB Info

Get detailed information about a VDB:

```bash
curl "http://localhost:8080/vdb/users" \
  -H "Authorization: Bearer $API_KEY"
```

Response:

```json
{
  "name": "users",
  "shard_count": 4,
  "total_docs": 10000,
  "total_disk_size": 52428800,
  "shards": [
    {"db": "users_s0", "doc_count": 2500, "disk_size": 13107200},
    {"db": "users_s1", "doc_count": 2500, "disk_size": 13107200},
    {"db": "users_s2", "doc_count": 2500, "disk_size": 13107200},
    {"db": "users_s3", "doc_count": 2500, "disk_size": 13107200}
  ]
}
```

---

## Shard Assignments

View shard assignments and status:

```bash
curl "http://localhost:8080/vdb/users/_shards" \
  -H "Authorization: Bearer $API_KEY"
```

Response:

```json
{
  "shards": [
    {
      "shard_id": 0,
      "primary": "node1@localhost",
      "replicas": ["node2@localhost"],
      "status": "active"
    },
    {
      "shard_id": 1,
      "primary": "node2@localhost",
      "replicas": ["node1@localhost"],
      "status": "active"
    }
  ]
}
```

### Shard Statuses

| Status | Description |
|--------|-------------|
| `active` | Normal operation |
| `splitting` | Shard is being split |
| `merging` | Shard is being merged |

---

## Bulk Operations

Write multiple documents in a single request:

```bash
curl -X POST "http://localhost:8080/vdb/users/_bulk_docs" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "docs": [
      {"id": "user1", "name": "Alice"},
      {"id": "user2", "name": "Bob"},
      {"id": "user3", "name": "Charlie"}
    ]
  }'
```

Response:

```json
{
  "results": [
    {"id": "user1", "ok": true, "rev": "1-abc123"},
    {"id": "user2", "ok": true, "rev": "1-def456"},
    {"id": "user3", "ok": true, "rev": "1-ghi789"}
  ]
}
```

---

## Changes Feed

Get merged changes from all shards:

```bash
# Get changes since beginning
curl "http://localhost:8080/vdb/users/_changes?since=first" \
  -H "Authorization: Bearer $API_KEY"

# Get changes with limit
curl "http://localhost:8080/vdb/users/_changes?since=first&limit=100" \
  -H "Authorization: Bearer $API_KEY"
```

Response:

```json
{
  "changes": [
    {"id": "user1", "seq": 1, "deleted": false},
    {"id": "user2", "seq": 2, "deleted": false}
  ],
  "last_seq": 2
}
```

**Note:** Changes are ordered using Hybrid Logical Clocks (HLC) to ensure causal consistency across shards. The `last_seq` value is an HLC timestamp that can be used to resume the changes feed.

---

## Data Import

Import documents from a regular database into a VDB. Documents are automatically distributed across shards based on their IDs.

### Import All Documents

=== "HTTP API"

    ```bash
    curl -X POST "http://localhost:8080/vdb/users/_import" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $API_KEY" \
      -d '{"source_db": "legacy_users"}'
    ```

=== "Erlang API"

    ```erlang
    {ok, Stats} = barrel_vdb_import:import(<<"legacy_users">>, <<"users">>, #{}).
    ```

Response:

```json
{
  "docs_read": 10000,
  "docs_written": 9950,
  "docs_skipped": 50,
  "errors": 0,
  "started_at": 1736712345000,
  "finished_at": 1736712400000,
  "status": "completed"
}
```

### Import with Filter

Import only specific documents:

=== "HTTP API"

    ```bash
    curl -X POST "http://localhost:8080/vdb/users/_import" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $API_KEY" \
      -d '{
        "source_db": "legacy_data",
        "filter": {
          "where": [{"path": ["type"], "value": "user"}]
        },
        "batch_size": 500,
        "on_conflict": "skip"
      }'
    ```

=== "Erlang API"

    ```erlang
    {ok, Stats} = barrel_vdb_import:import(<<"legacy_data">>, <<"users">>, #{
        filter => #{where => [{path, [<<"type">>], <<"user">>}]},
        batch_size => 500,
        on_conflict => skip
    }).
    ```

### Import Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `source_db` | string | (required) | Source database name |
| `filter` | object | - | Filter documents (same format as `_find`) |
| `batch_size` | integer | 100 | Documents per batch |
| `on_conflict` | string | `skip` | Conflict handling: `skip`, `overwrite`, `merge` |

---

## Replication Status

Check replication status for a VDB:

```bash
curl "http://localhost:8080/vdb/users/_replication" \
  -H "Authorization: Bearer $API_KEY"
```

Response:

```json
{
  "vdb_name": "users",
  "replica_factor": 2,
  "shard_count": 4,
  "shards": [
    {
      "shard_id": 0,
      "replication_tasks": [
        {"source": "users_s0", "target": "http://node2:8080/db/users_s0", "status": "active"}
      ]
    }
  ]
}
```

---

## Erlang API Reference

### VDB Lifecycle

```erlang
%% Create a VDB
ok = barrel_vdb:create(VdbName, Opts).
%% Opts: #{shard_count => 4, placement => #{replica_factor => 2}}

%% Check if VDB exists
true = barrel_vdb:exists(VdbName).

%% List all VDBs
{ok, [<<"users">>, <<"orders">>]} = barrel_vdb:list().

%% Get VDB info
{ok, Info} = barrel_vdb:info(VdbName).

%% Delete a VDB
ok = barrel_vdb:delete(VdbName).
```

### Document Operations

```erlang
%% Put document (ID auto-generated if not provided)
{ok, #{<<"id">> := DocId, <<"rev">> := Rev}} = barrel_vdb:put_doc(VdbName, Doc).

%% Put with specific revision (for updates)
{ok, Result} = barrel_vdb:put_doc(VdbName, DocId, Doc#{<<"_rev">> => Rev}).

%% Get document
{ok, Doc} = barrel_vdb:get_doc(VdbName, DocId).

%% Get with options
{ok, Doc} = barrel_vdb:get_doc(VdbName, DocId, #{include_revs => true}).

%% Delete document
{ok, Result} = barrel_vdb:delete_doc(VdbName, DocId).
```

### Query Operations

```erlang
%% Find documents (scatter-gather across all shards)
{ok, Results, Meta} = barrel_vdb:find(VdbName, #{
    where => [{path, [<<"type">>], <<"user">>}],
    limit => 100
}).

%% Get changes (merged from all shards)
{ok, Changes} = barrel_vdb:get_changes(VdbName, first).
{ok, Changes} = barrel_vdb:get_changes(VdbName, Since, #{limit => 100}).

%% Fold over all documents
{ok, Result} = barrel_vdb:fold_docs(VdbName, Fun, Acc, Opts).
```

### Bulk Operations

```erlang
%% Bulk write documents
Docs = [
    #{<<"id">> => <<"doc1">>, <<"value">> => 1},
    #{<<"id">> => <<"doc2">>, <<"value">> => 2}
],
{ok, Results} = barrel_vdb:bulk_docs(VdbName, Docs).
```

### Cluster-Wide Operations

```erlang
%% List all VDBs known across the cluster (local + discovered from peers)
{ok, VDBs} = barrel_vdb_sync:list_cluster_vdbs().
%% VDBs = [<<"users">>, <<"orders">>, <<"products">>]

%% Get list of peer URLs that have a specific VDB
{ok, Nodes} = barrel_vdb_sync:get_vdb_nodes(<<"users">>).
%% Nodes = [<<"http://node1:8080">>, <<"http://node2:8080">>]
```

---

## Shard Rebalancing

When shards become unbalanced, you can split or merge them.

### Split a Shard

Split a large shard into two. The original shard keeps the lower half of its hash range, and a new shard is created for the upper half.

=== "HTTP API"

    ```bash
    # Split shard 0 into two shards
    curl -X POST "http://localhost:8080/vdb/users/_shards/0/_split" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $API_KEY"

    # With batch size option
    curl -X POST "http://localhost:8080/vdb/users/_shards/0/_split" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $API_KEY" \
      -d '{"batch_size": 500}'
    ```

    Response:
    ```json
    {
      "ok": true,
      "new_shard_id": 4,
      "message": "Shard 0 split into shards 0 and 4"
    }
    ```

=== "Erlang API"

    ```erlang
    %% Split shard 0 into two shards
    {ok, NewShardId} = barrel_shard_rebalance:split_shard(<<"users">>, 0).

    %% Split with progress callback
    ProgressFun = fun(#{phase := Phase, migrated := N, total := T}) ->
        io:format("~p: ~p/~p~n", [Phase, N, T])
    end,
    {ok, NewShardId} = barrel_shard_rebalance:split_shard(<<"users">>, 0, #{
        progress_callback => ProgressFun,
        batch_size => 100
    }).
    ```

### Merge Shards

Merge two adjacent shards. Only shards that are adjacent in hash space can be merged.

=== "HTTP API"

    ```bash
    # Merge shard 2 into shard 3
    curl -X POST "http://localhost:8080/vdb/users/_shards/2/_merge" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $API_KEY" \
      -d '{"target_shard": 3}'

    # With batch size option
    curl -X POST "http://localhost:8080/vdb/users/_shards/2/_merge" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $API_KEY" \
      -d '{"target_shard": 3, "batch_size": 500}'
    ```

    Response:
    ```json
    {
      "ok": true,
      "message": "Shard 2 merged into shard 3"
    }
    ```

    Error (shards not adjacent):
    ```json
    {
      "error": "Shards are not adjacent in hash space"
    }
    ```

=== "Erlang API"

    ```erlang
    %% Check if shards can be merged
    {ok, true} = barrel_shard_rebalance:can_merge(<<"users">>, 0, 1).

    %% Merge shards 0 and 1
    ok = barrel_shard_rebalance:merge_shards(<<"users">>, 0, 1).
    ```

### Estimate Migration

Before splitting/merging, estimate the work involved:

```erlang
%% Estimate documents to migrate
{ok, DocCount} = barrel_shard_rebalance:estimate_migration(<<"users">>, 0, 1).
```

---

## Best Practices

### Shard Count Selection

- **Start small**: Begin with 4-8 shards
- **Plan for growth**: Choose a power of 2 (4, 8, 16, 32)
- **Consider document count**: ~1M docs per shard is reasonable
- **Monitor disk usage**: Split shards when they exceed 10GB

### Document ID Design

Since documents are routed by ID hash:

- **Use UUIDs or random IDs** for even distribution
- **Avoid sequential IDs** (doc1, doc2, doc3) which may cluster
- **Consider composite IDs** (user:123:order:456) for related documents

### Query Optimization

- **Use specific selectors** to reduce scatter-gather overhead
- **Add limits** to prevent large result sets
- **Consider denormalization** to avoid cross-shard joins

---

## Troubleshooting

### Uneven Shard Distribution

Check document counts per shard:

```bash
curl "http://localhost:8080/vdb/users" -H "Authorization: Bearer $API_KEY" | jq '.shards'
```

If distribution is uneven:
1. Check document ID patterns
2. Consider splitting large shards
3. Ensure hash function is consistent

### Replication Lag

Check replication status:

```bash
curl "http://localhost:8080/vdb/users/_replication" -H "Authorization: Bearer $API_KEY"
```

If tasks show "error" status:
1. Verify network connectivity between nodes
2. Check authentication configuration
3. Review node logs for errors

### Query Performance

For slow queries:

1. Check if query can be satisfied by fewer shards
2. Add appropriate indexes on shard databases
3. Consider query result caching at application level
