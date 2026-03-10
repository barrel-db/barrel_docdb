<div align="center">

# barrel_docdb

**Embeddable document database for Erlang with MVCC, declarative queries, and P2P replication**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Alpha-orange.svg)]()

[Documentation](https://docs.barrel-db.eu/docdb) |
[Examples](./examples) |
[barrel-db.eu](https://barrel-db.eu)

</div>

---

> **Alpha Software** - API may change. Feedback welcome via [GitHub Issues](https://github.com/barrel-db/barrel_docdb/issues).

## Overview

barrel_docdb provides:

- **Document CRUD** with MVCC revision trees and conflict resolution
- **Declarative Queries** with automatic path indexing
- **Real-time Subscriptions** via MQTT-style path patterns and queries
- **HTTP API** with REST endpoints for all operations
- **Peer-to-Peer Replication** with configurable patterns (chain, group, fanout)
- **Federated Queries** across multiple databases
- **Tiered Storage** with automatic TTL/capacity-based migration
- **Prometheus Metrics** for monitoring and alerting
- **HLC Ordering** for distributed event coordination

### Use Cases

- **Edge Computing**: Deploy nodes that sync to cloud when connected
- **Multi-Region**: Replicate data across regions with conflict resolution
- **Tiered Caching**: Hot/warm/cold data tiers with automatic migration
- **Event Distribution**: Fan-out patterns for event streaming
- **Offline-First Apps**: Full MVCC for seamless sync

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

%% Query documents
{ok, Users, _Meta} = barrel_docdb:find(<<"mydb">>, #{
    where => [{path, [<<"type">>], <<"user">>}]
}).

%% Subscribe to changes (MQTT-style patterns)
{ok, SubRef} = barrel_docdb:subscribe(<<"mydb">>, <<"type/user/#">>),
receive {barrel_change, _, Change} -> io:format("~p~n", [Change]) end.
```

## HTTP API

Start the HTTP server (port 8080 by default):

```erlang
{ok, _} = barrel_http_server:start([]).
```

### Basic Operations

```bash
# Health check
curl http://localhost:8080/health

# Create database
curl -X PUT http://localhost:8080/db/mydb

# Create document with auto-generated ID
curl -X POST http://localhost:8080/db/mydb \
  -H "Content-Type: application/json" \
  -d '{"type": "user", "name": "Alice"}'

# Or create document with specific ID
curl -X PUT http://localhost:8080/db/mydb/doc1 \
  -H "Content-Type: application/json" \
  -d '{"type": "user", "name": "Alice"}'

# Get document
curl http://localhost:8080/db/mydb/doc1

# Query documents
curl -X POST http://localhost:8080/db/mydb/_find \
  -H "Content-Type: application/json" \
  -d '{"where": [{"path": ["type"], "value": "user"}]}'
```

### Changes Feed

```bash
# Poll for changes
curl "http://localhost:8080/db/mydb/_changes?since=first"

# Long-poll (wait for changes)
curl "http://localhost:8080/db/mydb/_changes?feed=longpoll&timeout=30000"

# Server-Sent Events stream
curl http://localhost:8080/db/mydb/_changes/stream
```

### Attachments

```bash
# Put attachment
curl -X PUT http://localhost:8080/db/mydb/doc1/_attachments/photo.jpg \
  -H "Content-Type: image/jpeg" \
  --data-binary @photo.jpg

# Get attachment
curl http://localhost:8080/db/mydb/doc1/_attachments/photo.jpg > photo.jpg
```

## Replication

### Basic Replication

```erlang
%% One-shot replication
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>).

%% With filter (path patterns)
{ok, _} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{paths => [<<"users/#">>]}
}).

%% With query filter
{ok, _} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{query => #{where => [{path, [<<"status">>], <<"active">>}]}}
}).
```

### HTTP Replication

```erlang
%% Replicate to remote node
{ok, _} = barrel_rep:replicate(<<"mydb">>, <<"http://remote:8080/db/mydb">>, #{
    source_transport => barrel_rep_transport_local,
    target_transport => barrel_rep_transport_http
}).
```

### Replication Policies

High-level patterns for common topologies:

```erlang
%% Chain replication: A -> B -> C
barrel_rep_policy:create(<<"my_chain">>, #{
    pattern => chain,
    nodes => [<<"http://nodeA:8080">>, <<"http://nodeB:8080">>, <<"http://nodeC:8080">>],
    database => <<"mydb">>,
    mode => continuous
}).
barrel_rep_policy:enable(<<"my_chain">>).

%% Group (multi-master): A <-> B <-> C
barrel_rep_policy:create(<<"region_sync">>, #{
    pattern => group,
    members => [<<"db1">>, <<"http://nodeB:8080/db1">>, <<"http://nodeC:8080/db1">>],
    mode => continuous
}).

%% Fanout: A -> B, C, D
barrel_rep_policy:create(<<"events">>, #{
    pattern => fanout,
    source => <<"events">>,
    targets => [<<"replica1">>, <<"replica2">>, <<"http://remote:8080/events">>]
}).

%% Check status
{ok, Status} = barrel_rep_policy:status(<<"my_chain">>).
%% #{name => <<"my_chain">>, pattern => chain, enabled => true, task_count => 2}
```

### HTTP API for Policies

```bash
# List all policies
curl http://localhost:8080/_policies

# Create a policy
curl -X POST http://localhost:8080/_policies \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_fanout",
    "pattern": "fanout",
    "source": "events",
    "targets": ["replica1", "replica2"],
    "mode": "continuous"
  }'

# Get a policy
curl http://localhost:8080/_policies/my_fanout

# Enable a policy
curl -X POST http://localhost:8080/_policies/my_fanout/_enable

# Disable a policy
curl -X POST http://localhost:8080/_policies/my_fanout/_disable

# Get policy status
curl http://localhost:8080/_policies/my_fanout/_status

# Delete a policy
curl -X DELETE http://localhost:8080/_policies/my_fanout
```

### Sync Writes (Chain Confirmation)

```erlang
%% Write to A, wait until C confirms
{ok, _} = barrel_docdb:put_doc(<<"mydb">>, Doc, #{
    replicate => sync,
    wait_for => [<<"http://nodeC:8080/db/mydb">>]
}).
```

## Federation (Cross-Database Queries)

Query across multiple databases and merge results:

```erlang
%% Create a federation with a stored query
barrel_federation:create(<<"all_users">>, [
    <<"local_db">>,
    <<"http://nodeB:8080/users">>,
    <<"http://nodeC:8080/users">>
], #{
    query => #{where => [{path, [<<"active">>], true}]}
}).

%% Query using stored query
{ok, Results, Meta} = barrel_federation:find(<<"all_users">>).

%% Query with additional filters (merged with stored query)
{ok, Results, Meta} = barrel_federation:find(<<"all_users">>, #{
    where => [{path, [<<"role">>], <<"admin">>}]
}).
```

### HTTP Federation

```bash
# Create federation
curl -X POST http://localhost:8080/_federation \
  -H "Content-Type: application/json" \
  -d '{"name": "all_users", "members": ["users", "http://nodeB:8080/users"]}'

# Query federation
curl -X POST http://localhost:8080/_federation/all_users/_find \
  -H "Content-Type: application/json" \
  -d '{"where": [{"path": ["role"], "value": "admin"}]}'
```

## Tiered Storage

Automatic data migration between hot/warm/cold tiers:

```erlang
%% Configure tiered storage
barrel_tier:configure(<<"cache">>, #{
    warm_db => <<"main">>,
    cold_db => <<"archive">>,
    ttl => 3600,           %% Move to warm after 1 hour
    capacity => 10000000   %% Or when size exceeds 10MB
}).

%% Query spans all tiers transparently
{ok, Results, Meta} = barrel_tier:find(<<"cache">>, #{
    where => [{path, [<<"type">>], <<"event">>}]
}).

%% Manual migration
barrel_tier:migrate_expired(<<"cache">>, #{force => true}).
```

## Peer Discovery

Discover nodes via DNS SRV records or manual configuration:

```erlang
%% Add a peer manually
barrel_discovery:add_peer(<<"http://nodeB:8080">>).

%% Add DNS domain for SRV discovery
barrel_discovery:add_dns_domain(<<"barrel.example.com">>).

%% List discovered peers
{ok, Peers} = barrel_discovery:list_peers().
%% [#{id => <<"nodeB">>, url => <<"http://nodeB:8080">>, status => active}]
```

## Conflict Resolution

barrel_docdb uses revision trees (CRDT-style) for conflict handling:

```erlang
%% Detect conflicts
case barrel_docdb:get_conflicts(<<"mydb">>, DocId) of
    {ok, []} ->
        %% No conflicts
        ok;
    {ok, Conflicts} ->
        %% Resolve by choosing a winner
        barrel_docdb:resolve_conflict(<<"mydb">>, DocId, WinningRev, choose)
end.
```

## Prometheus Metrics

Metrics are exposed at `/metrics` in Prometheus text format:

```bash
curl http://localhost:8080/metrics
```

Available metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `barrel_doc_operations_total` | Counter | Document operations by db/operation |
| `barrel_doc_operation_duration_seconds` | Histogram | Operation latency |
| `barrel_query_operations_total` | Counter | Query operations by db |
| `barrel_query_duration_seconds` | Histogram | Query latency |
| `barrel_replication_docs_total` | Counter | Documents replicated |
| `barrel_replication_errors_total` | Counter | Replication errors |
| `barrel_federation_queries_total` | Counter | Federation queries |
| `barrel_http_requests_total` | Counter | HTTP requests by method/path/status |
| `barrel_peers_active` | Gauge | Active peer connections |

## Configuration

In your `sys.config`:

```erlang
{barrel_docdb, [
    {data_dir, "data/barrel_docdb"},
    {http_port, 8080},
    {http_enabled, true}
]}.
```

## Requirements

- Erlang/OTP 27+
- RocksDB (via rocksdb hex package)

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {barrel_docdb, "0.4.1"}
]}.
```

## Architecture

```
barrel_docdb_sup
├── barrel_metrics         (Prometheus metrics)
├── barrel_cache           (RocksDB block cache)
├── barrel_hlc_clock       (Hybrid Logical Clock)
├── barrel_sub             (Path subscriptions)
├── barrel_query_sub       (Query subscriptions)
├── barrel_db_sup          (Database supervisor)
│   └── barrel_db_server   (Per-database process)
├── barrel_rep_tasks       (Replication task manager)
├── barrel_rep_policy      (Policy-based replication)
└── barrel_discovery       (Peer discovery)
```

## API Reference

### Document Operations

| Function | Description |
|----------|-------------|
| `put_doc/2,3` | Create or update document |
| `get_doc/2,3` | Get document by ID |
| `delete_doc/2,3` | Delete document |
| `find/2,3` | Query documents |
| `fold_docs/3` | Iterate documents |

### Replication

| Function | Description |
|----------|-------------|
| `barrel_rep:replicate/2,3` | One-shot replication |
| `barrel_rep_policy:create/2` | Create replication policy |
| `barrel_rep_policy:enable/1` | Enable policy |
| `barrel_rep_tasks:start_task/1` | Start persistent task |

### Federation

| Function | Description |
|----------|-------------|
| `barrel_federation:create/2,3` | Create federation |
| `barrel_federation:find/1,2,3` | Query federation |
| `barrel_federation:set_query/2` | Set stored query |

### Tier Management

| Function | Description |
|----------|-------------|
| `barrel_tier:configure/2` | Configure tiers |
| `barrel_tier:find/2,3` | Query across tiers |
| `barrel_tier:migrate_expired/1,2` | Manual migration |

## Support

| Channel | For |
|---------|-----|
| [GitHub Issues](https://github.com/barrel-db/barrel_docdb/issues) | Bug reports, feature requests |
| [Email](mailto:support@barrel-db.eu) | Commercial inquiries |

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

---

Built by [Enki Multimedia](https://enki-multimedia.eu) | [barrel-db.eu](https://barrel-db.eu)
