# HTTP API Reference

Barrel DocDB provides a RESTful HTTP API for all operations. The server listens on port 8080 by default.

## Server Configuration

### Starting the HTTP Server

```erlang
%% Basic start (HTTP/1.1 and HTTP/2 cleartext)
barrel_http_server:start_link(#{port => 8080}).

%% With TLS (HTTPS with HTTP/2 ALPN)
barrel_http_server:start_link(#{
    port => 8443,
    certfile => "/path/to/cert.pem",
    keyfile => "/path/to/key.pem"
}).

%% Full configuration
barrel_http_server:start_link(#{
    port => 8443,
    num_acceptors => 100,
    max_connections => 10000,
    protocols => [http2, http],
    certfile => "/path/to/cert.pem",
    keyfile => "/path/to/key.pem",
    cacertfile => "/path/to/ca.pem",
    verify => verify_peer
}).
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `port` | integer | 8080 | Listen port |
| `num_acceptors` | integer | 100 | Number of acceptor processes |
| `max_connections` | integer | infinity | Maximum concurrent connections |
| `protocols` | list | `[http2, http]` | Enabled protocols |
| `certfile` | string | - | Path to TLS certificate (enables HTTPS) |
| `keyfile` | string | - | Path to TLS private key |
| `cacertfile` | string | - | Path to CA certificate (optional) |
| `verify` | atom | `verify_none` | TLS verification: `verify_none` or `verify_peer` |

### HTTP/2 Support

The server supports HTTP/2 with automatic degradation to HTTP/1.1:

**HTTPS Mode (recommended for production):**

- Uses ALPN (Application-Layer Protocol Negotiation) to negotiate HTTP/2 or HTTP/1.1
- Requires TLS certificates (`certfile` and `keyfile`)
- Clients that support HTTP/2 will use it automatically
- Legacy clients fall back to HTTP/1.1

**HTTP Mode (cleartext):**

- Supports HTTP/2 cleartext (h2c) via:
  - HTTP/2 prior knowledge (client sends HTTP/2 preface directly)
  - HTTP/1.1 Upgrade header
- Falls back to HTTP/1.1 for clients that don't support h2c
- Suitable for internal/trusted networks

### Get Server Info

```erlang
{ok, Info} = barrel_http_server:get_info().
%% Info = #{port => 8080, tls => false, protocols => [http2, http], http2 => true, http11 => true}
```

### Environment Variables (for Releases)

When running as a release, configure the HTTP server via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_HTTP_ENABLED` | `true` | Enable/disable HTTP server |
| `BARREL_HTTP_PORT` | `8080` | Listen port |
| `BARREL_HTTP_ACCEPTORS` | `100` | Number of acceptor processes |
| `BARREL_HTTP_MAX_CONNECTIONS` | `infinity` | Maximum concurrent connections |
| `BARREL_HTTP_TLS_ENABLED` | `false` | Enable HTTPS with HTTP/2 ALPN |
| `BARREL_HTTP_CERTFILE` | - | Path to TLS certificate |
| `BARREL_HTTP_KEYFILE` | - | Path to TLS private key |
| `BARREL_HTTP_CACERTFILE` | - | Path to CA certificate (optional) |
| `BARREL_HTTP_VERIFY` | `verify_none` | TLS verification (`verify_none` or `verify_peer`) |

**Example - HTTPS with HTTP/2:**

```bash
export BARREL_HTTP_PORT=8443
export BARREL_HTTP_TLS_ENABLED=true
export BARREL_HTTP_CERTFILE=/etc/barrel/server.pem
export BARREL_HTTP_KEYFILE=/etc/barrel/server-key.pem
./bin/barrel_docdb start
```

**Example - HTTP/1.1 only:**

```bash
# In sys.config.src, protocols can be configured
# For HTTP/1.1 only, edit sys.config:
# {http_protocols, [http]}
```

## Content Types

The API supports both JSON and CBOR:

- `application/json` (default)
- `application/cbor`

Set the `Content-Type` and `Accept` headers accordingly.

## Endpoints Overview

| Endpoint | Methods | Description |
|----------|---------|-------------|
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |
| `/db/:db` | GET, PUT, DELETE | Database operations |
| `/db/:db/:doc_id` | GET, PUT, DELETE | Document operations |
| `/db/:db/_find` | POST | Query documents |
| `/db/:db/_changes` | GET | Changes feed |
| `/db/:db/_changes/stream` | GET | SSE changes stream |
| `/db/:db/_bulk_docs` | POST | Bulk operations |
| `/db/:db/_replicate` | POST | Trigger replication |
| `/db/:db/:doc_id/_attachments/:name` | GET, PUT, DELETE | Attachments |
| `/db/:db/_tier/config` | GET, POST | Tiered storage config |
| `/db/:db/_tier/capacity` | GET | Tier capacity info |
| `/db/:db/_tier/migrate` | POST | Migrate document |
| `/db/:db/_tier/run_migration` | POST | Run migration policy |
| `/db/:db/:doc_id/_tier` | GET | Get document tier |
| `/db/:db/:doc_id/_tier/ttl` | GET, POST | Document TTL |
| `/vdb` | GET, POST | List/create VDBs |
| `/vdb/:vdb` | GET, DELETE | VDB info/delete |
| `/vdb/:vdb/_shards` | GET | List VDB shards |
| `/vdb/:vdb/_import` | POST | Import from database |
| `/vdb/:vdb/_shards/:shard/_split` | POST | Split a shard |
| `/vdb/:vdb/_shards/:shard/_merge` | POST | Merge shards |
| `/vdb/:vdb/:doc_id` | GET, PUT, DELETE | VDB document operations |
| `/_federation` | GET, POST | Federation management |
| `/_policies` | GET, POST | Replication policies |

---

## Health & Metrics

### Health Check

```bash
GET /health
```

Returns server health status including database information.

**Response:**
```json
{
  "status": "ok",
  "databases": [
    {"name": "mydb", "doc_count": 1234}
  ]
}
```

### Prometheus Metrics

```bash
GET /metrics
```

Returns metrics in Prometheus text format.

---

## Database Operations

### Create Database

```bash
POST /db/:db
```

**Example:**
```bash
curl -X POST http://localhost:8080/db/mydb
```

**Response:** `201 Created`

### Get Database Info

```bash
GET /db/:db
```

**Response:**
```json
{
  "name": "mydb",
  "doc_count": 1234,
  "update_seq": "1-abc123"
}
```

### Delete Database

```bash
DELETE /db/:db
```

**Response:** `200 OK`

---

## Document Operations

### Create/Update Document

```bash
PUT /db/:db/:doc_id
```

**Headers:**
- `Content-Type: application/json`
- `If-Match: <rev>` (optional, for updates)

**Example - Create:**
```bash
curl -X PUT http://localhost:8080/db/mydb/user1 \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com"}'
```

**Example - Update:**
```bash
curl -X PUT http://localhost:8080/db/mydb/user1 \
  -H "Content-Type: application/json" \
  -H "If-Match: 1-abc123" \
  -d '{"name": "Alice Smith", "email": "alice@example.com"}'
```

**Response:**
```json
{
  "ok": true,
  "id": "user1",
  "rev": "1-abc123"
}
```

### Get Document

```bash
GET /db/:db/:doc_id
```

**Query Parameters:**

| Parameter | Description |
|-----------|-------------|
| `rev` | Specific revision |
| `revs` | Include revision history |
| `conflicts` | Include conflicting revisions |

**Example:**
```bash
curl http://localhost:8080/db/mydb/user1
```

**Response:**
```json
{
  "_id": "user1",
  "_rev": "1-abc123",
  "name": "Alice",
  "email": "alice@example.com"
}
```

### Delete Document

```bash
DELETE /db/:db/:doc_id
```

**Headers:**
- `If-Match: <rev>` (required)

**Example:**
```bash
curl -X DELETE http://localhost:8080/db/mydb/user1 \
  -H "If-Match: 1-abc123"
```

---

## Queries

### Find Documents

```bash
POST /db/:db/_find
```

**Request Body:**
```json
{
  "where": [
    {"path": ["type"], "value": "user"},
    {"path": ["age"], "op": ">=", "value": 18}
  ],
  "limit": 100,
  "offset": 0
}
```

**Operators:**

| Operator | Description |
|----------|-------------|
| `=` (default) | Equals |
| `>`, `>=`, `<`, `<=` | Comparisons |
| `!=` | Not equals |
| `in` | Value in list |
| `contains` | Array contains value |
| `prefix` | String prefix match |
| `regex` | Regular expression |

**Example:**
```bash
curl -X POST http://localhost:8080/db/mydb/_find \
  -H "Content-Type: application/json" \
  -d '{
    "where": [{"path": ["status"], "value": "active"}],
    "limit": 10
  }'
```

**Response:**
```json
{
  "docs": [
    {"_id": "doc1", "_rev": "1-abc", "status": "active", ...},
    {"_id": "doc2", "_rev": "1-def", "status": "active", ...}
  ],
  "meta": {
    "total": 42,
    "offset": 0,
    "limit": 10
  }
}
```

---

## Changes Feed

### Poll Changes

```bash
GET /db/:db/_changes
```

**Query Parameters:**

| Parameter | Description |
|-----------|-------------|
| `since` | Start from sequence (use `first` for beginning) |
| `limit` | Maximum changes to return |
| `feed` | `normal` or `longpoll` |
| `timeout` | Long-poll timeout in ms |

**Example:**
```bash
curl "http://localhost:8080/db/mydb/_changes?since=first&limit=100"
```

### SSE Stream

```bash
GET /db/:db/_changes/stream
```

Server-Sent Events stream for real-time updates.

**Example:**
```bash
curl http://localhost:8080/db/mydb/_changes/stream
```

**Events:**
```
data: {"seq": "1-abc", "id": "doc1", "changes": [{"rev": "1-xyz"}]}

data: {"seq": "2-def", "id": "doc2", "changes": [{"rev": "1-uvw"}]}
```

---

## Bulk Operations

### Bulk Docs

```bash
POST /db/:db/_bulk_docs
```

**Request Body:**
```json
{
  "docs": [
    {"_id": "doc1", "name": "Alice"},
    {"_id": "doc2", "name": "Bob"},
    {"_id": "doc3", "_rev": "1-abc", "_deleted": true}
  ]
}
```

**Response:**
```json
[
  {"ok": true, "id": "doc1", "rev": "1-abc"},
  {"ok": true, "id": "doc2", "rev": "1-def"},
  {"ok": true, "id": "doc3", "rev": "2-ghi"}
]
```

---

## Attachments

### Put Attachment

```bash
PUT /db/:db/:doc_id/_attachments/:name
```

**Headers:**
- `Content-Type: <mime-type>`
- `If-Match: <rev>` (for existing documents)

**Example:**
```bash
curl -X PUT http://localhost:8080/db/mydb/doc1/_attachments/photo.jpg \
  -H "Content-Type: image/jpeg" \
  --data-binary @photo.jpg
```

### Get Attachment

```bash
GET /db/:db/:doc_id/_attachments/:name
```

**Example:**
```bash
curl http://localhost:8080/db/mydb/doc1/_attachments/photo.jpg > photo.jpg
```

### Delete Attachment

```bash
DELETE /db/:db/:doc_id/_attachments/:name
```

---

## Federation

### List Federations

```bash
GET /_federation
```

### Create Federation

```bash
POST /_federation
```

**Request Body:**
```json
{
  "name": "all_users",
  "members": ["local_db", "http://nodeB:8080/users"]
}
```

### Query Federation

```bash
POST /_federation/:name/_find
```

Same query format as `/db/:db/_find`.

---

## Replication Policies

### List Policies

```bash
GET /_policies
```

### Create Policy

```bash
POST /_policies
```

**Request Body:**
```json
{
  "name": "my_chain",
  "pattern": "chain",
  "nodes": ["http://nodeA:8080", "http://nodeB:8080"],
  "database": "mydb",
  "mode": "continuous"
}
```

**Patterns:** `chain`, `group`, `fanout`

### Enable/Disable Policy

```bash
POST /_policies/:name/_enable
POST /_policies/:name/_disable
```

### Get Policy Status

```bash
GET /_policies/:name/_status
```

---

## Tiered Storage

### Configure Tiering

```bash
POST /db/:db/_tier/config
```

**Request Body:**
```json
{
  "enabled": true,
  "warm_db": "archive_db",
  "cold_db": "cold_db",
  "hot_threshold": 3600,
  "warm_threshold": 86400
}
```

### Get Tier Config

```bash
GET /db/:db/_tier/config
```

### Get Capacity Info

```bash
GET /db/:db/_tier/capacity
```

**Response:**
```json
{
  "doc_count": 1000,
  "size_bytes": 1048576,
  "capacity_limit": 10737418240,
  "exceeded": false
}
```

### Migrate Document

```bash
POST /db/:db/_tier/migrate
```

**Request Body:**
```json
{
  "doc_id": "doc1",
  "to_tier": "warm"
}
```

### Run Migration Policy

```bash
POST /db/:db/_tier/run_migration
```

### Get Document Tier

```bash
GET /db/:db/:doc_id/_tier
```

**Response:**
```json
{"tier": "hot", "doc_id": "doc1"}
```

### Set/Get Document TTL

```bash
POST /db/:db/:doc_id/_tier/ttl
GET /db/:db/:doc_id/_tier/ttl
```

**Request Body (POST):**
```json
{"ttl": 3600}
```

**Response:**
```json
{"ok": true, "expires_at": 1736538000}
```

---

## Replication

### Trigger Replication

```bash
POST /db/:db/_replicate
```

**Request Body:**
```json
{
  "target": "http://remote:8080/db/target",
  "auth": {"bearer_token": "api_key"},
  "filter": {"paths": ["type/user"]}
}
```

**Response:**
```json
{
  "ok": true,
  "docs_read": 100,
  "docs_written": 100
}
```

---

## VDB (Virtual Database / Sharded Database)

Virtual Databases provide automatic horizontal sharding for large datasets. Documents are distributed across shards based on consistent hashing of the document ID.

### List VDBs

```bash
GET /vdb
```

**Response:**
```json
{
  "vdbs": ["users", "orders", "products"]
}
```

### Create VDB

```bash
POST /vdb
```

**Request Body:**
```json
{
  "name": "users",
  "shard_count": 8,
  "hash_function": "phash2",
  "placement": {
    "zones": ["us-east", "us-west"]
  }
}
```

**Response:** `201 Created`
```json
{
  "ok": true,
  "name": "users",
  "shard_count": 8
}
```

### Get VDB Info

```bash
GET /vdb/:vdb
```

**Response:**
```json
{
  "name": "users",
  "shard_count": 8,
  "doc_count": 1234567,
  "hash_function": "phash2"
}
```

### Delete VDB

```bash
DELETE /vdb/:vdb
```

**Response:** `200 OK`

### List VDB Shards

```bash
GET /vdb/:vdb/_shards
```

**Response:**
```json
{
  "shards": [
    {"shard_id": 0, "status": "active", "primary": "node1@host1"},
    {"shard_id": 1, "status": "active", "primary": "node2@host2"}
  ],
  "ranges": [
    {"shard_id": 0, "start_hash": 0, "end_hash": 2147483647},
    {"shard_id": 1, "start_hash": 2147483648, "end_hash": 4294967295}
  ]
}
```

### Import from Database

Import documents from a regular database into a VDB. Documents are automatically distributed across shards.

```bash
POST /vdb/:vdb/_import
```

**Request Body:**
```json
{
  "source_db": "legacy_users",
  "filter": {
    "where": [{"path": ["type"], "value": "user"}]
  },
  "batch_size": 100,
  "on_conflict": "skip"
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `source_db` | string | required | Source database name |
| `filter` | object | - | Optional filter (same format as `_find`) |
| `batch_size` | integer | 100 | Documents per batch |
| `on_conflict` | string | `skip` | Conflict handling: `skip`, `overwrite`, `merge` |

**Response:**
```json
{
  "docs_read": 1000,
  "docs_written": 950,
  "docs_skipped": 50,
  "errors": 0,
  "started_at": 1736712345000,
  "finished_at": 1736712355000,
  "status": "completed"
}
```

### Split Shard

Split a shard into two shards when it grows too large. The original shard keeps the lower half of its hash range, and a new shard is created for the upper half.

```bash
POST /vdb/:vdb/_shards/:shard/_split
```

**Request Body (optional):**
```json
{
  "batch_size": 100
}
```

**Response:**
```json
{
  "ok": true,
  "new_shard_id": 8,
  "message": "Shard 2 split into shards 2 and 8"
}
```

### Merge Shards

Merge two adjacent shards when they become underutilized. The source shard is merged into the target shard.

```bash
POST /vdb/:vdb/_shards/:shard/_merge
```

**Request Body:**
```json
{
  "target_shard": 3,
  "batch_size": 100
}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `target_shard` | integer | required | Shard to merge into |
| `batch_size` | integer | 100 | Documents per batch during migration |

**Response:**
```json
{
  "ok": true,
  "message": "Shard 2 merged into shard 3"
}
```

**Error - Shards not adjacent:**
```json
{
  "error": "Shards are not adjacent in hash space"
}
```

### VDB Document Operations

VDB document operations work the same as regular database operations, but the VDB automatically routes to the correct shard.

**Create/Update Document:**
```bash
PUT /vdb/:vdb/:doc_id
```

**Get Document:**
```bash
GET /vdb/:vdb/:doc_id
```

**Delete Document:**
```bash
DELETE /vdb/:vdb/:doc_id
```

### VDB Bulk Operations

```bash
POST /vdb/:vdb/_bulk_docs
```

Same format as regular database bulk operations. Documents are automatically routed to the correct shards.

### VDB Queries

```bash
POST /vdb/:vdb/_find
```

Queries are executed across all shards in parallel and results are merged.
