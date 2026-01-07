# Barrel P2P Framework - Progress Tracker

## Workflow

**Small verifiable steps:**
1. Make a small change
2. Add/update tests
3. Run tests for the changed module
4. Run full test suite (`rebar3 ct`)
5. If all pass → ask for review
6. If approved → commit and continue

**All features must work:**
- Embedded (Erlang API)
- HTTP API

---

## Completed Features ✅

### Federation with Stored Queries ✅
- `barrel_federation:create/3` with `query` option
- `barrel_federation:find/1` uses stored query
- `barrel_federation:find/2,3` merges stored + provided queries
- `barrel_federation:set_query/2` to update stored query
- Commits: `fc96759`, `ff6901d`

### DNS Domain Discovery ✅
- `barrel_discovery:add_dns_domain/1`
- `barrel_discovery:remove_dns_domain/1`
- `barrel_discovery:list_dns_domains/0`
- Periodic DNS refresh in `handle_info(refresh_peers, ...)`
- Domain members in federation trigger DNS SRV lookup

### HTTP API for Federation ✅
- `POST /_federation` - create with query
- `PUT /_federation/:name` - set/update query
- `GET /_federation/:name/_find` - query with stored query
- `POST /_federation/:name/_find` - query with body

### Peer Discovery ✅
- `barrel_discovery:add_peer/1,2`
- `barrel_discovery:remove_peer/1`
- `barrel_discovery:list_peers/0,1`
- `barrel_discovery:get_peer/1`
- Periodic refresh via `/.well-known/barrel`
- HTTP API: `GET/POST/DELETE /_discovery/peers`

### HTTP API Extensions ✅
- Attachment HTTP API (CRUD + streaming for large files)
- Query HTTP API (`POST /db/:db/_find`)
- Views HTTP API (list, create, delete, query, refresh)
- Changes Stream (poll, longpoll, SSE)

### Local Document Storage ✅
- Dedicated `local_cf` column family in RocksDB
- Per-database local docs: `put_local_doc/3`, `get_local_doc/2`, `delete_local_doc/2`
- System (global) docs: `put_system_doc/2`, `get_system_doc/1`, `delete_system_doc/1`
- Key format: `DbName + NUL + DocId` for per-db, `_system + NUL + DocId` for global

### Conflict Resolution ✅
- `get_conflicts/2` - detect conflicting revisions
- `resolve_conflict/4` - resolve with choose or merge strategy
- Deterministic winner selection (highest rev hash)
- `barrel_compaction_filter.erl` - revision pruning during compaction

### Tier Lifecycle ✅
- `barrel_tier:remove_config/1` - completely remove tier config
- Default `enabled => true` when configured
- `barrel_tier:enable/1`, `barrel_tier:disable/1` for pause/resume

---

## Completed HTTP API Extensions (Reference)

#### Step 1: Attachment HTTP API ✅

**Erlang API (exists):** `barrel_docdb:put_attachment/4`, `get_attachment/3`, `delete_attachment/3`, `list_attachments/2`

**HTTP Routes to add:**
```
GET    /db/:db/:doc_id/_attachments              -> list
GET    /db/:db/:doc_id/_attachments/:att_name    -> get
PUT    /db/:db/:doc_id/_attachments/:att_name    -> put
DELETE /db/:db/:doc_id/_attachments/:att_name    -> delete
```

**Implementation Steps:**

1. **Add routes** to `src/barrel_http_server.erl`:
   - Add attachment routes BEFORE the `/:doc_id` catch-all
   - Routes: `{"/db/:db/:doc_id/_attachments", ...}` and `{"/db/:db/:doc_id/_attachments/:att_name", ...}`

2. **Add handlers** to `src/barrel_http_handler.erl`:
   - `handle_list_attachments/1` → `barrel_docdb:list_attachments/2`
   - `handle_get_attachment/1` → `barrel_docdb:get_attachment/3` (return raw binary)
   - `handle_put_attachment/1` → `barrel_docdb:put_attachment/4` (read Content-Type header)
   - `handle_delete_attachment/1` → `barrel_docdb:delete_attachment/3`

3. **Add tests** to `test/barrel_http_SUITE.erl`:
   - Test group: `attachment_http`
   - Tests: `put_attachment_http`, `get_attachment_http`, `list_attachments_http`, `delete_attachment_http`

**Content-Type handling:**
- PUT: Read `Content-Type` header, store as metadata
- GET: Return stored Content-Type or `application/octet-stream`

**Files:** `src/barrel_http_server.erl`, `src/barrel_http_handler.erl`, `test/barrel_http_SUITE.erl`

#### Step 2: Query HTTP API ✅
**Erlang API (exists):** `barrel_docdb:find/2,3`

**HTTP Routes to add:**
```
POST /db/:db/_find    -> execute query
```

**Request body:**
```json
{
  "where": [{"path": ["type"], "value": "user"}],
  "limit": 100
}
```

**Files:** `src/barrel_http_handler.erl`
**Tests:** `test/barrel_http_SUITE.erl`

#### Step 3: Views HTTP API ✅
**Erlang API (exists):** `barrel_view:register/3`, `query/3`, `list/1`, `unregister/2`

**HTTP Routes:**
```
GET    /db/:db/_views                    -> list views
POST   /db/:db/_views                    -> create view
DELETE /db/:db/_views/:view_id           -> delete view
GET    /db/:db/_views/:view_id/_query    -> query view
```

**Files:** `src/barrel_http_server.erl`, `src/barrel_http_handler.erl`
**Tests:** `test/barrel_http_SUITE.erl`

#### Step 4: Changes Stream (SSE) ✅
**Erlang API (exists):** `barrel_changes_stream:start_link/3`

**HTTP Routes:**
```
GET  /db/:db/_changes         -> poll/longpoll
GET  /db/:db/_changes/stream  -> SSE stream
```

**Files:** `src/barrel_http_handler.erl`
**Tests:** `test/barrel_http_SUITE.erl`

---

## Future Work

### Revision Pruning During Compaction
(Details preserved below)

---

## Core Concepts

```
┌─────────────────────────────────────────────────────────────┐
│                     BARREL P2P NODE                          │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              barrel_docdb (storage)                  │   │
│  │  - Documents with revision trees                     │   │
│  │  - HLC-ordered changes                               │   │
│  │  - Path indexes                                      │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              HTTP Transport                          │   │
│  │  - Replication endpoints                             │   │
│  │  - Query endpoints                                   │   │
│  │  - Cross-db query federation                         │   │
│  └─────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Replication Manager                     │   │
│  │  - Outgoing replications (push)                      │   │
│  │  - Incoming replications (pull)                      │   │
│  │  - Topology configuration                            │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## Replication Patterns

### Pattern 1: Chain Replication

```
A ──▶ B ──▶ C

Write to A, wait until C confirms.
Read from any of A, B, C.
If B is down: A still has data, C may be stale.
```

**Use case**: Strong consistency with read scaling.

### Pattern 2: Tiered Storage

```
A (hot) ──ttl/full──▶ B (warm) ──ttl/full──▶ C (cold)

Recent data in A.
When A exceeds capacity or TTL: migrate to B.
When B exceeds capacity or TTL: migrate to C.
Query spans all tiers transparently.
```

**Use case**: Cost-effective storage, edge caching.

### Pattern 3: Grouped Databases

```
┌───────────────┐
│  Group: users │
│  ┌───┐ ┌───┐ ┌───┐
│  │ A │ │ B │ │ C │
│  └───┘ └───┘ └───┘
│    ↕     ↕     ↕
│  (all replicate to each other)
└───────────────┘
```

**Use case**: Multi-region availability, edge nodes.

### Pattern 4: Multi-Master

```
A ◀──▶ B

Both accept writes.
Conflicts resolved via revision tree (CRDT).
Eventually consistent.
```

**Use case**: Offline-first, distributed editing.

### Pattern 5: Fan-Out

```
      ┌──▶ B
A ────┼──▶ C
      └──▶ D
```

**Use case**: Event distribution, CDN-style.

---

## Conflict Resolution: Revision Tree CRDT

barrel_docdb already has revision trees. We extend this to be a proper CRDT:

```
Document: user:123
           ┌─ 2-abc (node A)
1-xyz ─────┤
           └─ 2-def (node B)

Merge: Both revisions kept in tree.
Winner: Deterministic (highest rev hash).
App can read conflicts and resolve.
```

### Compaction Strategy

Conflicting branches can be compacted:
1. **On read**: Detect conflicts, return winner + conflicts
2. **On compaction**: RocksDB callback merges branches
3. **On explicit resolve**: App writes resolution

---

## Implementation Plan

### Phase 1: HTTP Transport

**Goal**: Enable cross-node replication over HTTP.

#### Step 1.1: HTTP Server Skeleton
- Add cowboy/gun dependencies
- Create `barrel_http_server` module
- Basic health endpoint

**Test**: Start server, GET /health returns 200.

#### Step 1.2: Replication Endpoints
- `POST /db/:name/_changes` - get changes since HLC
- `POST /db/:name/_bulk_docs` - write batch of docs
- `GET /db/:name/:doc_id` - get document

**Test**: Replicate one doc between two nodes via HTTP.

#### Step 1.3: HTTP Transport for barrel_rep
- Implement `barrel_rep_transport_http` behaviour
- Integrate with existing `barrel_rep` module

**Test**: Full replication cycle over HTTP.

---

### Phase 2: Replication Topologies

**Goal**: Support configurable replication patterns.

#### Step 2.1: Replication Configuration
```erlang
-type replication_config() :: #{
    source := binary(),           % local db name
    target := binary(),           % remote URL or local db
    direction := push | pull | both,
    mode := continuous | one_shot,
    wait_for := [binary()]        % chain: wait for these targets
}.
```

**Test**: Configure push replication, verify docs arrive.

#### Step 2.2: Chain Replication
- Support `wait_for` option
- A→B→C: A waits for C to confirm

**Test**: Write to A, verify arrives at C before ACK.

#### Step 2.3: Sync vs Async Per-Request
```erlang
barrel_docdb:put_doc(Db, Doc, #{
    replicate => sync,      % wait for replicas
    wait_for => [<<"nodeC">>]
}).
```

**Test**: Sync write waits, async returns immediately.

---

### Phase 3: Tiered Storage ✅ (COMPLETED)

**Goal**: Automatic data migration based on TTL/capacity.

**Status**: Core functionality implemented. Refinement in progress.

#### Completed:
- TTL Support (set_ttl/4, get_ttl/3, is_expired/3)
- Tier operations (get_tier/3, set_tier/4, classify_by_age/3)
- Migration (migrate_expired/2, migrate_to_tier/4)
- Capacity monitoring (get_db_size/1, get_capacity_info/1, is_capacity_exceeded/1)
- Migration policy (apply_migration_policy/1, run_migration/1,2)
- Cross-tier query (find/3, fold_all_tiers/4)
- Configuration validation (warm_db required for capacity-based auto-migration)

#### Step 3.5: Local Document Storage Formalization ✅

**Goal**: Formalize local document storage for configuration (tier, replication, etc.)

**Key changes:**
1. Dedicated column family for local documents
2. Consistent API for config stored in local docs
3. Proper lifecycle (configure → enable/disable → remove)

---

##### Part A: Add `local_cf` Column Family

**Current state**: Local docs stored in default CF with prefix `0x05`

**Proposed**: Dedicated column family `local_cf`

```erlang
%% barrel_store_rocksdb.erl
-define(LOCAL_CF_NAME, "local").

-type db_ref() :: #{
    db := rocksdb:db_handle(),
    default_cf := rocksdb:cf_handle(),
    bitmap_cf := rocksdb:cf_handle(),
    posting_cf := rocksdb:cf_handle(),
    body_cf := rocksdb:cf_handle(),
    local_cf := rocksdb:cf_handle()   % NEW
}.
```

**Benefits:**
- Cleaner separation (config vs data)
- Different compaction options possible
- No revision tree overhead (already the case, but more explicit)
- Easier to enumerate all configs

**Files:**
- `src/barrel_store_rocksdb.erl`: Add CF, update open/close
- `src/barrel_store_keys.erl`: Simplify local_doc key (no prefix needed in own CF)
- `src/barrel_db_server.erl`: Update local doc operations to use local_cf

---

##### Part B: Global + Per-Database Scope

**Keep existing API** - just add `local_cf` and support both scopes:

```erlang
%% Per-database local docs (existing)
barrel_docdb:put_local_doc(DbName, DocId, Doc)
barrel_docdb:get_local_doc(DbName, DocId)
barrel_docdb:delete_local_doc(DbName, DocId)

%% Global/system local docs (new)
barrel_docdb:put_system_doc(DocId, Doc)
barrel_docdb:get_system_doc(DocId)
barrel_docdb:delete_system_doc(DocId)
```

**Key format in `local_cf`:**
```erlang
%% Per-database: DbName + separator + DocId
local_doc_key(DbName, DocId) -> <<DbName/binary, 0, DocId/binary>>.

%% Global (system-level): "_system" + separator + DocId
system_doc_key(DocId) -> <<"_system", 0, DocId/binary>>.
```

**Use cases:**
- Per-database: tier config, per-db settings
- Global: replication tasks, system-wide settings

---

##### Part C: Tier Config Lifecycle

**Behavior clarification:**

| Action | Result |
|--------|--------|
| `configure(DbName, #{...})` | Creates config, `enabled => true` by default |
| `disable(DbName)` | Sets `enabled => false`, pauses operations |
| `enable(DbName)` | Sets `enabled => true`, resumes operations |
| `remove_config(DbName)` | Deletes config, clears cache, stops completely |

**Changes to barrel_tier.erl:**
1. `default_config()`: `enabled => true` (was `false`)
2. Add `remove_config/1`: Delete local doc + clear persistent_term
3. Migration happens when: `enabled=true` AND (`warm_db` OR `cold_db`) configured

---

**Implementation Order:**
1. Add `local_cf` column family to barrel_store_rocksdb
2. Add local_cf operations (put/get/delete for local_cf)
3. Update barrel_store_keys with new key functions
4. Update barrel_db_server to use local_cf for local docs
5. Add system_doc API to barrel_docdb
6. Update barrel_tier with lifecycle changes (remove_config, default enabled)
7. Add tests

**Files to modify:**
- `src/barrel_store_rocksdb.erl`: Add local_cf column family, CF operations
- `src/barrel_store_keys.erl`: Add local_doc_key/2, system_doc_key/1
- `src/barrel_db_server.erl`: Update local doc ops to use local_cf
- `src/barrel_docdb.erl`: Add put/get/delete_system_doc functions
- `src/barrel_tier.erl`: Add remove_config/1, change default to enabled=>true
- `test/barrel_tier_SUITE.erl`: Add lifecycle tests
- `test/barrel_local_doc_SUITE.erl`: Test local_cf and system docs

---

### Phase 4: Conflict Resolution

**Goal**: CRDT-like merge using revision trees.

#### Step 4.1: Conflict Detection
- Detect conflicting branches in rev tree
- Return `{ok, Doc, Conflicts}` from get_doc

**Test**: Create conflict, verify both branches returned.

#### Step 4.2: Deterministic Winner
- Consistent winner selection across nodes
- Based on rev hash (lexicographic)

**Test**: Same conflict on two nodes, same winner.

#### Step 4.3: Compaction Filter
- Implement RocksDB compaction filter callback
- Merge resolved conflicts during compaction

**Test**: Resolve conflict, trigger compaction, verify single branch.

---

### Phase 5: Cross-Database Query

**Goal**: Query across multiple databases, merge results.

#### Step 5.1: Query Federation Config
```erlang
-type federation() :: #{
    name := binary(),
    members := [binary() | {remote, binary()}]  % local db or URL
}.
```

**Test**: Create federation, query returns results from all members.

#### Step 5.2: Parallel Query Execution
- Fan-out query to all members
- Collect results with timeout

**Test**: Query 3-node federation, all results returned.

#### Step 5.3: Result Merge
- Merge by doc ID
- Conflict resolution (newest rev wins)
- Continuation across federated sources

**Test**: Same doc in multiple dbs, query returns one (newest).

---

### Phase 6: Edge Framework

**Goal**: Easy deployment for edge scenarios.

#### Step 6.1: Node Discovery (Optional)
- Simple DNS-based or config-based discovery
- No automatic clustering (explicit config)

**Test**: Node A discovers Node B via config.

#### Step 6.2: Replication Policies
```erlang
-type policy() :: #{
    name := binary(),
    pattern := chain | tiered | group | fanout,
    nodes := [binary()],
    options := map()
}.
```

**Test**: Apply "chain" policy, verify replication setup.

#### Step 6.3: Monitoring & Metrics
- Replication lag per target
- Conflict rate
- Query latency by source

**Test**: Verify metrics exported.

---

## File Structure

```
src/
├── barrel_http_server.erl      % HTTP server (cowboy)
├── barrel_http_handler.erl     % Request handlers
├── barrel_rep_transport_http.erl % HTTP transport for replication
├── barrel_tier.erl             % Tiered storage manager
├── barrel_federation.erl       % Cross-db query federation
├── barrel_conflict.erl         % Conflict detection/resolution
└── barrel_compaction_filter.erl % RocksDB compaction callback
```

---

## API Examples

### Chain Replication Setup
```erlang
%% Node A
barrel_rep:replicate(#{
    source => <<"mydb">>,
    target => <<"http://nodeB:8080/mydb">>,
    direction => push,
    mode => continuous
}).

%% Node B
barrel_rep:replicate(#{
    source => <<"mydb">>,
    target => <<"http://nodeC:8080/mydb">>,
    direction => push,
    mode => continuous
}).

%% Write with chain confirmation
barrel_docdb:put_doc(<<"mydb">>, Doc, #{
    replicate => sync,
    wait_for => [<<"http://nodeC:8080/mydb">>]
}).
```

### Tiered Storage Setup
```erlang
barrel_tier:configure(#{
    hot => #{db => <<"cache">>, migrate_when => #{ttl => 3600}},
    warm => #{db => <<"main">>, migrate_when => #{capacity => 10_000_000_000}},
    cold => #{db => <<"archive">>}
}).

%% Query spans all tiers
barrel_tier:find(<<"cache">>, #{where => [{path, [<<"type">>], <<"order">>}]}).
```

### Federated Query
```erlang
barrel_federation:create(<<"all_users">>, [
    <<"local_db">>,
    <<"http://nodeB:8080/users">>,
    <<"http://nodeC:8080/users">>
]).

{ok, Results, Meta} = barrel_federation:find(<<"all_users">>, #{
    where => [{path, [<<"active">>], true}]
}).
```

### Multi-Master Sync
```erlang
%% Node A
barrel_rep:replicate(#{
    source => <<"shared">>,
    target => <<"http://nodeB:8080/shared">>,
    direction => both,
    mode => continuous
}).

%% Both nodes can write, conflicts auto-resolved
```

---

## Trade-offs

| Aspect | This Design | Traditional Cluster |
|--------|-------------|---------------------|
| Complexity | Low (composable primitives) | High (coordinator, consensus) |
| Consistency | Configurable per-use | Global setting |
| Failure modes | Explicit (replication lag) | Hidden (quorum loss) |
| Flexibility | High (any topology) | Low (fixed patterns) |
| Coordination | None (P2P) | Required (leader election) |

---

## Project Structure: Extend vs Separate

### Option A: Extend barrel_docdb

| Pros | Cons |
|------|------|
| Single deployment unit | Adds HTTP dependencies (cowboy) to embedded lib |
| Shared test infrastructure | Larger binary for users who don't need P2P |
| Direct access to internals | Feature creep risk |
| Simpler release management | May complicate API surface |
| Users get P2P "for free" | |

### Option B: Separate Project (barrel_p2p)

| Pros | Cons |
|------|------|
| Clean separation of concerns | Two repos to maintain |
| barrel_docdb stays minimal | Version compatibility issues |
| Different release cycles | Duplication of test setup |
| Users opt-in to P2P | Need to export more from barrel_docdb |
| Clear dependency direction | |

### Recommendation: **Extend barrel_docdb**

**Reasoning**:
1. HTTP transport is useful even for single-node (remote API access)
2. Replication already exists in barrel_docdb - HTTP is just a transport
3. Federation/tiering are query-layer features that belong with queries
4. Avoiding version skew between storage and P2P layers
5. Users can ignore P2P if they don't start the HTTP server

**Mitigation for cons**:
- Make cowboy optional (start HTTP only if configured)
- Keep P2P modules prefixed (`barrel_p2p_*` or `barrel_http_*`)
- Document that P2P features are opt-in

---

## HTTP API Extensions (Current Sprint)

### Existing Erlang APIs to Expose

The following APIs exist in barrel_docdb but need HTTP endpoints:

| Module | Functions | Description |
|--------|-----------|-------------|
| `barrel_docdb` | `put_attachment/4`, `get_attachment/3`, `delete_attachment/3`, `list_attachments/2` | Binary attachments |
| `barrel_docdb` | `find/2`, `find/3` | Document queries |
| `barrel_docdb` | `subscribe_query/2`, `unsubscribe_query/1` | Query subscriptions |
| `barrel_view` | `register/3`, `query/3`, `list/1`, `unregister/2` | Materialized views |
| `barrel_sub` | `subscribe/3`, `unsubscribe/2` | Path subscriptions |
| `barrel_changes_stream` | `start_link/3`, `next/1`, `await/1` | Streaming changes |

---

### Step 1: Attachment HTTP API

**Routes:**
```
GET    /db/:db/:doc_id/_attachments              -> list attachments
GET    /db/:db/:doc_id/_attachments/:att_name    -> get attachment
PUT    /db/:db/:doc_id/_attachments/:att_name    -> put attachment
DELETE /db/:db/:doc_id/_attachments/:att_name    -> delete attachment
```

**Implementation:**
1. Add routes to `barrel_http_server.erl` (before `:doc_id` catch-all)
2. Add handlers in `barrel_http_handler.erl`:
   - `handle_list_attachments/1` -> `barrel_docdb:list_attachments/2`
   - `handle_get_attachment/1` -> `barrel_docdb:get_attachment/3` (return raw binary with Content-Type)
   - `handle_put_attachment/1` -> `barrel_docdb:put_attachment/4` (read raw body)
   - `handle_delete_attachment/1` -> `barrel_docdb:delete_attachment/3`

**Content-Type handling:**
- GET returns `application/octet-stream` or stored content-type
- PUT reads `Content-Type` header for storage metadata

---

### Step 2: Query HTTP API

**Routes:**
```
POST /db/:db/_find    -> execute query (find documents)
```

**Request body:**
```json
{
  "where": [{"path": ["type"], "op": "eq", "value": "user"}],
  "order_by": [{"path": ["created_at"], "dir": "desc"}],
  "limit": 100,
  "offset": 0
}
```

**Implementation:**
1. Add route to `barrel_http_server.erl`
2. Add `handle_find/1` handler:
   - Parse query from JSON body
   - Call `barrel_docdb:find/3`
   - Return results with continuation token for pagination

---

### Step 3: Changes API with Subscriptions

**Routes:**
```
GET  /db/:db/_changes                -> poll/longpoll changes
GET  /db/:db/_changes/stream         -> SSE stream
POST /db/:db/_changes/stream         -> SSE stream with filter in body
```

**Query Parameters (both modes):**
- `since=<hlc>` - start position
- `filter=<pattern>` - MQTT-style path filter (uses `barrel_sub`)
- `include_docs=true` - include full documents
- `query=<json_encoded>` - filter using query conditions
- `limit=<n>` - max changes to return (poll mode)
- `feed=longpoll` - hold connection until changes (poll mode)
- `timeout=<ms>` - longpoll timeout (default 60000)

**Long Polling Implementation:**
```erlang
handle_changes(Req) ->
    Feed = cowboy_req:match_qs([{feed, [], <<"normal">>}], Req),
    case Feed of
        <<"longpoll">> ->
            handle_changes_longpoll(Req);  %% Hold until changes or timeout
        _ ->
            handle_changes_poll(Req)       %% Return immediately
    end.
```

**SSE (Server-Sent Events) Implementation:**
```erlang
handle_changes_stream(Req) ->
    Opts = parse_subscribe_opts(Req),
    %% Start SSE response
    Req2 = cowboy_req:stream_reply(200, #{
        <<"content-type">> => <<"text/event-stream">>,
        <<"cache-control">> => <<"no-cache">>
    }, Req),
    %% Start changes stream in push mode
    {ok, Stream} = barrel_changes_stream:start_link(StoreRef, DbName, #{
        mode => push,
        owner => self()
    }),
    %% Loop sending events
    sse_loop(Req2, Stream, FilterFun).
```

**SSE Event Format:**
```
event: change
data: {"id":"doc1","rev":"1-abc","hlc":"..."}

event: heartbeat
data: {}
```

---

### Step 4: Materialized Views HTTP API

**Routes:**
```
GET    /db/:db/_views                           -> list views
POST   /db/:db/_views                           -> create view
GET    /db/:db/_views/:view_id                  -> get view definition
DELETE /db/:db/_views/:view_id                  -> delete view
GET    /db/:db/_views/:view_id/_query           -> query view
POST   /db/:db/_views/:view_id/_query           -> query view with body
```

**Create view request:**
```json
{
  "id": "users_by_email",
  "where": [{"path": ["type"], "op": "eq", "value": "user"}],
  "key": ["email"],
  "value": 1
}
```

**Implementation:**
1. Add routes to `barrel_http_server.erl`
2. Add handlers:
   - `handle_list_views/1` -> `barrel_view:list/1`
   - `handle_create_view/1` -> `barrel_view:register/3`
   - `handle_delete_view/1` -> `barrel_view:unregister/2`
   - `handle_query_view/1` -> `barrel_view:query/3`

---

### Implementation Order

1. **Attachments** - Simple CRUD, no streaming
2. **Query (_find)** - Uses existing `find/3` API
3. **Views** - Uses existing `barrel_view` module
4. **Changes with SSE** - Requires streaming response handling

### Files to Modify

| File | Changes |
|------|---------|
| `src/barrel_http_server.erl` | Add routes for attachments, query, views, changes subscribe |
| `src/barrel_http_handler.erl` | Add handlers for all new endpoints |
| `test/barrel_rep_http_SUITE.erl` | Add tests for each new endpoint |

---

## Summary

This is a **framework**, not a database. Users compose:
- **Replication** for availability and consistency
- **Tiers** for cost and performance
- **Federation** for distributed queries
- **Conflicts** resolved via revision tree CRDT

Built on barrel_docdb primitives, extended with HTTP transport and policy layers.
