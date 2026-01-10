# Advanced Features Guide

This guide provides practical examples using curl for Barrel DocDB's advanced features: replication, replication policies, federation, and tiered storage.

All examples assume Barrel DocDB is running on `localhost:8080` with authentication enabled. Set your API key:

```bash
export API_KEY="your_api_key_here"
```

---

## Replication

Replication synchronizes documents between databases. Documents are transferred with their full revision history, enabling automatic conflict detection.

### One-Shot Replication

Copy all documents from source to target:

```bash
# Create source and target databases
curl -X PUT "http://localhost:8080/db/source" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://localhost:8080/db/target" -H "Authorization: Bearer $API_KEY"

# Add documents to source
curl -X PUT "http://localhost:8080/db/source/user1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"name": "Alice", "role": "admin", "active": true}'

curl -X PUT "http://localhost:8080/db/source/user2" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"name": "Bob", "role": "user", "active": true}'

# Replicate source -> target
curl -X POST "http://localhost:8080/db/source/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"target": "http://localhost:8080/db/target"}'
```

Response:
```json
{
  "ok": true,
  "docs_read": 2,
  "docs_written": 2
}
```

### Replication to Remote Node

Replicate to another Barrel DocDB instance:

```bash
# Replicate to remote node with authentication
curl -X POST "http://localhost:8080/db/source/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "target": "http://remote-node:8080/db/target",
    "auth": {"bearer_token": "remote_api_key"}
  }'
```

### Filtered Replication by Path

Replicate only documents matching specific path patterns:

```bash
# Create documents with different types
curl -X PUT "http://localhost:8080/db/source/order1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"type": "order", "total": 150, "customer": "alice"}'

curl -X PUT "http://localhost:8080/db/source/invoice1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"type": "invoice", "amount": 150, "customer": "alice"}'

# Replicate only orders (filter by type/order path)
curl -X POST "http://localhost:8080/db/source/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "target": "http://localhost:8080/db/orders_only",
    "filter": {"paths": ["type/order"]}
  }'
```

### Filtered Replication by Query

Replicate documents matching query conditions:

```bash
# Replicate only active users
curl -X POST "http://localhost:8080/db/source/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "target": "http://localhost:8080/db/active_users",
    "filter": {
      "query": {
        "where": [
          {"path": ["role"], "value": "user"},
          {"path": ["active"], "value": true}
        ]
      }
    }
  }'
```

### Bidirectional Replication

Sync changes in both directions between two databases:

```bash
# Replicate A -> B
curl -X POST "http://localhost:8080/db/db_a/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"target": "http://localhost:8080/db/db_b"}'

# Replicate B -> A
curl -X POST "http://localhost:8080/db/db_b/_replicate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"target": "http://localhost:8080/db/db_a"}'
```

### Verify Replication with Changes Feed

Check that documents were replicated by comparing changes feeds:

```bash
# Get changes from source
curl "http://localhost:8080/db/source/_changes?since=first" \
  -H "Authorization: Bearer $API_KEY"

# Get changes from target (should match)
curl "http://localhost:8080/db/target/_changes?since=first" \
  -H "Authorization: Bearer $API_KEY"
```

---

## Replication Policies

Replication policies provide high-level patterns for common topologies. Policies manage the underlying replication tasks automatically.

### Chain Replication

Linear replication where each node replicates to the next: A -> B -> C

```bash
# Create databases on each node
curl -X PUT "http://node1:8080/db/chain_db" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://node2:8080/db/chain_db" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://node3:8080/db/chain_db" -H "Authorization: Bearer $API_KEY"

# Create chain policy
curl -X POST "http://node1:8080/_policies" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "my_chain",
    "pattern": "chain",
    "nodes": [
      "http://node1:8080",
      "http://node2:8080",
      "http://node3:8080"
    ],
    "database": "chain_db",
    "mode": "continuous",
    "auth": {"bearer_token": "shared_api_key"}
  }'

# Enable the policy
curl -X POST "http://node1:8080/_policies/my_chain/_enable" \
  -H "Authorization: Bearer $API_KEY"

# Add documents to head (node1) - they propagate through the chain
curl -X PUT "http://node1:8080/db/chain_db/doc1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"data": "propagates through chain"}'

# Verify document reached tail (node3)
curl "http://node3:8080/db/chain_db/doc1" \
  -H "Authorization: Bearer $API_KEY"
```

### Group Replication (Multi-Master)

Bidirectional replication between all members:

```bash
# Create databases
curl -X PUT "http://node1:8080/db/group_db" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://node2:8080/db/group_db" -H "Authorization: Bearer $API_KEY"

# Create group policy
curl -X POST "http://node1:8080/_policies" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "region_sync",
    "pattern": "group",
    "members": [
      "http://node1:8080/db/group_db",
      "http://node2:8080/db/group_db"
    ],
    "mode": "continuous",
    "auth": {"bearer_token": "shared_api_key"}
  }'

# Enable the policy
curl -X POST "http://node1:8080/_policies/region_sync/_enable" \
  -H "Authorization: Bearer $API_KEY"

# Add document to node1
curl -X PUT "http://node1:8080/db/group_db/from_node1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"source": "node1", "data": "syncs to all"}'

# Add document to node2
curl -X PUT "http://node2:8080/db/group_db/from_node2" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"source": "node2", "data": "syncs to all"}'

# Both nodes now have both documents
curl "http://node1:8080/db/group_db/from_node2" -H "Authorization: Bearer $API_KEY"
curl "http://node2:8080/db/group_db/from_node1" -H "Authorization: Bearer $API_KEY"
```

### Fanout Replication

One source replicates to multiple targets:

```bash
# Create source and target databases
curl -X PUT "http://localhost:8080/db/events" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://replica1:8080/db/events" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://replica2:8080/db/events" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://analytics:8080/db/events" -H "Authorization: Bearer $API_KEY"

# Create fanout policy
curl -X POST "http://localhost:8080/_policies" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "event_fanout",
    "pattern": "fanout",
    "source": "http://localhost:8080/db/events",
    "targets": [
      "http://replica1:8080/db/events",
      "http://replica2:8080/db/events",
      "http://analytics:8080/db/events"
    ],
    "mode": "continuous",
    "auth": {"bearer_token": "shared_api_key"}
  }'

# Enable the policy
curl -X POST "http://localhost:8080/_policies/event_fanout/_enable" \
  -H "Authorization: Bearer $API_KEY"

# Add event to source - replicates to all targets
curl -X PUT "http://localhost:8080/db/events/event1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"type": "user_signup", "user": "alice", "timestamp": "2024-01-15T10:30:00Z"}'
```

### Policy Management

```bash
# List all policies
curl "http://localhost:8080/_policies" -H "Authorization: Bearer $API_KEY"

# Get policy details
curl "http://localhost:8080/_policies/my_chain" -H "Authorization: Bearer $API_KEY"

# Get policy status
curl "http://localhost:8080/_policies/my_chain/_status" -H "Authorization: Bearer $API_KEY"

# Disable policy
curl -X POST "http://localhost:8080/_policies/my_chain/_disable" \
  -H "Authorization: Bearer $API_KEY"

# Delete policy
curl -X DELETE "http://localhost:8080/_policies/my_chain" \
  -H "Authorization: Bearer $API_KEY"
```

### One-Shot vs Continuous Mode

```bash
# One-shot: replicate once and stop
curl -X POST "http://localhost:8080/_policies" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "backup_sync",
    "pattern": "chain",
    "nodes": ["http://primary:8080", "http://backup:8080"],
    "database": "mydb",
    "mode": "one_shot",
    "auth": {"bearer_token": "api_key"}
  }'

# Continuous: keep replicating indefinitely
curl -X POST "http://localhost:8080/_policies" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "live_sync",
    "pattern": "group",
    "members": ["http://node1:8080/db/live", "http://node2:8080/db/live"],
    "mode": "continuous",
    "auth": {"bearer_token": "api_key"}
  }'
```

---

## Federation

Federation allows querying across multiple databases with merged results.

### Create a Federation

```bash
# Create local and remote databases
curl -X PUT "http://localhost:8080/db/local_users" -H "Authorization: Bearer $API_KEY"

# Create federation with multiple members
curl -X POST "http://localhost:8080/_federation" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "all_users",
    "members": [
      "local_users",
      "http://eu-node:8080/db/users",
      "http://us-node:8080/db/users"
    ]
  }'
```

### Query a Federation

```bash
# Add users to local database
curl -X PUT "http://localhost:8080/db/local_users/alice" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"name": "Alice", "region": "local", "role": "admin"}'

# Query across all federation members
curl -X POST "http://localhost:8080/_federation/all_users/_find" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "where": [{"path": ["role"], "value": "admin"}],
    "limit": 100
  }'
```

Response includes results from all accessible members:
```json
{
  "results": [
    {"_id": "alice", "name": "Alice", "region": "local", "role": "admin"},
    {"_id": "bob", "name": "Bob", "region": "eu", "role": "admin"}
  ],
  "meta": {
    "total": 2,
    "sources": ["local_users", "http://eu-node:8080/db/users"]
  }
}
```

### Federation with Query Filter

Query with conditions:

```bash
# Find active admins across all regions
curl -X POST "http://localhost:8080/_federation/all_users/_find" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "where": [
      {"path": ["role"], "value": "admin"},
      {"path": ["active"], "value": true}
    ]
  }'

# Find users in specific region
curl -X POST "http://localhost:8080/_federation/all_users/_find" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "where": [{"path": ["region"], "value": "us"}]
  }'
```

### Manage Federation Members

```bash
# List all federations
curl "http://localhost:8080/_federation" -H "Authorization: Bearer $API_KEY"

# Get federation info
curl "http://localhost:8080/_federation/all_users" -H "Authorization: Bearer $API_KEY"

# Add a member
curl -X POST "http://localhost:8080/_federation/all_users/members/http%3A%2F%2Fasia-node%3A8080%2Fdb%2Fusers" \
  -H "Authorization: Bearer $API_KEY"

# Remove a member
curl -X DELETE "http://localhost:8080/_federation/all_users/members/http%3A%2F%2Feu-node%3A8080%2Fdb%2Fusers" \
  -H "Authorization: Bearer $API_KEY"

# Delete federation
curl -X DELETE "http://localhost:8080/_federation/all_users" \
  -H "Authorization: Bearer $API_KEY"
```

---

## Tiered Storage

Tiered storage automatically migrates documents between hot, warm, and cold tiers based on age or capacity.

### Setup Tiered Storage

```bash
# Create tier databases
curl -X PUT "http://localhost:8080/db/cache" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://localhost:8080/db/main_storage" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://localhost:8080/db/archive" -H "Authorization: Bearer $API_KEY"

# Configure tiering on hot database
curl -X POST "http://localhost:8080/db/cache/_tier/config" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "enabled": true,
    "warm_db": "main_storage",
    "cold_db": "archive",
    "hot_threshold": 3600,
    "warm_threshold": 86400
  }'
```

### Add Documents and Check Tier

```bash
# Add document to hot tier
curl -X PUT "http://localhost:8080/db/cache/event1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"type": "event", "data": "recent data"}'

# Check document tier
curl "http://localhost:8080/db/cache/event1/_tier" \
  -H "Authorization: Bearer $API_KEY"
```

Response:
```json
{"tier": "hot", "doc_id": "event1"}
```

### Manual Document Migration

```bash
# Migrate specific document to warm tier
curl -X POST "http://localhost:8080/db/cache/_tier/migrate" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"doc_id": "event1", "to_tier": "warm"}'

# Document is now in warm_db (main_storage)
curl "http://localhost:8080/db/main_storage/event1" \
  -H "Authorization: Bearer $API_KEY"
```

### Set Document TTL

```bash
# Set TTL on a document (expires in 1 hour)
curl -X POST "http://localhost:8080/db/cache/session123/_tier/ttl" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"ttl": 3600}'

# Check TTL
curl "http://localhost:8080/db/cache/session123/_tier/ttl" \
  -H "Authorization: Bearer $API_KEY"
```

Response:
```json
{"ttl": 3600, "expires_at": 1736538000}
```

### Run Migration Policy

Trigger migration of all eligible documents:

```bash
curl -X POST "http://localhost:8080/db/cache/_tier/run_migration" \
  -H "Authorization: Bearer $API_KEY"
```

Response:
```json
{
  "ok": true,
  "action": "migration_run",
  "expired": {"deleted": 5},
  "capacity": {"migrated": 10},
  "age": {"classified": 3}
}
```

### Get Capacity Info

```bash
curl "http://localhost:8080/db/cache/_tier/capacity" \
  -H "Authorization: Bearer $API_KEY"
```

Response:
```json
{
  "doc_count": 1000,
  "size_bytes": 1048576,
  "capacity_limit": 10737418240,
  "exceeded": false
}
```

### Disable Tiering

```bash
curl -X POST "http://localhost:8080/db/cache/_tier/config" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"enabled": false}'
```

---

## Combined Example: Multi-Region Architecture

This example shows a multi-region setup with federation, replication policies, and tiered storage.

```bash
# === Setup Regional Databases ===

# US Region (primary)
curl -X PUT "http://us-primary:8080/db/users" -H "Authorization: Bearer $API_KEY"
curl -X PUT "http://us-primary:8080/db/users_archive" -H "Authorization: Bearer $API_KEY"

# EU Region
curl -X PUT "http://eu-primary:8080/db/users" -H "Authorization: Bearer $API_KEY"

# Asia Region
curl -X PUT "http://asia-primary:8080/db/users" -H "Authorization: Bearer $API_KEY"

# === Configure Tiered Storage (US) ===

curl -X POST "http://us-primary:8080/db/users/_tier/config" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "enabled": true,
    "warm_db": "users_archive",
    "hot_threshold": 604800
  }'

# === Setup Group Replication Between Regions ===

curl -X POST "http://us-primary:8080/_policies" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "global_users_sync",
    "pattern": "group",
    "members": [
      "http://us-primary:8080/db/users",
      "http://eu-primary:8080/db/users",
      "http://asia-primary:8080/db/users"
    ],
    "mode": "continuous",
    "auth": {"bearer_token": "global_sync_key"}
  }'

curl -X POST "http://us-primary:8080/_policies/global_users_sync/_enable" \
  -H "Authorization: Bearer $API_KEY"

# === Create Federation for Global Queries ===

curl -X POST "http://us-primary:8080/_federation" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "global_users",
    "members": [
      "users",
      "http://eu-primary:8080/db/users",
      "http://asia-primary:8080/db/users"
    ]
  }'

# === Query All Users Globally ===

curl -X POST "http://us-primary:8080/_federation/global_users/_find" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "where": [{"path": ["active"], "value": true}],
    "limit": 1000
  }'
```

---

## Troubleshooting

### Check Replication Status

```bash
# Get policy status with task details
curl "http://localhost:8080/_policies/my_policy/_status" \
  -H "Authorization: Bearer $API_KEY"
```

### Verify Document Counts

```bash
# Count documents using _find
curl -X POST "http://localhost:8080/db/mydb/_find" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"selector": {}, "limit": 0}'
```

### Check Changes Feed

```bash
# Get all changes
curl "http://localhost:8080/db/mydb/_changes?since=first" \
  -H "Authorization: Bearer $API_KEY"

# Get changes count
curl "http://localhost:8080/db/mydb/_changes?since=first" \
  -H "Authorization: Bearer $API_KEY" | jq '.results | length'
```

### Health Check

```bash
curl "http://localhost:8080/health" -H "Authorization: Bearer $API_KEY"
```
