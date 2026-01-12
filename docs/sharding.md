# Multi-Datacenter Sharding Guide

This guide covers deploying Barrel DocDB in a multi-datacenter or multi-region environment using Virtual Databases (VDBs) with zone-aware placement.

## Architecture Overview

```
                    ┌──────────────────────────────────────────────────────┐
                    │                   Load Balancer                       │
                    └─────────────────────────┬────────────────────────────┘
                                              │
        ┌─────────────────────────────────────┼─────────────────────────────────────┐
        │                                     │                                     │
   US-EAST Zone                          EU-WEST Zone                         ASIA Zone
        │                                     │                                     │
┌───────▼───────┐                     ┌───────▼───────┐                     ┌───────▼───────┐
│   Node 1      │                     │   Node 3      │                     │   Node 5      │
│ ┌───────────┐ │                     │ ┌───────────┐ │                     │ ┌───────────┐ │
│ │ users_s0  │◄├─────────────────────┤►│ users_s0  │◄├─────────────────────┤►│ users_s1  │ │
│ │ users_s1  │ │    Replication      │ │ users_s1  │ │    Replication      │ │ users_s2  │ │
│ └───────────┘ │                     │ └───────────┘ │                     │ └───────────┘ │
└───────────────┘                     └───────────────┘                     └───────────────┘
```

## Zone Configuration

### Node Configuration

Each Barrel node is assigned to a zone via its configuration:

```erlang
%% sys.config for us-east node
[
  {barrel_docdb, [
    {zone, <<"us-east">>},
    {node_id, <<"node1">>}
  ]}
].
```

```erlang
%% sys.config for eu-west node
[
  {barrel_docdb, [
    {zone, <<"eu-west">>},
    {node_id, <<"node3">>}
  ]}
].
```

### Environment Variables

Alternatively, configure via environment variables:

```bash
export BARREL_ZONE="us-east"
export BARREL_NODE_ID="node1"
```

### Docker Configuration

```yaml
# docker-compose.yml
services:
  barrel-us-1:
    image: barrel/docdb:latest
    environment:
      BARREL_ZONE: "us-east"
      BARREL_NODE_ID: "us-1"
    ports:
      - "8081:8080"

  barrel-eu-1:
    image: barrel/docdb:latest
    environment:
      BARREL_ZONE: "eu-west"
      BARREL_NODE_ID: "eu-1"
    ports:
      - "8082:8080"
```

---

## Zone Discovery

Nodes discover each other and their zones through the discovery system.

### Check Node Zone

```bash
curl "http://localhost:8080/_zone" \
  -H "Authorization: Bearer $API_KEY"
```

Response:
```json
{
  "zone": "us-east",
  "node_id": "node1"
}
```

### List Nodes by Zone

```erlang
%% Get current node's zone
Zone = barrel_discovery:get_zone().

%% Get all nodes in a zone
{ok, Nodes} = barrel_discovery:nodes_in_zone(<<"us-east">>).

%% Get all known zones
{ok, Zones} = barrel_discovery:list_zones().
```

---

## Zone-Aware VDB Creation

### Basic Multi-Zone Setup

Create a VDB with replicas distributed across zones:

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

This creates:
- 4 shards distributed across both zones
- Each shard replicated to 2 nodes (one per zone when possible)

### Placement Constraints

Fine-tune shard placement with constraints:

```bash
curl -X POST "http://localhost:8080/vdb" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "orders",
    "shard_count": 8,
    "placement": {
      "replica_factor": 3,
      "zones": ["us-east", "eu-west", "asia"],
      "constraints": [
        {"type": "min_per_zone", "value": 1},
        {"type": "prefer_zones", "value": ["us-east", "eu-west"]}
      ]
    }
  }'
```

### Placement Options

| Option | Type | Description |
|--------|------|-------------|
| `replica_factor` | integer | Total replicas per shard |
| `zones` | array | Preferred zones for placement |
| `constraints.min_per_zone` | integer | Minimum replicas per zone |
| `constraints.max_per_zone` | integer | Maximum replicas per zone |
| `constraints.prefer_zones` | array | Ordered zone preference |

---

## Cross-Node Replication

When a VDB is created with `replica_factor > 1`, Barrel automatically sets up replication between nodes.

### How It Works

1. **Primary election**: Each shard has a primary node
2. **Replica assignment**: Replicas placed in different zones when possible
3. **Continuous sync**: Changes replicate asynchronously
4. **Conflict resolution**: MVCC revision trees handle conflicts

### Replication Topology

```
         Primary (us-east)
              │
    ┌─────────┼─────────┐
    ▼         ▼         ▼
 Replica   Replica   Replica
(eu-west)  (asia)   (us-east-2)
```

### Check Replication Status

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
      "primary": "http://us-east-1:8080",
      "replicas": ["http://eu-west-1:8080"],
      "replication_tasks": [
        {
          "source": "users_s0",
          "target": "http://eu-west-1:8080/db/users_s0",
          "status": "active",
          "docs_written": 1500
        }
      ]
    }
  ]
}
```

---

## VDB Config Synchronization

VDB configurations are automatically synchronized across all nodes through a dedicated meta database.

### Automatic Sync

When a VDB is created on any node:

1. Config stored in local `_barrel_vdb_meta` database
2. Broadcast sent to all discovered peers
3. Peers pull and store the config locally
4. Physical shard databases created if needed

### On-Demand Config Pull

When accessing a VDB that doesn't exist locally, the node attempts to pull its config from peers:

```erlang
%% Automatically happens when you access a VDB
{ok, Info} = barrel_vdb:info(<<"users">>).
%% If VDB not found locally, tries to fetch from peers
```

### Manual Sync

Force sync of all VDB configs:

```bash
curl -X POST "http://localhost:8080/_vdb/_sync" \
  -H "Authorization: Bearer $API_KEY"
```

---

## Multi-Region Deployment

### Example: 3-Region Setup

Deploy a globally distributed VDB across US, EU, and Asia regions.

#### 1. Configure Nodes

**US Region (2 nodes):**
```bash
# Node us-east-1
docker run -d --name barrel-us-1 \
  -e BARREL_ZONE=us-east \
  -e BARREL_NODE_ID=us-east-1 \
  -e BARREL_PEERS="http://eu-west-1:8080,http://asia-1:8080" \
  -p 8081:8080 barrel/docdb

# Node us-east-2
docker run -d --name barrel-us-2 \
  -e BARREL_ZONE=us-east \
  -e BARREL_NODE_ID=us-east-2 \
  -e BARREL_PEERS="http://us-east-1:8080" \
  -p 8082:8080 barrel/docdb
```

**EU Region (2 nodes):**
```bash
# Node eu-west-1
docker run -d --name barrel-eu-1 \
  -e BARREL_ZONE=eu-west \
  -e BARREL_NODE_ID=eu-west-1 \
  -e BARREL_PEERS="http://us-east-1:8080,http://asia-1:8080" \
  -p 8083:8080 barrel/docdb

# Node eu-west-2
docker run -d --name barrel-eu-2 \
  -e BARREL_ZONE=eu-west \
  -e BARREL_NODE_ID=eu-west-2 \
  -e BARREL_PEERS="http://eu-west-1:8080" \
  -p 8084:8080 barrel/docdb
```

**Asia Region (1 node):**
```bash
# Node asia-1
docker run -d --name barrel-asia-1 \
  -e BARREL_ZONE=asia \
  -e BARREL_NODE_ID=asia-1 \
  -e BARREL_PEERS="http://us-east-1:8080,http://eu-west-1:8080" \
  -p 8085:8080 barrel/docdb
```

#### 2. Create VDB with Multi-Region Replication

```bash
curl -X POST "http://us-east-1:8080/vdb" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "global_users",
    "shard_count": 8,
    "placement": {
      "replica_factor": 3,
      "zones": ["us-east", "eu-west", "asia"],
      "constraints": [
        {"type": "min_per_zone", "value": 1}
      ]
    }
  }'
```

#### 3. Verify Replication

```bash
# Check status from any node
curl "http://asia-1:8080/vdb/global_users/_replication" \
  -H "Authorization: Bearer $API_KEY"
```

#### 4. Write and Read from Any Region

```bash
# Write to US region
curl -X PUT "http://us-east-1:8080/vdb/global_users/user1" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{"name": "Alice", "region": "us"}'

# Read from Asia region (after replication)
curl "http://asia-1:8080/vdb/global_users/user1" \
  -H "Authorization: Bearer $API_KEY"
```

---

## Shard Rebalancing

As data grows or nodes change, you may need to rebalance shards.

### Split a Large Shard

When a shard grows too large, split it:

```erlang
%% Check shard sizes
{ok, Info} = barrel_vdb:info(<<"users">>).
Shards = maps:get(shards, Info),
%% Find shards with high doc_count or disk_size

%% Split the large shard
{ok, NewShardId} = barrel_shard_rebalance:split_shard(<<"users">>, 0).
```

### Merge Underutilized Shards

When shards are too small, merge adjacent ones:

```erlang
%% Check if shards can be merged (must be adjacent)
{ok, true} = barrel_shard_rebalance:can_merge(<<"users">>, 2, 3).

%% Merge them
ok = barrel_shard_rebalance:merge_shards(<<"users">>, 2, 3).
```

### Monitor Progress

```erlang
ProgressFun = fun(#{phase := Phase, migrated := M, total := T}) ->
    io:format("Phase: ~p, Progress: ~p/~p~n", [Phase, M, T])
end,

%% Split with progress monitoring
{ok, _} = barrel_shard_rebalance:split_shard(<<"users">>, 0, #{
    progress_callback => ProgressFun
}).
```

---

## Failure Handling

### Node Failure

When a node fails:

1. **Read availability**: Reads can be served from any replica
2. **Write handling**: Writes route to available primary/replicas
3. **Automatic recovery**: When node recovers, replication catches up

### Zone Failure

When an entire zone fails:

1. **Read availability**: Other zones serve reads
2. **Write availability**: Writes continue if quorum available
3. **Recovery**: Full resync when zone recovers

### Network Partition

During network partitions:

1. **Split-brain prevention**: MVCC handles conflicts
2. **Eventual consistency**: Conflicts resolved on merge
3. **Manual resolution**: Use revision tree for complex conflicts

---

## Monitoring

### Key Metrics

Monitor these metrics for multi-region deployments:

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `vdb_replication_lag_seconds` | Replication delay | > 30s |
| `vdb_shard_doc_count` | Documents per shard | Variance > 50% |
| `vdb_cross_zone_latency_ms` | Inter-zone latency | > 200ms |
| `vdb_replication_errors_total` | Replication failures | > 0 |

### Health Checks

```bash
# Per-node health
curl "http://node1:8080/health"

# VDB-specific health
curl "http://node1:8080/vdb/users/_replication" | jq '.shards[].replication_tasks[].status'
```

### Prometheus Metrics

```bash
# Scrape metrics endpoint
curl "http://node1:8080/metrics" | grep vdb_
```

---

## Best Practices

### Zone Placement

1. **Odd number of zones**: Use 3 or 5 zones for quorum
2. **Geographic distribution**: Place zones in different regions
3. **Network quality**: Ensure low latency between zones (<100ms)

### Replica Factor Selection

| Scenario | Replica Factor | Zones |
|----------|----------------|-------|
| Development | 1 | 1 |
| Production (single region) | 2 | 1 |
| Production (multi-region) | 3 | 3 |
| High availability | 5 | 3+ |

### Shard Count Guidelines

| Document Count | Recommended Shards |
|----------------|-------------------|
| < 100K | 2-4 |
| 100K - 1M | 4-8 |
| 1M - 10M | 8-16 |
| 10M+ | 16-32 |

### Network Configuration

1. **Use private networks** between zones when possible
2. **Configure timeouts** appropriately for cross-region latency
3. **Enable compression** for cross-region replication
4. **Use TLS** for all inter-node communication

---

## Troubleshooting

### Replication Not Working

1. Check node discovery:
   ```bash
   curl "http://node1:8080/_nodes"
   ```

2. Verify authentication:
   ```bash
   curl "http://node2:8080/health" -H "Authorization: Bearer $API_KEY"
   ```

3. Check replication logs:
   ```bash
   docker logs barrel-node1 | grep replication
   ```

### Config Not Syncing

1. Check VDB exists on source:
   ```bash
   curl "http://source:8080/vdb/mydb"
   ```

2. Force sync:
   ```bash
   curl -X POST "http://target:8080/_vdb/_sync"
   ```

3. Check network connectivity between nodes

### Uneven Shard Distribution

1. Check document IDs for patterns
2. Review hash function configuration
3. Consider manual shard split/merge
