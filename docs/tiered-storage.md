# Tiered Storage

Tiered storage automatically migrates data between hot, warm, and cold tiers based on TTL or capacity thresholds.

## Overview

```
┌─────────────┐     TTL/Capacity     ┌─────────────┐     TTL/Capacity     ┌─────────────┐
│    HOT      │ ─────────────────►   │    WARM     │ ─────────────────►   │    COLD     │
│   (cache)   │                      │   (main)    │                      │  (archive)  │
│  Fast SSD   │                      │  Standard   │                      │  Slow/Cheap │
└─────────────┘                      └─────────────┘                      └─────────────┘
```

Use cases:

- Caching layer: Hot tier for recent data, warm tier for persistence
- Time-series data: Automatic archival of old data
- Cost optimization: Move cold data to cheaper storage

## Configuration

Configure tiered storage for a database:

=== "HTTP API"

    ```bash
    # Configure tiered storage
    curl -X POST http://localhost:8080/db/cache/_tier/config \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $API_KEY" \
      -d '{
        "enabled": true,
        "warm_db": "main",
        "cold_db": "archive",
        "hot_threshold": 3600,
        "warm_threshold": 86400
      }'
    ```

=== "Erlang API"

    ```erlang
    barrel_tier:configure(<<"cache">>, #{
        warm_db => <<"main">>,
        cold_db => <<"archive">>,
        hot_threshold => 3600,
        warm_threshold => 86400
    }).
    ```

## Configuration Options

| Option | Type | Description |
|--------|------|-------------|
| `enabled` | boolean | Enable/disable tiered storage |
| `warm_db` | string | Database name for warm tier |
| `cold_db` | string | Database name for cold tier (optional) |
| `hot_threshold` | integer | Seconds before migration to warm |
| `warm_threshold` | integer | Seconds before migration to cold |
| `capacity_limit` | integer | Bytes threshold for capacity-based migration |

## HTTP API Reference

### Configure Tiering

```bash
POST /db/:db/_tier/config
```

**Request:**
```json
{
  "enabled": true,
  "warm_db": "main_archive",
  "cold_db": "cold_archive",
  "hot_threshold": 3600,
  "warm_threshold": 86400
}
```

**Response:**
```json
{"ok": true}
```

### Get Configuration

```bash
GET /db/:db/_tier/config
```

**Response:**
```json
{
  "enabled": true,
  "warm_db": "main_archive",
  "cold_db": "cold_archive",
  "hot_threshold": 3600,
  "warm_threshold": 86400
}
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

### Manual Document Migration

Migrate a specific document to a different tier:

```bash
POST /db/:db/_tier/migrate
```

**Request:**
```json
{
  "doc_id": "doc1",
  "to_tier": "warm"
}
```

**Response:**
```json
{"ok": true, "migrated": true}
```

### Run Migration Policy

Trigger the migration policy to process all eligible documents:

```bash
POST /db/:db/_tier/run_migration
```

**Response:**
```json
{
  "ok": true,
  "action": "migration_run",
  "expired": {"deleted": 5},
  "capacity": {"migrated": 10},
  "age": {"classified": 3}
}
```

### Get Document Tier

Check which tier a document is in:

```bash
GET /db/:db/:doc_id/_tier
```

**Response:**
```json
{
  "tier": "hot",
  "doc_id": "doc1"
}
```

### Set Document TTL

Set a time-to-live on a specific document:

```bash
POST /db/:db/:doc_id/_tier/ttl
```

**Request:**
```json
{"ttl": 3600}
```

**Response:**
```json
{
  "ok": true,
  "expires_at": 1736538000
}
```

### Get Document TTL

```bash
GET /db/:db/:doc_id/_tier/ttl
```

**Response:**
```json
{
  "ttl": 3600,
  "expires_at": 1736538000
}
```

## Two-Tier Setup

For simpler setups, use only hot and warm tiers:

```bash
curl -X POST http://localhost:8080/db/events/_tier/config \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "enabled": true,
    "warm_db": "events_archive",
    "hot_threshold": 86400
  }'
```

## Best Practices

1. Choose appropriate TTLs: Balance between performance and storage costs
2. Monitor tier sizes: Set alerts when tiers grow unexpectedly
3. Use capacity limits: Prevent hot tier from consuming too much memory
4. Test queries: Ensure cross-tier queries perform acceptably
5. Consider cold tier latency: Cold storage may be slower
