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

- **Caching layer** - Hot tier for recent data, warm tier for persistence
- **Time-series data** - Automatic archival of old data
- **Cost optimization** - Move cold data to cheaper storage

## Configuration

Configure tiered storage for a database:

=== "Erlang API"

    ```erlang
    barrel_tier:configure(<<"cache">>, #{
        warm_db => <<"main">>,
        cold_db => <<"archive">>,
        ttl => 3600,           %% Move to warm after 1 hour
        capacity => 10000000   %% Or when size exceeds 10MB
    }).
    ```

=== "HTTP API"

    ```bash
    curl -X POST http://localhost:8080/_tiers/cache \
      -H "Content-Type: application/json" \
      -d '{
        "warm_db": "main",
        "cold_db": "archive",
        "ttl": 3600,
        "capacity": 10000000
      }'
    ```

## Configuration Options

| Option | Type | Description |
|--------|------|-------------|
| `warm_db` | binary() | Database for warm tier |
| `cold_db` | binary() | Database for cold tier (optional) |
| `ttl` | integer() | Seconds before migration to warm |
| `capacity` | integer() | Bytes threshold for migration |
| `warm_ttl` | integer() | Seconds before migration to cold |
| `warm_capacity` | integer() | Warm tier capacity threshold |

## Querying Across Tiers

Queries automatically span all tiers:

```erlang
%% Query searches hot, warm, and cold tiers transparently
{ok, Results, Meta} = barrel_tier:find(<<"cache">>, #{
    where => [{path, [<<"type">>], <<"event">>}]
}).

%% Meta shows which tiers contained results
%% #{tiers => [hot, warm, cold], counts => #{hot => 10, warm => 50, cold => 200}}
```

## Manual Migration

Force immediate migration of expired data:

```erlang
%% Migrate expired documents from hot to warm
barrel_tier:migrate_expired(<<"cache">>).

%% Force migration even if TTL not reached
barrel_tier:migrate_expired(<<"cache">>, #{force => true}).

%% Migrate specific documents
barrel_tier:migrate(<<"cache">>, [<<"doc1">>, <<"doc2">>], warm).
```

## Tier Status

Check current tier status:

```erlang
{ok, Status} = barrel_tier:status(<<"cache">>).
%% #{
%%     hot => #{doc_count => 100, size => 1024000},
%%     warm => #{doc_count => 5000, size => 50000000},
%%     cold => #{doc_count => 100000, size => 1000000000}
%% }
```

## Migration Callbacks

Register callbacks for migration events:

```erlang
barrel_tier:on_migrate(<<"cache">>, fun(DocId, FromTier, ToTier) ->
    logger:info("Migrated ~s from ~p to ~p", [DocId, FromTier, ToTier])
end).
```

## Two-Tier Setup

For simpler setups, use only hot and warm tiers:

```erlang
barrel_tier:configure(<<"events">>, #{
    warm_db => <<"events_archive">>,
    ttl => 86400  %% Archive after 24 hours
}).
```

## Best Practices

1. **Choose appropriate TTLs** - Balance between performance and storage costs
2. **Monitor tier sizes** - Set alerts when tiers grow unexpectedly
3. **Use capacity limits** - Prevent hot tier from consuming too much memory
4. **Test queries** - Ensure cross-tier queries perform acceptably
5. **Consider cold tier latency** - Cold storage may be slower
