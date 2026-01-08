# Barrel DocDB Features

## V2 Posting Lists (erlang-rocksdb 2.5.0)

Barrel DocDB uses erlang-rocksdb 2.5.0's native posting list features for high-performance query execution.

### Key Features

| Feature | Description |
|---------|-------------|
| **Native Intersection** | `postings_intersect_all/1` performs multi-way set intersection in C++ |
| **Pre-sorted Keys** | V2 posting lists store keys in lexicographic order - no Erlang sorting needed |
| **Roaring Bitmaps** | Built-in roaring64 bitmaps for O(1) existence checks |
| **Parse Once** | Postings resources can be parsed once and reused for multiple lookups |

### API

The `barrel_postings` module provides a wrapper for the native postings API:

```erlang
%% Parse a posting list binary into a resource
{ok, Postings} = barrel_postings:open(Binary),

%% Get all keys (already sorted in V2)
Keys = barrel_postings:keys(Postings),

%% Get count without iteration
Count = barrel_postings:count(Postings),

%% O(1) bitmap existence check
true = barrel_postings:bitmap_contains(Postings, <<"doc1">>),

%% Exact existence check
true = barrel_postings:contains(Postings, <<"doc1">>),

%% Set operations
{ok, Result} = barrel_postings:intersection(Postings1, Postings2),
{ok, Result} = barrel_postings:union(Postings1, Postings2),
{ok, Result} = barrel_postings:difference(Postings1, Postings2),

%% Multi-way intersection (most efficient for 3+ lists)
{ok, Result} = barrel_postings:intersect_all([Bin1, Bin2, Bin3]).
```

### Query Execution

Multi-condition equality queries now use native posting list intersection:

```erlang
%% Query: type=user AND status=active
%% Internally uses postings_intersect_all for O(min(n,m)) performance
barrel_docdb:find(Db, #{
    where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>}
    ]
}).
```

### Performance Comparison

Benchmarks run with 5000 documents:

| Operation | Before (Jan 6) | After (Jan 8) | Improvement |
|-----------|----------------|---------------|-------------|
| `multi_index` (2-cond) | 10 ops/s, p50: 102ms | 59 ops/s, p50: 17ms | **6x faster** |
| `three_cond` (3-cond) | 4 ops/s, p50: 270ms | 43 ops/s, p50: 23ms | **11x faster** |
| `three_cond_limit` | 2 ops/s, p50: 303ms | 43 ops/s, p50: 23ms | **13x faster** |
| `multi_index_limit` | 4 ops/s, p50: 302ms | 59 ops/s, p50: 17ms | **15x faster** |
| `multi_index_range` | 6 ops/s, p50: 143ms | 52 ops/s, p50: 19ms | **8x faster** |
| `multi_condition` | 21 ops/s, p50: 48ms | 33 ops/s, p50: 30ms | **1.6x faster** |

**Note**: The dramatic improvements in multi-condition queries come from:
1. Native C++ intersection replacing Erlang set operations
2. Pre-sorted keys eliminating `lists:sort/1` overhead
3. Roaring bitmaps for fast existence verification

### Storage Changes

The V2 integration removed the separate bitmap column family:

| Before | After |
|--------|-------|
| 5 column families (default, bitmap, posting, bodies, local) | 4 column families (default, posting, bodies, local) |
| Bloom filter bitmaps for pre-filtering | V2 posting lists have built-in roaring64 bitmaps |
| Erlang `sets:from_list/1` for intersection | Native `postings_intersect_all/1` |
| `lists:sort/1` for ordering | Keys pre-sorted in V2 format |

### Migration

V2 posting lists are backwards compatible:
- V2 reader can read V1 format
- First merge operation on a posting list triggers automatic V1->V2 upgrade
- No data migration required

---

## Path Indexing

All document paths are automatically indexed for efficient queries:

```erlang
%% Document
#{
    <<"type">> => <<"user">>,
    <<"profile">> => #{
        <<"city">> => <<"Paris">>
    }
}

%% Indexed paths:
%% - [<<"type">>, <<"user">>]
%% - [<<"profile">>, <<"city">>, <<"Paris">>]
```

### Query Types

| Type | Example | Index Usage |
|------|---------|-------------|
| Equality | `{path, [<<"type">>], <<"user">>}` | Posting list lookup |
| Compare | `{compare, [<<"age">>], '>', 18}` | Range scan |
| Prefix | `{prefix, [<<"name">>], <<"Jo">>}` | Interval scan |
| Exists | `{exists, [<<"email">>]}` | Path prefix scan |
| Regex | `{regex, [<<"name">>], <<"^Jo.*">>}` | Path scan + filter |

---

## Materialized Views

Precomputed query results that update automatically:

```erlang
%% Create a view
barrel_view:register(Db, <<"active_users">>, #{
    where => [{path, [<<"status">>], <<"active">>}],
    key => [<<"email">>]
}).

%% Query the view (uses precomputed index)
{ok, Results} = barrel_view:query(Db, <<"active_users">>, #{}).
```

---

## Subscriptions

Real-time notifications for document changes:

```erlang
%% Subscribe to changes matching a path pattern
{ok, SubRef} = barrel_sub:subscribe(Db, [<<"users">>, '+'], self()).

%% Receive change notifications
receive
    {barrel_change, SubRef, #{id := DocId, rev := Rev}} ->
        io:format("Document ~s changed~n", [DocId])
end.
```

---

## Tiered Storage

Automatic data migration based on age or capacity:

```erlang
barrel_tier:configure(<<"hot_db">>, #{
    warm_db => <<"warm_db">>,
    cold_db => <<"cold_db">>,
    ttl => 3600,           %% Move to warm after 1 hour
    capacity => 1000000000  %% Or when hot exceeds 1GB
}).
```

---

## Replication

Multi-master replication with automatic conflict resolution:

```erlang
barrel_rep:replicate(#{
    source => <<"local_db">>,
    target => <<"http://remote:8080/remote_db">>,
    direction => both,
    mode => continuous
}).
```

Conflicts are resolved using revision tree CRDTs with deterministic winner selection.

---

## Latest Benchmark Results (2026-01-08)

Configuration: 5000 documents, 1000 iterations

### Query Performance

#### Fast Operations (>1000 ops/sec)

| Operation | Ops/sec | P50 Latency | P99 Latency |
|-----------|---------|-------------|-------------|
| `prefix_limit` | 35,174 | 22µs | 69µs |
| `prefix` | 33,409 | 22µs | 73µs |
| `simple_eq_limit` | 16,502 | 38µs | 292µs |
| `pure_compare_limit` | 6,021 | 159µs | 255µs |
| `range_cont_100` | 4,798 | 209µs | 275µs |
| `pure_topk` | 2,471 | 374µs | 732µs |
| `range_cont_500` | 1,290 | 707µs | 1.5ms |

#### Moderate Operations (100-1000 ops/sec)

| Operation | Ops/sec | P50 Latency | P99 Latency |
|-----------|---------|-------------|-------------|
| `exists_limit` | 941 | 1.0ms | 1.2ms |
| `pure_compare` | 802 | 1.2ms | 1.8ms |
| `exists` | 769 | 1.3ms | 1.5ms |
| `simple_eq` | 319 | 1.9ms | 24ms |
| `selective_eq` | 281 | 2.7ms | 19.5ms |
| `very_selective_eq` | 269 | 2.8ms | 20ms |
| `nested_path` | 158 | 6.2ms | 8.2ms |

#### Multi-Index Intersection (V2 Postings)

| Operation | Ops/sec | P50 Latency | P99 Latency |
|-----------|---------|-------------|-------------|
| `multi_index` | 59 | 16.8ms | 18.9ms |
| `multi_index_limit` | 59 | 17.0ms | 19.1ms |
| `multi_index_range` | 52 | 19.1ms | 23.6ms |
| `multi_index_range_limit` | 52 | 19.0ms | 28ms |
| `three_cond` | 43 | 23.3ms | 30.5ms |
| `three_cond_limit` | 43 | 22.9ms | 30ms |
| `multi_condition` | 33 | 29.6ms | 39.3ms |
| `range` | 31 | 32.6ms | 40ms |

### CRUD Performance

| Operation | Ops/sec | P50 Latency | P99 Latency |
|-----------|---------|-------------|-------------|
| `update` | 9,344 | 5µs | 15µs |
| `read` | 8,837 | 5µs | 17µs |
| `delete` | 4,943 | 6µs | 32µs |
| `insert` | 4,653 | 179µs | 393µs |

### Changes Performance

| Operation | Ops/sec | P50 Latency | P99 Latency |
|-----------|---------|-------------|-------------|
| `subscription` | 6,530 | 141µs | 289µs |
| `incremental` | 1,373 | 397µs | 6.6ms |
| `full_scan` | 14 | 74ms | 74ms |

Raw results: `bench/results/2026-01-08_16-32-36.json`
