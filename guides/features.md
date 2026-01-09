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

| Operation | Before (Jan 6) | After (Jan 9) | Improvement |
|-----------|----------------|---------------|-------------|
| `simple_eq_limit` | ~500 ops/s | 23,607 ops/s, 0.04ms | **47x faster** |
| `multi_index_limit` | 4 ops/s, p50: 302ms | 9,707 ops/s, 0.10ms | **2,400x faster** |
| `three_cond_limit` | 2 ops/s, p50: 303ms | 7,648 ops/s, 0.13ms | **3,800x faster** |
| `range_limit` | ~50 ops/s | 3,505 ops/s, 0.29ms | **70x faster** |
| `multi_index` (no limit) | 10 ops/s, p50: 102ms | 212 ops/s, 4.73ms | **21x faster** |
| `simple_eq` (no limit) | ~300 ops/s | 512 ops/s, 1.95ms | **1.7x faster** |

**Note**: The dramatic improvements in LIMIT queries come from:
1. **Lazy intersection with early termination** - stops after collecting `limit` results
2. **Value-index iteration** - individual keys per DocId enable true early termination
3. Native C++ intersection replacing Erlang set operations
4. Pre-sorted keys eliminating `lists:sort/1` overhead
5. Roaring bitmaps for O(1) existence verification

**Additional optimizations (Jan 9)**:
6. **Cardinality-based condition ordering** - starts with most selective condition
7. **Batch key verification** - `multi_key_exists` replaces individual `key_exists` calls
8. Mixed equality+range queries now choose optimal iteration order based on cardinality

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

## Latest Benchmark Results (2026-01-09)

Configuration: 5000 documents, 100 iterations

### Query Performance

#### LIMIT Queries (Optimized with lazy iteration + early termination)

| Operation | Ops/sec | Latency | Notes |
|-----------|---------|---------|-------|
| `simple_eq_limit` | **23,607** | 0.04ms | Single equality, limit=10 |
| `multi_index_limit` | **9,707** | 0.10ms | 2 equalities, limit=10 |
| `three_cond_limit` | **7,648** | 0.13ms | 2 equalities + 1 range, limit=10 |
| `range_limit` | **3,505** | 0.29ms | 1 equality + 1 range, limit=10 |

#### Non-LIMIT Queries (Returns all matching docs)

| Operation | Ops/sec | Latency | Notes |
|-----------|---------|---------|-------|
| `simple_eq` | 512 | 1.95ms | Returns all 5000 docs |
| `multi_index` | 212 | 4.73ms | Returns ~1666 docs |
| `range` (include_docs) | 58 | 17.38ms | Returns ~2380 docs with bodies |

### Key Optimizations

1. **Value-index iteration for equality conditions**
   - Individual keys per DocId enable true early termination
   - Previously: loaded entire posting list (e.g., 3333 DocIds)
   - Now: stops after finding `limit` matches

2. **Lazy intersection with early termination**
   - Iterates smallest cardinality condition
   - Verifies others with O(1) index lookups
   - Stops immediately when `limit` reached

3. **Streaming body fetch for include_docs**
   - Fetches bodies in small batches
   - Adaptive batch sizing based on selectivity

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
| `subscription` | 4,449 | 169µs | 422µs |
| `incremental` | 2,567 | 346µs | 1.2ms |
| `wildcard_path` | **96.7** | 1.9ms | 29ms |
| `full_scan` | 13.3 | 75ms | 75ms |

---

## Sharded Prefix Posting Lists (Wildcard Path Changes)

The `wildcard_path` changes query (e.g., `paths => [<<"users/#">>]`) uses **sharded prefix posting lists** for efficient HLC-ordered iteration.

### Key Features

| Feature | Description |
|---------|-------------|
| **Time-Bucketed Sharding** | Entries sharded by 1-hour buckets to bound posting list size |
| **Native Merge Operator** | Uses RocksDB `posting_list_merge_operator` for sorted inserts |
| **Range Scan Discovery** | Only existing buckets are scanned (no empty bucket iteration) |
| **Sorted HLC Entries** | Entries stored as `<< HLC:12, Change/binary >>` in sorted order |

### Index Layout

```
prefix_changes CF (posting_cf):
  Key: PREFIX_CHANGES (0x1B) | db | prefix | 0x00 | bucket (4 bytes BE)
  Value: Posting list of << HLC:12, DocId, Rev, Deleted >>

Example for doc with path type/user:
  Bucket = wall_time div 3600  (1-hour granularity)
  Key: 0x1B | "mydb" | "type/user" | 0x00 | bucket
  Value: [<< hlc1, "doc1", "1-abc", 0 >>, << hlc2, "doc2", "2-def", 0 >>, ...]
```

### Query Execution

1. Compute start bucket from `since` HLC
2. Range scan `posting_cf` from `prefix + start_bucket` to `prefix + 0xFFFFFF`
3. For each bucket, iterate sorted entries and filter by HLC
4. Collect results until `limit` reached

### Performance Improvement

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Ops/sec | 1.9 | 96.7 | **50x faster** |
| P50 latency | 746ms | 1.9ms | **390x faster** |

The improvement comes from:
1. **No full scan** - directly seek to prefix + bucket
2. **No deduplication** - posting lists are already unique
3. **Sorted iteration** - entries pre-sorted by HLC
4. **Bounded bucket size** - 1-hour sharding limits posting list growth

---

## Document Size & Structure Performance

### Document Types Benchmark

Configuration: 1000 documents per type, 100 iterations

| Type | Fields | Depth | Size | Insert (ops/s) | Read (ops/s) | Query EQ | Multi-Idx |
|------|--------|-------|------|----------------|--------------|----------|-----------|
| **small_flat** | 5 | 1 | ~200B | 10,245 | 54,083 | 429 | 223 |
| **medium_flat** | 20 | 1 | ~1KB | 3,305 | 62,854 | 365 | 203 |
| **large_flat** | 100 | 1 | ~5KB | 444 | 24,826 | 442 | 113 |
| **small_nested** | 8 | 3 | ~300B | 7,199 | 58,207 | 342 | 206 |
| **medium_nested** | 25 | 4 | ~2KB | 2,798 | 53,163 | 363 | 191 |
| **large_nested** | 80 | 5 | ~8KB | 649 | 35,398 | 328 | 93 |

**Key Observations:**
- Read performance remains high even for large documents (24-62K ops/s)
- Insert throughput scales inversely with document size (indexing overhead)
- Query performance is relatively stable across sizes (index-driven)
- Nesting depth has minimal impact compared to total field count

### Maximum Document Size

| Size | Write Time | Status |
|------|------------|--------|
| 1 KB | < 1ms | OK |
| 10 KB | < 1ms | OK |
| 100 KB | < 1ms | OK |
| 1 MB | 2ms | OK |
| 5 MB | 11ms | OK |
| 10 MB | 22ms | OK |
| 20 MB | 51ms | OK |
| 50 MB | 178ms | OK |
| 100 MB | 331ms | OK |

**Practical Limits:**
- No hard document size limit in barrel_docdb
- RocksDB handles large values efficiently via BlobDB (values > 4KB stored in separate blob files)
- Recommended: Keep documents under 16 MB for optimal performance
- For large binary data, use attachments (`barrel_att`) which are stored separately
