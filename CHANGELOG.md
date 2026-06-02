# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.2] - 2026-06-02

### Removed
- JWT authentication (`barrel_docdb_jwt`, `console_public_key` config, `bdb_` token branches in the HTTP handler) and the `jose` dependency. API key auth (`ak_*`) is the only bearer-token scheme.
- Dead VDB benchmark code (`bench/src/workloads/barrel_bench_vdb.erl`, `barrel_bench:run_vdb*`, VDB portions of the HTTP bench) referencing the long-removed `barrel_vdb` module.
- Stale `test/docker/CHECKPOINT.md` referencing removed `barrel_rep_policy` work.

### Changed
- Bumped `instrument` to v1.1.3 for Erlang/OTP 29 compatibility (replaces deprecated `catch Expr` with `try ... catch`).
- Replaced deprecated `catch Expr` patterns in `barrel_docdb`, `barrel_db_server`, `barrel_query`, `barrel_query_cursor`, and the affected test suites; added `barrel_store_rocksdb:safe_release_snapshot/1` helper.
- Updated docs to drop references to removed modules (`run_vdb` benchmark; "Sharding Strategy" renamed to "Bucketing Strategy" for the time-bucketed posting list).

## [0.6.1] - 2026-06-01

### Changed
- Moved to Erlang/OTP 28 (CI containers, Dockerfile builder, and docs).

### Fixed
- Docker image now boots on OTP 28: the runtime stage uses `debian:trixie-slim` to match the `erlang:28` builder's glibc (it was left on `bookworm-slim`, so ERTS failed with `GLIBC_2.38 not found`).
- Release CI job builds in the `erlang` container instead of installing Erlang from `packages.erlang-solutions.com`, which had failed the v0.6.0 release.
- The relx release version was hardcoded `0.4.2`; it now matches the application version.

## [0.6.0] - 2026-05-26

### Added
- `GET /.well-known/barrel` node identity endpoint returning `node_id`, `version`, and (when peer auth is initialized) `public_key`, with no dependency on discovery or federation
- Public `barrel_docdb:node_id/0` seam (persistent `_node_id` system document) for an external discovery or cluster layer

### Removed
- **Virtual Database (VDB) and sharding**: removed `barrel_vdb*`, `barrel_shard_map`, and `barrel_shard_rebalance` with the `/vdb` HTTP endpoints
- **Federation and peer discovery**: removed `barrel_federation` and `barrel_discovery` with the `/_federation`, `/_peers`, and `/.well-known/barrel` endpoints
- **Replication policies**: removed `barrel_rep_policy` (chain/group/fanout/tiered patterns) and the `/_policies` endpoints; the replication engine (`barrel_rep`, `barrel_rep_tasks`, transports) is unchanged
- **Tiered storage**: removed `barrel_tier` and the `/db/:db/_tier/*` endpoints; the `created_at`/`expires_at`/`tier` entity columns are kept as reserved fields so the on-disk format is unchanged
- **Materialized views**: removed `barrel_view`, `barrel_view_index`, `barrel_view_sup`, the `register_view`/`unregister_view`/`query_view`/`list_views`/`refresh_view` API, and the `/db/:db/_views/*` endpoints

### Changed
- Reduced scope to a document-database core: CRUD with MVCC, declarative queries, changes feed, attachments, pub/sub, and the replication engine. Sharding, tiering, and clustering can be built on top via the public API (changes feed, revision primitives, HLC, system and local documents, store/index introspection, and metrics).
- `barrel_peer_auth` is retained (used by the HTTP replication transport); its node id now persists in a `_node_id` system document.
- Upgraded the instrument dependency to v1.1.2.

### Fixed
- Metrics exemplar reservoir table is no longer lost on an application stop/start. It is initialized in `barrel_metrics:setup/0`, which previously caused `barrel_rep_tasks` to crash on restart when the first histogram value was recorded.

## [0.5.0] - 2026-04-04

### Added
- **Views with Map/Reduce**: Secondary indexes with map/reduce support
  - Module-based views with `map/1` and optional `reduce/3` callbacks
  - Query-based views with declarative key/value extraction
  - Built-in reduce functions: `_count`, `_sum`, `_stats`
  - Rereduce support for merging results from sharded queries via `merge_reduced_results/2`
  - Manual and automatic refresh modes
- **OpenAPI 3.0 Specification**: Full API documentation with Swagger UI at `/api-docs`
- `barrel_docdb:fold_docs/4` with options support (limit, skip, start_key, end_key)
- `barrel_query:matches/2` for simple condition matching outside of queries

### Changed
- Migrated from legacy `{Epoch, Counter}` sequence format to HLC timestamps throughout
- Removed `barrel_sequence` module (functionality consolidated in `barrel_hlc`)
- Upgraded hlc to 3.0.3 and match_trie to 1.0.0
- Upgraded rocksdb to 2.6.2
- Removed unused bitmap dependency

### Fixed
- Multiple dialyzer warnings across modules
- Dead code removal in fold_range functions
- Unmatched return values in shard rebalancing

## [0.4.1] - 2026-03-08

### Fixed
- Snapshot handle leak in chunked exists/prefix query paths where temporary
  snapshots were never released
- Streaming attachment uploads now clean up orphaned chunks on failure via
  new `abort_stream/1` function

### Added
- `abort_attachment_writer/1` API to clean up partial attachment uploads
- Snapshot support for pure compare queries ensuring read consistency
- `fold_compare_docids_with_snapshot/8` in barrel_ars_index
- `fold_range_posting_compare_with_snapshot/6` in barrel_store_rocksdb

## [0.4.0] - 2026-03-05

### Added
- Authentication support for federation queries with `bearer_token` and `basic_auth` formats
- Federation-level auth stored with config, per-query auth override in find options
- `doc_ids` filtering for changes feed to subscribe to specific documents
- Query-based filtering for changes feed with `where` conditions
- GitHub Actions CI/CD workflows for automated testing and releases

### Fixed
- VDB replication setup by adding `sync` option to peer registration
- JSON encoding of peer public keys in HTTP API responses

### Changed
- Upgraded hackney to 3.2.1

## [0.3.2] - 2026-02-13

### Fixed
- SSE changes stream now correctly includes document bodies when `include_docs=true`
- Fixed `get_changes_full_scan` to route to filtered path when `include_docs` is requested
- Fixed `get_changes_filtered` to set `NeedsDoc` flag when `include_docs=true`

### Added
- `changes_stream_include_docs` test to verify document bodies in SSE events

## [0.3.1] - 2026-02-13

### Verified
- SSE changes stream stability confirmed with all tests passing
- `since=now` parameter handling working correctly
- Heartbeat mechanism keeping connections alive

## [0.3.0] - 2026-02-12

### Added

#### JWT Authentication
- ES256 (ECDSA P-256) JWT token validation for API authentication
- Token format: `bdb_<base64-encoded-JWT>` prefix for barrel_docdb tokens
- Required claims validation: `sub`, `typ`, `oid`, `prm`, `exp`
- Workspace isolation via optional `wid` claim
- Permission-based access control with `is_admin` flag
- File-based or inline PEM key configuration

#### Usage Reporting
- New `barrel_docdb_usage` module for database statistics
- `GET /admin/usage` - Get usage stats for all databases
- `GET /admin/databases/:db/usage` - Get stats for specific database
- Stats include: document_count, storage_bytes, memtable_size, sst_files_size

#### Ed25519 Peer Authentication for P2P Replication
- `barrel_peer_auth` gen_server for Ed25519 key management
- Automatic keypair generation on startup
- Request signing with canonical format: `timestamp|peer_id|method|path|body_hash`
- 5-minute timestamp window for replay protection
- HTTP headers: `X-Peer-Id`, `X-Peer-Timestamp`, `X-Peer-Signature`
- Public key exposed via `/.well-known/barrel` discovery endpoint
- Optional `peer_auth` option in HTTP transport (disabled by default)

### Fixed

#### SSE Changes Stream Reliability
- Reduced heartbeat interval from 60s to 30s (below Cowboy's idle_timeout)
- Added `idle_timeout => 120000` to Cowboy protocol options
- Added `request_timeout => infinity` for long-running SSE streams
- Fixed `since=now` parameter crash (was calling `barrel_hlc:encode(now)`)
- Simplified `parse_since/1` to only accept valid base64-encoded HLC binaries

#### API Compatibility
- Fixed `barrel_docdb_usage` to use `db_pid/1` + `get_store_ref/1` API
- Updated all HTTP tests for hackney 3.x API (body in 4th tuple element)

### Changed
- Upgraded hackney to 3.0.2
- Default heartbeat for SSE streams changed from 60s to 30s

### Tests Added
- `barrel_docdb_usage_SUITE` - 6 tests for usage statistics module
- `barrel_docdb_jwt_SUITE` - 10 tests for JWT authentication
- `barrel_peer_auth_tests` - 11 EUnit tests for peer authentication
- HTTP usage endpoint tests (4 tests in barrel_http_SUITE)
- SSE stream tests: `changes_stream_since_now`, `changes_stream_heartbeat`

## [0.2.0] - 2025-01-12

### Added

#### Virtual Databases (VDB) - Automatic Sharding
- New VDB layer for horizontal scalability with automatic document sharding
- Consistent hashing of document IDs for even distribution across shards
- Scatter-gather queries across all shards with merged results
- HTTP API endpoints for VDB operations (`/vdb/:vdb/*`)
- Shard split and merge operations for rebalancing
- Multi-datacenter sharding with zone-aware placement
- Cross-node VDB configuration synchronization
- VDB shard replication with automatic failover
- Import from regular database to VDB (`POST /vdb/:vdb/_import`)
- VDB benchmarks and comprehensive documentation

#### HTTP/2 Support
- HTTP/2 cleartext (h2c) via HTTP Upgrade mechanism
- HTTP/2 over TLS (h2) with ALPN negotiation
- Graceful degradation to HTTP/1.1 for legacy clients
- Environment variable configuration for TLS/HTTP2 in releases

#### Docker & CI
- Multi-architecture Docker builds (amd64 + arm64)
- GitLab CI pipeline with buildx for container builds
- Docker multi-region VDB test suite

#### Documentation
- VDB overview and sharding guide
- Multi-datacenter deployment documentation
- Advanced features guide with curl examples
- HTTP API reference for VDB endpoints

### Fixed
- HTTP test suite isolation by unlinking server process
- VDB cross-node sync authentication
- Replication policy auth and checkpoint error handling
- HTTP benchmarks and documentation links

### Changed
- Upgraded hackney to 2.0.0-beta.1
- Added LZ4 and Snappy libraries for RocksDB native build

## [0.1.0] - 2024-12-01

### Added
- Initial release
- Document CRUD with MVCC revision trees
- Declarative queries with automatic path indexing
- Real-time subscriptions via MQTT-style path patterns
- HTTP API with REST endpoints
- Peer-to-peer replication with configurable patterns (chain, group, fanout)
- Federated queries across multiple databases
- Tiered storage with automatic TTL/capacity-based migration
- Prometheus metrics for monitoring
- HLC ordering for distributed event coordination
