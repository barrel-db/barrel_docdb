# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
