# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
