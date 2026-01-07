# Barrel DocDB

**A document database built in Erlang. Embed it in your app or run it standalone over HTTP.**

Sync anywhere. Query anything. Your data, wherever you need it.

<div class="grid cards" markdown>

-   :rocket: __Quick Start__

    ---

    Get up and running in 5 minutes with our step-by-step guide

    [:octicons-arrow-right-24: Quick Start Guide](getting-started.md)

-   :mag: __Declarative Queries__

    ---

    Query documents with automatic path indexing and powerful filters

    [:octicons-arrow-right-24: Query Guide](queries.md)

-   :zap: __Real-time Changes__

    ---

    Subscribe to changes with MQTT-style path patterns

    [:octicons-arrow-right-24: Changes Feed](changes.md)

-   :arrows_counterclockwise: __P2P Replication__

    ---

    Chain, group, and fanout replication patterns

    [:octicons-arrow-right-24: Replication](replication.md)

</div>

## What is Barrel DocDB?

Barrel DocDB is a production-ready document database built on Erlang/OTP that provides:

- **Document CRUD** with MVCC revision trees and automatic conflict resolution
- **Declarative Queries** with automatic path indexing - no manual index creation needed
- **Real-time Subscriptions** via MQTT-style path patterns and query subscriptions
- **Peer-to-Peer Replication** with configurable patterns (chain, group, fanout)
- **Federated Queries** across multiple databases with merged results
- **Tiered Storage** with automatic TTL/capacity-based migration between hot/warm/cold tiers
- **HTTP API** with REST endpoints, SSE streaming, and Prometheus metrics

## Why Barrel DocDB?

### Use Cases

- **Edge Computing**: Deploy nodes that sync to cloud when connected
- **Multi-Region**: Replicate data across regions with automatic conflict resolution
- **Tiered Caching**: Hot/warm/cold data tiers with automatic migration
- **Event Distribution**: Fan-out patterns for event streaming architectures
- **Offline-First Apps**: Full MVCC support for seamless sync when back online

## Quick Example

Barrel DocDB can run as a **standalone server** accessible via HTTP from any language, or be **embedded** directly into your Erlang/OTP application.

=== "Standalone (HTTP API)"

    Run Barrel as a standalone server and access it from any language via REST:

    ```bash
    # Health check
    curl http://localhost:8080/health

    # Create database
    curl -X POST http://localhost:8080/db/mydb

    # Put document
    curl -X PUT http://localhost:8080/db/mydb/doc1 \
      -H "Content-Type: application/json" \
      -d '{"type": "user", "name": "Alice"}'

    # Get document
    curl http://localhost:8080/db/mydb/doc1

    # Query documents
    curl -X POST http://localhost:8080/db/mydb/_find \
      -H "Content-Type: application/json" \
      -d '{"where": [{"path": ["type"], "value": "user"}]}'

    # Stream changes (SSE)
    curl http://localhost:8080/db/mydb/_changes/stream
    ```

=== "Embedded (Erlang API)"

    Embed Barrel directly into your Erlang/OTP application for maximum performance:

    ```erlang
    %% Start the application
    application:ensure_all_started(barrel_docdb).

    %% Create a database
    {ok, _} = barrel_docdb:create_db(<<"mydb">>).

    %% Save a document
    {ok, #{<<"id">> := DocId, <<"rev">> := Rev}} = barrel_docdb:put_doc(<<"mydb">>, #{
        <<"type">> => <<"user">>,
        <<"name">> => <<"Alice">>
    }).

    %% Fetch the document
    {ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, DocId).

    %% Query documents
    {ok, Users, _Meta} = barrel_docdb:find(<<"mydb">>, #{
        where => [{path, [<<"type">>], <<"user">>}]
    }).

    %% Subscribe to changes (MQTT-style patterns)
    {ok, SubRef} = barrel_docdb:subscribe(<<"mydb">>, <<"type/user/#">>),
    receive {barrel_change, _, Change} -> io:format("~p~n", [Change]) end.
    ```

## Core Features

### :brain: Documents with MVCC

Every document maintains a revision tree, enabling automatic conflict detection and resolution during replication. No data loss, even in distributed scenarios.

### :mag: Declarative Queries

Query documents using path-based conditions with automatic indexing. No need to create indexes manually - Barrel handles it for you.

```erlang
{ok, Results, _} = barrel_docdb:find(<<"mydb">>, #{
    where => [
        {path, [<<"status">>], <<"active">>},
        {'and', [
            {path, [<<"age">>], '>=', 18},
            {path, [<<"role">>], <<"admin">>}
        ]}
    ],
    limit => 100
}).
```

### :arrows_counterclockwise: Replication Policies

High-level patterns for common topologies:

- **Chain**: A → B → C (write to head, read from tail)
- **Group**: A ↔ B ↔ C (multi-master sync)
- **Fanout**: A → B, C, D (event distribution)

### :globe_with_meridians: Federation

Query across multiple databases and merge results transparently:

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

## Get Started

<div class="grid cards" markdown>

-   :material-clock-fast: __5 minutes__

    ---

    Add to your Erlang project and start storing documents

    ```erlang
    {deps, [
        {barrel_docdb, "0.2.0"}
    ]}.
    ```

-   :material-api: __HTTP API__

    ---

    Full REST API for any language

    ```bash
    # Start server on port 8080
    curl http://localhost:8080/health
    ```

</div>

## Community & Support

- [:fontawesome-brands-gitlab: GitLab Repository](https://gitlab.enki.io/barrel-db/barrel_docdb)
- [:material-file-document: API Reference](api/http.md)
- [:material-bug: Report Issues](https://gitlab.enki.io/barrel-db/barrel_docdb/-/issues)
