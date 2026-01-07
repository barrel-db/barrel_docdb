# Replication Policies

Replication policies provide high-level patterns for common replication topologies, managing the underlying replication tasks automatically.

## Patterns Overview

| Pattern | Topology | Use Case |
|---------|----------|----------|
| **Chain** | A → B → C | Write to head, read from tail |
| **Group** | A ↔ B ↔ C | Multi-master sync |
| **Fanout** | A → B, C, D | Event distribution |

## Chain Replication

Linear replication where each node replicates to the next.

```
┌───────┐     ┌───────┐     ┌───────┐
│ Node A│ ──► │ Node B│ ──► │ Node C│
│ (head)│     │       │     │ (tail)│
└───────┘     └───────┘     └───────┘
   write                       read
```

**Use cases:**
- Strong consistency with read scaling
- Disaster recovery chains

=== "Erlang API"

    ```erlang
    barrel_rep_policy:create(<<"my_chain">>, #{
        pattern => chain,
        nodes => [
            <<"http://nodeA:8080">>,
            <<"http://nodeB:8080">>,
            <<"http://nodeC:8080">>
        ],
        database => <<"mydb">>,
        mode => continuous
    }).

    barrel_rep_policy:enable(<<"my_chain">>).
    ```

=== "HTTP API"

    ```bash
    curl -X POST http://localhost:8080/_policies \
      -H "Content-Type: application/json" \
      -d '{
        "name": "my_chain",
        "pattern": "chain",
        "nodes": [
          "http://nodeA:8080",
          "http://nodeB:8080",
          "http://nodeC:8080"
        ],
        "database": "mydb",
        "mode": "continuous"
      }'

    curl -X POST http://localhost:8080/_policies/my_chain/_enable
    ```

## Group Replication (Multi-Master)

Bidirectional replication between all members.

```
     ┌───────┐
     │ Node A│
     └───┬───┘
        ╱ ╲
       ▼   ▼
┌───────┐ ┌───────┐
│ Node B│◄►│ Node C│
└───────┘ └───────┘
```

**Use cases:**
- Multi-region active-active
- High availability clusters

=== "Erlang API"

    ```erlang
    barrel_rep_policy:create(<<"region_sync">>, #{
        pattern => group,
        members => [
            <<"db1">>,
            <<"http://nodeB:8080/db1">>,
            <<"http://nodeC:8080/db1">>
        ],
        mode => continuous
    }).

    barrel_rep_policy:enable(<<"region_sync">>).
    ```

=== "HTTP API"

    ```bash
    curl -X POST http://localhost:8080/_policies \
      -H "Content-Type: application/json" \
      -d '{
        "name": "region_sync",
        "pattern": "group",
        "members": ["db1", "http://nodeB:8080/db1", "http://nodeC:8080/db1"],
        "mode": "continuous"
      }'
    ```

## Fanout Replication

One source replicates to multiple targets.

```
              ┌───────┐
         ┌──► │ Rep 1 │
         │    └───────┘
┌───────┐│    ┌───────┐
│ Source│├──► │ Rep 2 │
└───────┘│    └───────┘
         │    ┌───────┐
         └──► │ Rep 3 │
              └───────┘
```

**Use cases:**
- Event distribution
- Read replicas
- Analytics pipelines

=== "Erlang API"

    ```erlang
    barrel_rep_policy:create(<<"events">>, #{
        pattern => fanout,
        source => <<"events">>,
        targets => [
            <<"replica1">>,
            <<"replica2">>,
            <<"http://analytics:8080/events">>
        ],
        mode => continuous
    }).

    barrel_rep_policy:enable(<<"events">>).
    ```

=== "HTTP API"

    ```bash
    curl -X POST http://localhost:8080/_policies \
      -H "Content-Type: application/json" \
      -d '{
        "name": "events",
        "pattern": "fanout",
        "source": "events",
        "targets": ["replica1", "replica2", "http://analytics:8080/events"],
        "mode": "continuous"
      }'
    ```

## Policy Management

### List Policies

```bash
curl http://localhost:8080/_policies
```

### Get Policy Details

```bash
curl http://localhost:8080/_policies/my_chain
```

### Enable/Disable

```bash
curl -X POST http://localhost:8080/_policies/my_chain/_enable
curl -X POST http://localhost:8080/_policies/my_chain/_disable
```

### Check Status

=== "Erlang API"

    ```erlang
    {ok, Status} = barrel_rep_policy:status(<<"my_chain">>).
    %% #{
    %%     name => <<"my_chain">>,
    %%     pattern => chain,
    %%     enabled => true,
    %%     task_count => 2,
    %%     tasks => [...]
    %% }
    ```

=== "HTTP API"

    ```bash
    curl http://localhost:8080/_policies/my_chain/_status
    ```

### Delete Policy

```bash
curl -X DELETE http://localhost:8080/_policies/my_chain
```

## Filtering

Apply filters to policies:

```erlang
barrel_rep_policy:create(<<"filtered_sync">>, #{
    pattern => group,
    members => [<<"db1">>, <<"http://remote:8080/db1">>],
    mode => continuous,
    filter => #{
        paths => [<<"users/#">>, <<"orders/#">>],
        query => #{where => [{path, [<<"active">>], true}]}
    }
}).
```

## Modes

| Mode | Description |
|------|-------------|
| `continuous` | Keep replicating indefinitely |
| `one_shot` | Replicate once then stop |

## Sync Writes

Wait for replication to complete before returning:

```erlang
{ok, _} = barrel_docdb:put_doc(<<"mydb">>, Doc, #{
    replicate => sync,
    wait_for => [<<"http://nodeC:8080/db/mydb">>]
}).
```

## Best Practices

1. **Start with simple topologies** - Add complexity only when needed
2. **Monitor policy status** - Check for failed tasks
3. **Use filters** - Don't replicate more data than necessary
4. **Consider network partitions** - Policies handle reconnection automatically
5. **Test failover** - Ensure your topology handles node failures
