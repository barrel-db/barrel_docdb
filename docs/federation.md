# Federation

Federation allows you to query across multiple databases - local or remote - and merge results transparently.

## Overview

A federation is a named collection of database members. When you query a federation, Barrel DocDB:

1. Sends the query to all members in parallel
2. Collects and merges results
3. Returns a unified result set

This is useful for:

- **Multi-region deployments** - Query users across all regions
- **Sharded data** - Query across shards without client-side logic
- **Hybrid architectures** - Combine local and remote data sources

## Creating a Federation

=== "Erlang API"

    ```erlang
    %% Create a federation with local and remote members
    barrel_federation:create(<<"all_users">>, [
        <<"local_users">>,
        <<"http://eu.example.com:8080/users">>,
        <<"http://us.example.com:8080/users">>
    ]).
    ```

=== "HTTP API"

    ```bash
    curl -X POST http://localhost:8080/_federation \
      -H "Content-Type: application/json" \
      -d '{
        "name": "all_users",
        "members": [
          "local_users",
          "http://eu.example.com:8080/users",
          "http://us.example.com:8080/users"
        ]
      }'
    ```

## Querying a Federation

Query a federation just like a regular database:

=== "Erlang API"

    ```erlang
    {ok, Results, Meta} = barrel_federation:find(<<"all_users">>, #{
        where => [{path, [<<"role">>], <<"admin">>}],
        limit => 100
    }).

    %% Meta contains source information
    %% #{total => 42, sources => [<<"local_users">>, ...]}
    ```

=== "HTTP API"

    ```bash
    curl -X POST http://localhost:8080/_federation/all_users/_find \
      -H "Content-Type: application/json" \
      -d '{"where": [{"path": ["role"], "value": "admin"}]}'
    ```

## Stored Queries

You can attach a stored query to a federation that acts as a base filter:

```erlang
%% Create federation with stored query
barrel_federation:create(<<"active_users">>, [
    <<"local_db">>,
    <<"http://remote:8080/users">>
], #{
    query => #{where => [{path, [<<"active">>], true}]}
}).

%% Additional query filters are merged with the stored query
{ok, Results, _} = barrel_federation:find(<<"active_users">>, #{
    where => [{path, [<<"role">>], <<"admin">>}]
}).
%% Finds active admins (both conditions apply)
```

## Authentication

When querying remote federation members that require authentication, you can configure auth at two levels:

### Federation-Level Authentication

Set default authentication when creating the federation. This applies to all queries.

=== "Erlang API"

    ```erlang
    %% Bearer token authentication
    barrel_federation:create(<<"secure_users">>, [
        <<"local_users">>,
        <<"http://secure.example.com:8080/users">>
    ], #{
        auth => #{bearer_token => <<"ak_your_api_key">>}
    }).

    %% Basic authentication
    barrel_federation:create(<<"secure_users">>, [
        <<"local_users">>,
        <<"http://secure.example.com:8080/users">>
    ], #{
        auth => #{basic_auth => {<<"admin">>, <<"password">>}}
    }).
    ```

=== "HTTP API"

    ```bash
    # Bearer token
    curl -X POST http://localhost:8080/_federation \
      -H "Content-Type: application/json" \
      -d '{
        "name": "secure_users",
        "members": ["local_users", "http://secure.example.com:8080/users"],
        "auth": {
          "bearer_token": "ak_your_api_key"
        }
      }'

    # Basic auth
    curl -X POST http://localhost:8080/_federation \
      -H "Content-Type: application/json" \
      -d '{
        "name": "secure_users",
        "members": ["local_users", "http://secure.example.com:8080/users"],
        "auth": {
          "basic_auth": {
            "username": "admin",
            "password": "secret"
          }
        }
      }'
    ```

### Per-Query Authentication Override

Override the federation's default auth for a specific query:

=== "Erlang API"

    ```erlang
    {ok, Results, Meta} = barrel_federation:find(<<"secure_users">>, #{
        where => [{path, [<<"role">>], <<"admin">>}]
    }, #{
        auth => #{bearer_token => <<"ak_different_key">>}
    }).
    ```

=== "HTTP API"

    ```bash
    curl -X POST http://localhost:8080/_federation/secure_users/_find \
      -H "Content-Type: application/json" \
      -d '{
        "where": [{"path": ["role"], "value": "admin"}],
        "auth": {
          "bearer_token": "ak_different_key"
        }
      }'
    ```

### Supported Authentication Methods

| Method | Config Key | Description |
|--------|------------|-------------|
| Bearer Token | `bearer_token` | API key or JWT token sent as `Authorization: Bearer <token>` |
| Basic Auth | `basic_auth` | Username/password sent as `Authorization: Basic <base64>` |

## Managing Federation Members

### Add a Member

```erlang
barrel_federation:add_member(<<"all_users">>, <<"http://asia.example.com:8080/users">>).
```

### Remove a Member

```erlang
barrel_federation:remove_member(<<"all_users">>, <<"http://eu.example.com:8080/users">>).
```

### List Federations

=== "Erlang API"

    ```erlang
    {ok, Federations} = barrel_federation:list().
    ```

=== "HTTP API"

    ```bash
    curl http://localhost:8080/_federation
    ```

## Error Handling

If a member is unavailable during a query:

- The query continues with available members
- `Meta` includes information about failed sources
- Results are partial but still returned

```erlang
{ok, Results, Meta} = barrel_federation:find(<<"all_users">>, Query).
%% Meta may contain: #{failed_sources => [<<"http://down.example.com:8080/users">>]}
```

## Best Practices

1. **Keep member counts reasonable** - More members = more latency
2. **Use stored queries** - Pre-filter data at the source
3. **Monitor failed sources** - Check `Meta` for issues
4. **Consider network topology** - Place federation coordinator near most data
