# Erlang API Reference

The Erlang API provides direct access to Barrel DocDB when embedded in your OTP application.

## Getting Started

```erlang
%% Start the application
application:ensure_all_started(barrel_docdb).
```

---

## Database Operations

### barrel_docdb:create_db/1,2

Create a new database.

```erlang
-spec create_db(DbName :: binary()) -> {ok, pid()} | {error, term()}.
-spec create_db(DbName :: binary(), Opts :: map()) -> {ok, pid()} | {error, term()}.
```

**Example:**
```erlang
{ok, _Pid} = barrel_docdb:create_db(<<"mydb">>).

%% With options
{ok, _Pid} = barrel_docdb:create_db(<<"mydb">>, #{
    data_dir => "/custom/path"
}).
```

### barrel_docdb:delete_db/1

Delete a database.

```erlang
-spec delete_db(DbName :: binary()) -> ok | {error, term()}.
```

**Example:**
```erlang
ok = barrel_docdb:delete_db(<<"mydb">>).
```

### barrel_docdb:list_dbs/0

List all databases.

```erlang
-spec list_dbs() -> [binary()].
```

**Example:**
```erlang
Dbs = barrel_docdb:list_dbs().
%% [<<"mydb">>, <<"otherdb">>]
```

### barrel_docdb:db_info/1

Get database information.

```erlang
-spec db_info(DbName :: binary()) -> {ok, map()} | {error, term()}.
```

**Example:**
```erlang
{ok, Info} = barrel_docdb:db_info(<<"mydb">>).
%% #{doc_count => 1234, update_seq => <<"1-abc123">>}
```

---

## Document Operations

### barrel_docdb:put_doc/2,3

Create or update a document.

```erlang
-spec put_doc(Db, Doc) -> {ok, Result} | {error, term()}
    when Db :: binary() | pid(),
         Doc :: map(),
         Result :: #{<<"id">> := binary(), <<"rev">> := binary()}.

-spec put_doc(Db, Doc, Opts) -> {ok, Result} | {error, term()}
    when Db :: binary() | pid(),
         Doc :: map(),
         Opts :: map(),
         Result :: #{<<"id">> := binary(), <<"rev">> := binary()}.
```

**Options:**

| Option | Type | Description |
|--------|------|-------------|
| `rev` | binary() | Required revision for updates |
| `replicate` | sync | Wait for replication |
| `wait_for` | [binary()] | Nodes to wait for |

**Example:**
```erlang
%% Create new document (ID auto-generated)
{ok, #{<<"id">> := DocId, <<"rev">> := Rev}} =
    barrel_docdb:put_doc(<<"mydb">>, #{
        <<"type">> => <<"user">>,
        <<"name">> => <<"Alice">>
    }).

%% Create with specific ID
{ok, _} = barrel_docdb:put_doc(<<"mydb">>, #{
    <<"_id">> => <<"user1">>,
    <<"name">> => <<"Alice">>
}).

%% Update existing document
{ok, _} = barrel_docdb:put_doc(<<"mydb">>, #{
    <<"_id">> => DocId,
    <<"_rev">> => Rev,
    <<"name">> => <<"Alice Smith">>
}).
```

### barrel_docdb:get_doc/2,3

Get a document by ID.

```erlang
-spec get_doc(Db, DocId) -> {ok, Doc} | {error, not_found | term()}
    when Db :: binary() | pid(),
         DocId :: binary(),
         Doc :: map().

-spec get_doc(Db, DocId, Opts) -> {ok, Doc} | {error, not_found | term()}
    when Db :: binary() | pid(),
         DocId :: binary(),
         Opts :: map(),
         Doc :: map().
```

**Options:**

| Option | Type | Description |
|--------|------|-------------|
| `rev` | binary() | Get specific revision |
| `revs` | boolean() | Include revision history |
| `conflicts` | boolean() | Include conflicts |

**Example:**
```erlang
{ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"user1">>).

%% With revision history
{ok, Doc} = barrel_docdb:get_doc(<<"mydb">>, <<"user1">>, #{revs => true}).
```

### barrel_docdb:delete_doc/2,3

Delete a document.

```erlang
-spec delete_doc(Db, DocId) -> {ok, Result} | {error, term()}.
-spec delete_doc(Db, DocId, Opts) -> {ok, Result} | {error, term()}.
```

**Example:**
```erlang
{ok, _} = barrel_docdb:delete_doc(<<"mydb">>, <<"user1">>, #{rev => Rev}).
```

---

## Queries

### barrel_docdb:find/2,3

Query documents using declarative conditions.

```erlang
-spec find(Db, Query) -> {ok, Docs, Meta} | {error, term()}
    when Db :: binary() | pid(),
         Query :: map(),
         Docs :: [map()],
         Meta :: map().
```

**Query Options:**

| Option | Type | Description |
|--------|------|-------------|
| `where` | list() | List of conditions |
| `limit` | integer() | Maximum results |
| `offset` | integer() | Skip results |
| `order_by` | path() | Sort field |
| `order` | asc \| desc | Sort direction |

**Condition Types:**

```erlang
%% Path equality
{path, [<<"field">>], Value}

%% Comparison
{path, [<<"age">>], '>=', 18}

%% Boolean logic
{'and', [Cond1, Cond2]}
{'or', [Cond1, Cond2]}
{'not', Cond}

%% Collection operators
{path, [<<"status">>], 'in', [<<"active">>, <<"pending">>]}
{path, [<<"tags">>], 'contains', <<"erlang">>}

%% Pattern matching
{path, [<<"email">>], 'prefix', <<"admin@">>}
{path, [<<"name">>], 'regex', <<"^A.*">>}
```

**Example:**
```erlang
{ok, Users, Meta} = barrel_docdb:find(<<"mydb">>, #{
    where => [
        {path, [<<"type">>], <<"user">>},
        {'and', [
            {path, [<<"age">>], '>=', 18},
            {path, [<<"status">>], <<"active">>}
        ]}
    ],
    limit => 100,
    order_by => [<<"created_at">>],
    order => desc
}).
```

### barrel_docdb:fold_docs/3

Iterate over all documents.

```erlang
-spec fold_docs(Db, Fun, Acc) -> {ok, Acc} | {error, term()}
    when Db :: binary() | pid(),
         Fun :: fun((Doc :: map(), Acc) -> {ok, Acc} | stop),
         Acc :: term().
```

**Example:**
```erlang
{ok, Count} = barrel_docdb:fold_docs(<<"mydb">>,
    fun(Doc, Acc) -> {ok, Acc + 1} end,
    0
).
```

---

## Subscriptions

### barrel_docdb:subscribe/2,3

Subscribe to document changes using MQTT-style path patterns.

```erlang
-spec subscribe(Db, Pattern) -> {ok, Ref} | {error, term()}
    when Db :: binary(),
         Pattern :: binary(),
         Ref :: reference().
```

**Pattern Syntax:**

| Pattern | Description |
|---------|-------------|
| `type/user` | Exact match |
| `type/user/#` | Match prefix |
| `type/+/active` | Single-level wildcard |

**Example:**
```erlang
%% Subscribe to all user changes
{ok, Ref} = barrel_docdb:subscribe(<<"mydb">>, <<"type/user/#">>),

%% Receive changes
receive
    {barrel_change, Ref, Change} ->
        io:format("Change: ~p~n", [Change])
end.

%% Unsubscribe
barrel_docdb:unsubscribe(Ref).
```

---

## Changes Feed

### barrel_changes:fold/4

Fold over changes since a sequence.

```erlang
-spec fold(Db, Since, Fun, Acc) -> {ok, Acc, LastSeq} | {error, term()}.
```

**Example:**
```erlang
{ok, Changes, LastSeq} = barrel_changes:fold(<<"mydb">>, first,
    fun(Change, Acc) -> {ok, [Change | Acc]} end,
    []
).
```

---

## Replication

### barrel_rep:replicate/2,3

One-shot replication between databases.

```erlang
-spec replicate(Source, Target) -> {ok, Result} | {error, term()}.
-spec replicate(Source, Target, Opts) -> {ok, Result} | {error, term()}.
```

**Options:**

| Option | Type | Description |
|--------|------|-------------|
| `filter` | map() | Filter documents |
| `source_transport` | module() | Source transport |
| `target_transport` | module() | Target transport |

**Example:**
```erlang
%% Local to local
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>).

%% Local to remote
{ok, Result} = barrel_rep:replicate(<<"mydb">>, <<"http://remote:8080/db/mydb">>, #{
    source_transport => barrel_rep_transport_local,
    target_transport => barrel_rep_transport_http
}).

%% With filter
{ok, Result} = barrel_rep:replicate(<<"source">>, <<"target">>, #{
    filter => #{
        paths => [<<"users/#">>],
        query => #{where => [{path, [<<"active">>], true}]}
    }
}).
```

---

## Federation

### barrel_federation:create/2,3

Create a federation across multiple databases.

```erlang
-spec create(Name, Members) -> ok | {error, term()}.
-spec create(Name, Members, Opts) -> ok | {error, term()}.
```

**Example:**
```erlang
barrel_federation:create(<<"all_users">>, [
    <<"local_db">>,
    <<"http://nodeB:8080/users">>,
    <<"http://nodeC:8080/users">>
], #{
    query => #{where => [{path, [<<"active">>], true}]}
}).
```

### barrel_federation:find/1,2

Query a federation.

```erlang
-spec find(Name) -> {ok, Docs, Meta} | {error, term()}.
-spec find(Name, Query) -> {ok, Docs, Meta} | {error, term()}.
```

**Example:**
```erlang
{ok, Results, Meta} = barrel_federation:find(<<"all_users">>, #{
    where => [{path, [<<"role">>], <<"admin">>}]
}).
```

---

## Replication Policies

### barrel_rep_policy:create/2

Create a replication policy.

```erlang
-spec create(Name :: binary(), Config :: map()) -> ok | {error, term()}.
```

**Patterns:**

- `chain` - A → B → C
- `group` - A ↔ B ↔ C (multi-master)
- `fanout` - A → B, C, D

**Example:**
```erlang
%% Chain replication
barrel_rep_policy:create(<<"my_chain">>, #{
    pattern => chain,
    nodes => [<<"http://nodeA:8080">>, <<"http://nodeB:8080">>],
    database => <<"mydb">>,
    mode => continuous
}).

%% Multi-master group
barrel_rep_policy:create(<<"region_sync">>, #{
    pattern => group,
    members => [<<"db1">>, <<"http://nodeB:8080/db1">>],
    mode => continuous
}).

%% Enable policy
barrel_rep_policy:enable(<<"my_chain">>).

%% Check status
{ok, Status} = barrel_rep_policy:status(<<"my_chain">>).
```
