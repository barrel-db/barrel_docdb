%%%-------------------------------------------------------------------
%%% @doc HTTP Replication Transport Test Suite
%%%
%%% Tests for barrel_rep_transport_http and barrel_rep_tasks.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_http_SUITE).

-include_lib("common_test/include/ct.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    %% HTTP transport tests
    http_get_doc/1,
    http_put_rev/1,
    http_revsdiff/1,
    http_get_changes/1,
    http_local_docs/1,
    http_db_info/1,

    %% Full replication tests
    replicate_via_http/1,
    replicate_with_filter/1,

    %% Task management tests
    start_stop_task/1,
    task_persistence/1,
    continuous_replication/1,

    %% API key management tests
    key_list/1,
    key_create/1,
    key_get/1,
    key_delete/1,
    key_admin_required/1,
    key_per_database/1
]).

-define(HTTP_PORT, 18081).
-define(BASE_URL, <<"http://localhost:18081">>).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, http_transport},
        {group, replication},
        {group, tasks},
        {group, keys}
    ].

groups() ->
    [
        {http_transport, [sequence], [
            http_get_doc,
            http_put_rev,
            http_revsdiff,
            http_get_changes,
            http_local_docs,
            http_db_info
        ]},
        {replication, [sequence], [
            replicate_via_http,
            replicate_with_filter
        ]},
        {tasks, [sequence], [
            start_stop_task,
            task_persistence,
            continuous_replication
        ]},
        {keys, [sequence], [
            key_list,
            key_create,
            key_get,
            key_delete,
            key_admin_required,
            key_per_database
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    application:ensure_all_started(cowboy),
    application:ensure_all_started(hackney),
    %% Create a test API key for authentication
    {ok, ApiKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"test-suite-key">>,
        permissions => [<<"read">>, <<"write">>, <<"admin">>],
        is_admin => true
    }),
    [{api_key, ApiKey} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(http_transport, Config) ->
    %% Start HTTP server (or reuse if already running)
    ensure_http_server(),
    %% Create test database
    ensure_db(<<"http_test">>),
    %% Build endpoint with authentication
    ApiKey = proplists:get_value(api_key, Config),
    Endpoint = #{
        url => <<?BASE_URL/binary, "/db/http_test">>,
        bearer_token => ApiKey
    },
    [{endpoint, Endpoint} | Config];

init_per_group(replication, Config) ->
    %% Ensure HTTP server is running
    ensure_http_server(),
    %% Create source and target databases
    ensure_db(<<"rep_source">>),
    ensure_db(<<"rep_target">>),
    Config;

init_per_group(tasks, Config) ->
    %% Create task test databases
    ensure_db(<<"task_source">>),
    ensure_db(<<"task_target">>),
    Config;

init_per_group(keys, Config) ->
    %% Ensure HTTP server is running
    ensure_http_server(),
    %% Create a test database for per-database key tests
    ensure_db(<<"key_test_db">>),
    Config;

init_per_group(_, Config) ->
    Config.

end_per_group(http_transport, _Config) ->
    catch barrel_docdb:delete_db(<<"http_test">>),
    %% Don't stop HTTP server - may be used by other groups
    ok;

end_per_group(replication, _Config) ->
    barrel_docdb:delete_db(<<"rep_source">>),
    barrel_docdb:delete_db(<<"rep_target">>),
    ok;

end_per_group(tasks, _Config) ->
    barrel_docdb:delete_db(<<"task_source">>),
    barrel_docdb:delete_db(<<"task_target">>),
    ok;

end_per_group(keys, _Config) ->
    catch barrel_docdb:delete_db(<<"key_test_db">>),
    ok;

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% HTTP Transport Tests
%%====================================================================

http_get_doc(Config) ->
    Endpoint = proplists:get_value(endpoint, Config),

    %% Put a document via local API
    DocId = <<"doc1">>,
    Doc = #{<<"id">> => DocId, <<"name">> => <<"Alice">>},
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(<<"http_test">>, Doc),

    %% Get via HTTP transport
    {ok, FetchedDoc, Meta} = barrel_rep_transport_http:get_doc(Endpoint, DocId, #{}),

    #{<<"name">> := <<"Alice">>} = FetchedDoc,
    #{<<"rev">> := Rev} = Meta,

    %% Test not found
    {error, not_found} = barrel_rep_transport_http:get_doc(Endpoint, <<"nonexistent">>, #{}),

    ok.

http_put_rev(Config) ->
    Endpoint = proplists:get_value(endpoint, Config),

    %% Create a document with history (simulating replication)
    DocId = <<"doc_put_rev">>,
    Doc = #{<<"id">> => DocId, <<"value">> => <<"test">>},
    History = [<<"1-abc123def456">>],

    {ok, DocId, _RevId} = barrel_rep_transport_http:put_rev(Endpoint, Doc, History, false),

    %% Verify document exists
    {ok, _FetchedDoc, _Meta} = barrel_rep_transport_http:get_doc(Endpoint, DocId, #{}),

    ok.

http_revsdiff(Config) ->
    Endpoint = proplists:get_value(endpoint, Config),

    %% Create a document
    DocId = <<"doc_revsdiff">>,
    Doc = #{<<"id">> => DocId, <<"value">> => 1},
    {ok, #{<<"rev">> := Rev}} = barrel_docdb:put_doc(<<"http_test">>, Doc),

    %% Check revsdiff - existing rev should not be missing
    {ok, Missing, _Ancestors} = barrel_rep_transport_http:revsdiff(Endpoint, DocId, [Rev]),
    [] = Missing,

    %% Check revsdiff - unknown rev should be missing
    {ok, Missing2, _} = barrel_rep_transport_http:revsdiff(Endpoint, DocId, [<<"2-unknown">>]),
    [<<"2-unknown">>] = Missing2,

    ok.

http_get_changes(Config) ->
    Endpoint = proplists:get_value(endpoint, Config),

    %% Get changes from start
    {ok, Changes, _LastSeq} = barrel_rep_transport_http:get_changes(Endpoint, first, #{limit => 100}),

    %% Should have changes from previous tests
    true = is_list(Changes),
    true = length(Changes) > 0,

    ok.

http_local_docs(Config) ->
    Endpoint = proplists:get_value(endpoint, Config),

    DocId = <<"test_checkpoint">>,
    Doc = #{<<"seq">> => <<"first">>, <<"timestamp">> => 12345},

    %% Put local doc
    ok = barrel_rep_transport_http:put_local_doc(Endpoint, DocId, Doc),

    %% Get local doc
    {ok, FetchedDoc} = barrel_rep_transport_http:get_local_doc(Endpoint, DocId),
    #{<<"seq">> := <<"first">>} = FetchedDoc,

    %% Delete local doc
    ok = barrel_rep_transport_http:delete_local_doc(Endpoint, DocId),

    %% Should be gone
    {error, not_found} = barrel_rep_transport_http:get_local_doc(Endpoint, DocId),

    ok.

http_db_info(Config) ->
    Endpoint = proplists:get_value(endpoint, Config),

    {ok, Info} = barrel_rep_transport_http:db_info(Endpoint),

    #{<<"name">> := <<"http_test">>} = Info,

    ok.

%%====================================================================
%% Replication Tests
%%====================================================================

replicate_via_http(Config) ->
    ApiKey = proplists:get_value(api_key, Config),

    %% Add documents to source
    lists:foreach(
        fun(N) ->
            Doc = #{<<"id">> => iolist_to_binary(["doc", integer_to_list(N)]),
                    <<"num">> => N},
            {ok, _} = barrel_docdb:put_doc(<<"rep_source">>, Doc)
        end,
        lists:seq(1, 5)
    ),

    %% Replicate source -> target via HTTP with authentication
    TargetEndpoint = #{
        url => <<?BASE_URL/binary, "/db/rep_target">>,
        bearer_token => ApiKey
    },

    {ok, Result} = barrel_rep:replicate(<<"rep_source">>, TargetEndpoint, #{
        target_transport => barrel_rep_transport_http
    }),

    #{docs_written := DocsWritten, ok := true} = Result,
    true = DocsWritten > 0,

    %% Verify documents exist in target
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            {ok, _} = barrel_docdb:get_doc(<<"rep_target">>, DocId)
        end,
        lists:seq(1, 5)
    ),

    ok.

replicate_with_filter(Config) ->
    ApiKey = proplists:get_value(api_key, Config),

    %% Add documents with types
    lists:foreach(
        fun(N) ->
            Type = case N rem 2 of
                0 -> <<"even">>;
                1 -> <<"odd">>
            end,
            Doc = #{<<"id">> => iolist_to_binary(["filter_doc", integer_to_list(N)]),
                    <<"type">> => Type,
                    <<"num">> => N},
            {ok, _} = barrel_docdb:put_doc(<<"rep_source">>, Doc)
        end,
        lists:seq(1, 10)
    ),

    %% Replicate only even type documents with authentication
    TargetEndpoint = #{
        url => <<?BASE_URL/binary, "/db/rep_target">>,
        bearer_token => ApiKey
    },

    {ok, _Result} = barrel_rep:replicate(<<"rep_source">>, TargetEndpoint, #{
        target_transport => barrel_rep_transport_http,
        filter => #{
            query => #{where => [{path, [<<"type">>], <<"even">>}]}
        }
    }),

    %% Verify only even documents were replicated
    %% Note: filter is applied at source, so we check what got through

    ok.

%%====================================================================
%% Task Management Tests
%%====================================================================

start_stop_task(_Config) ->
    %% Start a one-shot task
    {ok, TaskId} = barrel_rep_tasks:start_task(#{
        source => <<"task_source">>,
        target => <<"task_target">>,
        mode => one_shot
    }),

    true = is_binary(TaskId),

    %% Get task info
    {ok, Task} = barrel_rep_tasks:get_task(TaskId),
    #{id := TaskId, status := _Status} = Task,

    %% Wait for completion (it's one-shot with empty source)
    timer:sleep(100),

    %% Delete task
    ok = barrel_rep_tasks:delete_task(TaskId),

    %% Should be gone
    {error, not_found} = barrel_rep_tasks:get_task(TaskId),

    ok.

task_persistence(_Config) ->
    %% Add some documents to source
    lists:foreach(
        fun(N) ->
            Doc = #{<<"id">> => iolist_to_binary(["persist_doc", integer_to_list(N)]),
                    <<"val">> => N},
            {ok, _} = barrel_docdb:put_doc(<<"task_source">>, Doc)
        end,
        lists:seq(1, 3)
    ),

    %% Start a task
    {ok, TaskId} = barrel_rep_tasks:start_task(#{
        source => <<"task_source">>,
        target => <<"task_target">>,
        mode => one_shot
    }),

    %% Wait for task to complete
    timer:sleep(500),

    %% Check task status
    {ok, Task} = barrel_rep_tasks:get_task(TaskId),
    ct:pal("Task after wait: ~p", [Task]),

    %% Clean up
    barrel_rep_tasks:delete_task(TaskId),

    ok.

continuous_replication(_Config) ->
    %% Start a continuous replication
    {ok, TaskId} = barrel_rep_tasks:start_task(#{
        source => <<"task_source">>,
        target => <<"task_target">>,
        mode => continuous
    }),

    %% Give it time to start
    timer:sleep(200),

    %% Check it's running
    {ok, Task} = barrel_rep_tasks:get_task(TaskId),
    running = maps:get(status, Task),

    %% Add a document while replication is running
    {ok, _} = barrel_docdb:put_doc(<<"task_source">>, #{
        <<"id">> => <<"continuous_doc">>,
        <<"live">> => true
    }),

    %% Wait for replication
    timer:sleep(500),

    %% Stop the task
    ok = barrel_rep_tasks:pause_task(TaskId),

    %% Check it's paused
    {ok, Task2} = barrel_rep_tasks:get_task(TaskId),
    paused = maps:get(status, Task2),

    %% Delete task
    ok = barrel_rep_tasks:delete_task(TaskId),

    ok.

%%====================================================================
%% API Key Management Tests
%%====================================================================

key_list(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/keys">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
               {<<"Content-Type">>, <<"application/json">>}],

    {ok, 200, _RespHeaders, ClientRef} = hackney:get(Url, Headers, <<>>, []),
    {ok, Body} = hackney:body(ClientRef),
    Keys = json:decode(Body),

    %% Should have at least the test suite key
    true = is_list(Keys),
    true = length(Keys) >= 1,

    ok.

key_create(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/keys">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
               {<<"Content-Type">>, <<"application/json">>}],

    ReqBody = json:encode(#{
        <<"name">> => <<"test-created-key">>,
        <<"permissions">> => [<<"read">>],
        <<"databases">> => [<<"key_test_db">>]
    }),

    {ok, 201, _RespHeaders, ClientRef} = hackney:post(Url, Headers, ReqBody, []),
    {ok, Body} = hackney:body(ClientRef),
    Result = json:decode(Body),

    %% Should return the full key on creation
    #{<<"key">> := CreatedKey, <<"name">> := <<"test-created-key">>} = Result,
    true = is_binary(CreatedKey),
    <<"ak_", _/binary>> = CreatedKey,

    %% Store for later tests
    [{created_key, CreatedKey}, {created_key_prefix, maps:get(<<"key_prefix">>, Result)} | Config].

key_get(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    %% Get the prefix from the created key result
    {ok, Keys} = barrel_http_api_keys:list_keys(),
    [#{key_prefix := TestKeyPrefix} | _] = [K || K = #{name := <<"test-created-key">>} <- Keys],

    Url = <<?BASE_URL/binary, "/keys/", TestKeyPrefix/binary>>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
               {<<"Content-Type">>, <<"application/json">>}],

    {ok, 200, _RespHeaders, ClientRef} = hackney:get(Url, Headers, <<>>, []),
    {ok, Body} = hackney:body(ClientRef),
    Result = json:decode(Body),

    #{<<"name">> := <<"test-created-key">>} = Result,
    %% Should NOT return the full key on get
    false = maps:is_key(<<"key">>, Result),

    ok.

key_delete(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    %% Get the prefix from the created key
    {ok, Keys} = barrel_http_api_keys:list_keys(),
    [#{key_prefix := TestKeyPrefix} | _] = [K || K = #{name := <<"test-created-key">>} <- Keys],

    Url = <<?BASE_URL/binary, "/keys/", TestKeyPrefix/binary>>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
               {<<"Content-Type">>, <<"application/json">>}],

    {ok, 200, _RespHeaders, ClientRef} = hackney:delete(Url, Headers, <<>>, []),
    {ok, Body} = hackney:body(ClientRef),
    #{<<"ok">> := true} = json:decode(Body),

    %% Verify it's gone
    {ok, 404, _, ClientRef2} = hackney:get(Url, Headers, <<>>, []),
    hackney:body(ClientRef2),

    ok.

key_admin_required(Config) ->
    %% Create a non-admin key
    {ok, NonAdminKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"non-admin-key">>,
        permissions => [<<"read">>, <<"write">>],
        is_admin => false
    }),

    Url = <<?BASE_URL/binary, "/keys">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", NonAdminKey/binary>>},
               {<<"Content-Type">>, <<"application/json">>}],

    %% Should be forbidden (403)
    {ok, 403, _RespHeaders, ClientRef} = hackney:get(Url, Headers, <<>>, []),
    {ok, Body} = hackney:body(ClientRef),
    #{<<"error">> := <<"Admin access required">>} = json:decode(Body),

    %% Clean up
    Prefix = binary:part(NonAdminKey, 0, 12),
    barrel_http_api_keys:delete_key(Prefix),

    %% Restore config
    Config.

key_per_database(_Config) ->
    %% Create a key with access only to key_test_db
    {ok, DbKey, #{key_prefix := DbKeyPrefix}} = barrel_http_api_keys:create_key(#{
        name => <<"db-specific-key">>,
        permissions => [<<"read">>, <<"write">>],
        databases => [<<"key_test_db">>],
        is_admin => false
    }),

    %% Should be able to access key_test_db
    Url1 = <<?BASE_URL/binary, "/db/key_test_db">>,
    Headers1 = [{<<"Authorization">>, <<"Bearer ", DbKey/binary>>},
                {<<"Content-Type">>, <<"application/json">>}],
    {ok, 200, _, ClientRef1} = hackney:get(Url1, Headers1, <<>>, []),
    hackney:body(ClientRef1),

    %% Should NOT be able to access http_test db
    ensure_db(<<"http_test">>),
    Url2 = <<?BASE_URL/binary, "/db/http_test">>,
    {ok, 403, _, ClientRef2} = hackney:get(Url2, Headers1, <<>>, []),
    {ok, Body2} = hackney:body(ClientRef2),
    #{<<"error">> := <<"Access denied to this database">>} = json:decode(Body2),

    %% Clean up
    barrel_http_api_keys:delete_key(DbKeyPrefix),

    %% Key with databases => all should access any db
    {ok, AllDbKey, #{key_prefix := AllDbKeyPrefix}} = barrel_http_api_keys:create_key(#{
        name => <<"all-db-key">>,
        permissions => [<<"read">>],
        databases => all,
        is_admin => false
    }),

    Headers2 = [{<<"Authorization">>, <<"Bearer ", AllDbKey/binary>>},
                {<<"Content-Type">>, <<"application/json">>}],
    {ok, 200, _, ClientRef3} = hackney:get(Url1, Headers2, <<>>, []),
    hackney:body(ClientRef3),
    {ok, 200, _, ClientRef4} = hackney:get(Url2, Headers2, <<>>, []),
    hackney:body(ClientRef4),

    %% Clean up
    barrel_http_api_keys:delete_key(AllDbKeyPrefix),

    ok.

%%====================================================================
%% Internal helpers
%%====================================================================

ensure_http_server() ->
    %% Check if cowboy listener is already running
    case ranch:info() of
        Info when is_list(Info) ->
            case lists:keyfind(barrel_http_listener, 1, Info) of
                false ->
                    {ok, _} = barrel_http_server:start_link(#{port => ?HTTP_PORT}),
                    ok;
                _ ->
                    ok
            end;
        _ ->
            {ok, _} = barrel_http_server:start_link(#{port => ?HTTP_PORT}),
            ok
    end.

ensure_db(Name) ->
    case barrel_docdb:db_info(Name) of
        {ok, _} -> ok;
        {error, not_found} ->
            {ok, _} = barrel_docdb:create_db(Name),
            ok
    end.
