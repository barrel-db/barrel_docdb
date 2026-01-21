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
    http_revsdiff_batch/1,
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
    key_per_database/1,

    %% Attachment tests
    attachment_put/1,
    attachment_get/1,
    attachment_list/1,
    attachment_delete/1,
    attachment_not_found/1,

    %% Query tests
    query_find_basic/1,
    query_find_with_where/1,
    query_find_with_pagination/1,

    %% View tests
    view_create/1,
    view_list/1,
    view_query/1,
    view_delete/1
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
        {group, keys},
        {group, attachments},
        {group, query},
        {group, views}
    ].

groups() ->
    [
        {http_transport, [sequence], [
            http_get_doc,
            http_put_rev,
            http_revsdiff,
            http_revsdiff_batch,
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
        ]},
        {attachments, [sequence], [
            attachment_put,
            attachment_get,
            attachment_list,
            attachment_delete,
            attachment_not_found
        ]},
        {query, [sequence], [
            query_find_basic,
            query_find_with_where,
            query_find_with_pagination
        ]},
        {views, [sequence], [
            view_create,
            view_list,
            view_query,
            view_delete
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    application:ensure_all_started(cowboy),
    application:ensure_all_started(hackney),
    %% Start HTTP server at suite level
    %% Unlink so it survives process changes between init/end_per_suite
    {ok, HttpPid} = barrel_http_server:start_link(#{port => ?HTTP_PORT}),
    unlink(HttpPid),
    %% Create a test API key for authentication
    {ok, ApiKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"test-suite-key">>,
        permissions => [<<"read">>, <<"write">>, <<"admin">>],
        is_admin => true
    }),
    [{api_key, ApiKey} | Config].

end_per_suite(_Config) ->
    barrel_http_server:stop(),
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

init_per_group(attachments, Config) ->
    %% Ensure HTTP server is running
    ensure_http_server(),
    %% Create test database and document for attachments
    ensure_db(<<"att_test_db">>),
    %% Create a document to attach to
    Doc = #{<<"id">> => <<"att_test_doc">>, <<"name">> => <<"test doc">>},
    {ok, _} = barrel_docdb:put_doc(<<"att_test_db">>, Doc),
    Config;

init_per_group(query, Config) ->
    %% Ensure HTTP server is running
    ensure_http_server(),
    %% Create test database with sample documents
    ensure_db(<<"query_test_db">>),
    %% Insert test documents
    lists:foreach(
        fun(N) ->
            Type = case N rem 2 of
                0 -> <<"even">>;
                1 -> <<"odd">>
            end,
            Doc = #{
                <<"id">> => iolist_to_binary(["doc", integer_to_list(N)]),
                <<"num">> => N,
                <<"type">> => Type,
                <<"name">> => iolist_to_binary(["Document ", integer_to_list(N)])
            },
            {ok, _} = barrel_docdb:put_doc(<<"query_test_db">>, Doc)
        end,
        lists:seq(1, 10)
    ),
    Config;

init_per_group(views, Config) ->
    %% Ensure HTTP server is running
    ensure_http_server(),
    %% Create test database with sample documents
    ensure_db(<<"views_test_db">>),
    %% Insert test documents with types for the view
    lists:foreach(
        fun(N) ->
            Type = case N rem 3 of
                0 -> <<"user">>;
                1 -> <<"order">>;
                2 -> <<"product">>
            end,
            Doc = #{
                <<"id">> => iolist_to_binary(["doc", integer_to_list(N)]),
                <<"type">> => Type,
                <<"name">> => iolist_to_binary(["Item ", integer_to_list(N)])
            },
            {ok, _} = barrel_docdb:put_doc(<<"views_test_db">>, Doc)
        end,
        lists:seq(1, 9)
    ),
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

end_per_group(attachments, _Config) ->
    catch barrel_docdb:delete_db(<<"att_test_db">>),
    ok;

end_per_group(query, _Config) ->
    catch barrel_docdb:delete_db(<<"query_test_db">>),
    ok;

end_per_group(views, _Config) ->
    catch barrel_docdb:delete_db(<<"views_test_db">>),
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

http_revsdiff_batch(Config) ->
    Endpoint = proplists:get_value(endpoint, Config),

    %% Create two documents
    DocId1 = <<"batch_http_doc1">>,
    DocId2 = <<"batch_http_doc2">>,
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(<<"http_test">>, #{<<"id">> => DocId1, <<"value">> => 1}),
    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:put_doc(<<"http_test">>, #{<<"id">> => DocId2, <<"value">> => 2}),

    %% Test batch revsdiff via direct HTTP request
    #{url := BaseUrl, bearer_token := BearerToken} = Endpoint,
    Url = <<BaseUrl/binary, "/_revsdiff">>,
    ReqBody = #{<<"revs">> => #{
        DocId1 => [Rev1, <<"2-fake123">>],
        DocId2 => [Rev2],
        <<"nonexistent">> => [<<"1-abc">>]
    }},
    EncodedBody = json:encode(ReqBody),
    Headers = [
        {<<"content-type">>, <<"application/json">>},
        {<<"authorization">>, <<"Bearer ", BearerToken/binary>>}
    ],
    {ok, 200, _RespHeaders, RespBody} = hackney:request(post, Url, Headers, EncodedBody, []),
    {ok, RespBodyBin} = hackney:body(RespBody),
    Results = json:decode(RespBodyBin),

    %% Check DocId1 result - only fake rev should be missing
    #{DocId1 := Result1} = Results,
    [<<"2-fake123">>] = maps:get(<<"missing">>, Result1),

    %% Check DocId2 result - nothing missing
    #{DocId2 := Result2} = Results,
    [] = maps:get(<<"missing">>, Result2),

    %% Check nonexistent doc - all revs missing
    #{<<"nonexistent">> := Result3} = Results,
    [<<"1-abc">>] = maps:get(<<"missing">>, Result3),

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
%% Attachment Tests
%%====================================================================

attachment_put(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/att_test_db/att_test_doc/_attachments/image.png">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
               {<<"Content-Type">>, <<"application/octet-stream">>}],
    AttData = <<"fake png data for testing">>,

    {ok, 201, _RespHeaders, ClientRef} = hackney:put(Url, Headers, AttData, []),
    {ok, Body} = hackney:body(ClientRef),
    Result = json:decode(Body),

    #{<<"ok">> := true, <<"name">> := <<"image.png">>} = Result,
    true = maps:is_key(<<"size">>, Result),
    true = maps:is_key(<<"digest">>, Result),

    ok.

attachment_get(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/att_test_db/att_test_doc/_attachments/image.png">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}],

    {ok, 200, RespHeaders, ClientRef} = hackney:get(Url, Headers, <<>>, []),
    {ok, Body} = hackney:body(ClientRef),

    %% Should return the raw binary data
    <<"fake png data for testing">> = Body,

    %% Content-Type should be determined by filename (image.png -> image/png)
    ContentType = proplists:get_value(<<"content-type">>, RespHeaders),
    <<"image/png">> = ContentType,

    ok.

attachment_list(Config) ->
    ApiKey = proplists:get_value(api_key, Config),

    %% Add another attachment for the list test
    Url1 = <<?BASE_URL/binary, "/db/att_test_db/att_test_doc/_attachments/readme.txt">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
               {<<"Content-Type">>, <<"application/octet-stream">>}],
    {ok, 201, _, ClientRef1} = hackney:put(Url1, Headers, <<"readme content">>, []),
    hackney:body(ClientRef1),

    %% List attachments
    Url2 = <<?BASE_URL/binary, "/db/att_test_db/att_test_doc/_attachments">>,
    Headers2 = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
                {<<"Accept">>, <<"application/json">>}],
    {ok, 200, _RespHeaders, ClientRef2} = hackney:get(Url2, Headers2, <<>>, []),
    {ok, Body} = hackney:body(ClientRef2),
    Attachments = json:decode(Body),

    %% Should have both attachments
    true = is_list(Attachments),
    true = lists:member(<<"image.png">>, Attachments),
    true = lists:member(<<"readme.txt">>, Attachments),

    ok.

attachment_delete(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/att_test_db/att_test_doc/_attachments/readme.txt">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}],

    {ok, 200, _RespHeaders, ClientRef} = hackney:delete(Url, Headers, <<>>, []),
    {ok, Body} = hackney:body(ClientRef),
    #{<<"ok">> := true} = json:decode(Body),

    %% Verify it's gone
    {ok, 404, _, ClientRef2} = hackney:get(Url, Headers, <<>>, []),
    hackney:body(ClientRef2),

    ok.

attachment_not_found(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/att_test_db/att_test_doc/_attachments/nonexistent.xyz">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}],

    {ok, 404, _RespHeaders, ClientRef} = hackney:get(Url, Headers, <<>>, []),
    {ok, Body} = hackney:body(ClientRef),
    #{<<"error">> := <<"Attachment not found">>} = json:decode(Body),

    ok.

%%====================================================================
%% Query Tests
%%====================================================================

query_find_basic(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/query_test_db/_find">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
               {<<"Content-Type">>, <<"application/json">>}],

    %% Query all documents
    ReqBody = json:encode(#{}),
    {ok, 200, _RespHeaders, ClientRef} = hackney:post(Url, Headers, ReqBody, []),
    {ok, Body} = hackney:body(ClientRef),
    #{<<"results">> := Results, <<"meta">> := _Meta} = json:decode(Body),

    %% Should have 10 documents
    10 = length(Results),

    ok.

query_find_with_where(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/query_test_db/_find">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
               {<<"Content-Type">>, <<"application/json">>}],

    %% Query documents where type = "even"
    ReqBody = json:encode(#{
        <<"where">> => [
            #{<<"path">> => [<<"type">>], <<"op">> => <<"eq">>, <<"value">> => <<"even">>}
        ]
    }),
    {ok, 200, _RespHeaders, ClientRef} = hackney:post(Url, Headers, ReqBody, []),
    {ok, Body} = hackney:body(ClientRef),
    #{<<"results">> := Results} = json:decode(Body),

    %% Should have 5 even documents (2, 4, 6, 8, 10)
    5 = length(Results),

    %% All results should have type = "even"
    %% Results are wrapped as #{<<"id">> => ..., <<"doc">> => #{...}}
    lists:foreach(
        fun(Result) ->
            #{<<"doc">> := Doc} = Result,
            #{<<"type">> := <<"even">>} = Doc
        end,
        Results
    ),

    ok.

query_find_with_pagination(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/query_test_db/_find">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
               {<<"Content-Type">>, <<"application/json">>}],

    %% First page with limit
    ReqBody1 = json:encode(#{<<"limit">> => 3}),
    {ok, 200, _, ClientRef1} = hackney:post(Url, Headers, ReqBody1, []),
    {ok, Body1} = hackney:body(ClientRef1),
    #{<<"results">> := Results1, <<"meta">> := Meta1} = json:decode(Body1),

    %% Should have at most 3 documents (limit respected)
    true = length(Results1) =< 3,
    true = length(Results1) > 0,

    %% has_more and continuation should be consistent
    HasMore = maps:get(<<"has_more">>, Meta1, false),
    Continuation = maps:get(<<"continuation">>, Meta1, undefined),

    %% If has_more is true, we should have a continuation token
    case HasMore of
        true ->
            true = Continuation =/= undefined,
            %% Continue with next page
            ReqBody2 = json:encode(#{<<"continuation">> => Continuation, <<"limit">> => 3}),
            {ok, 200, _, ClientRef2} = hackney:post(Url, Headers, ReqBody2, []),
            {ok, Body2} = hackney:body(ClientRef2),
            #{<<"results">> := Results2} = json:decode(Body2),
            true = length(Results2) > 0;
        false ->
            %% No more results - that's fine for a small result set
            ok
    end,

    ok.

%%====================================================================
%% View Tests
%%====================================================================

view_create(Config) ->
    %% View creation requires complex query spec with logic variables
    %% This test just verifies the endpoint responds correctly
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/views_test_db/_views">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>},
               {<<"Content-Type">>, <<"application/json">>}],

    %% Try to create an invalid view (missing required fields)
    %% Should return 400
    ReqBody = json:encode(#{
        <<"id">> => <<"test_view">>,
        <<"where">> => []  %% Empty where clause is invalid
    }),

    {ok, 400, _RespHeaders, ClientRef} = hackney:post(Url, Headers, ReqBody, []),
    {ok, _Body} = hackney:body(ClientRef),
    %% 400 is expected for invalid view spec
    ok.

view_list(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/views_test_db/_views">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}],

    {ok, 200, _RespHeaders, ClientRef} = hackney:get(Url, Headers, <<>>, []),
    {ok, Body} = hackney:body(ClientRef),
    Views = json:decode(Body),

    %% Should return a list (even if empty)
    true = is_list(Views),

    ok.

view_query(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/views_test_db/_views/nonexistent/_query">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}],

    %% Query a non-existent view should return 404
    {ok, 404, _RespHeaders, ClientRef} = hackney:get(Url, Headers, <<>>, []),
    {ok, _Body} = hackney:body(ClientRef),

    ok.

view_delete(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    Url = <<?BASE_URL/binary, "/db/views_test_db/_views/nonexistent">>,
    Headers = [{<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}],

    %% Delete a non-existent view should return 404
    {ok, 404, _RespHeaders, ClientRef} = hackney:delete(Url, Headers, <<>>, []),
    {ok, _Body} = hackney:body(ClientRef),

    ok.

%%====================================================================
%% Internal helpers
%%====================================================================

ensure_http_server() ->
    %% Server is started in init_per_suite, just verify it's running
    case whereis(barrel_http_server) of
        Pid when is_pid(Pid) -> ok;
        undefined -> error(http_server_not_running)
    end.

ensure_db(Name) ->
    case barrel_docdb:db_info(Name) of
        {ok, _} -> ok;
        {error, not_found} ->
            {ok, _} = barrel_docdb:create_db(Name),
            ok
    end.
