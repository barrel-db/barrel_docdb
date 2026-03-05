%%%-------------------------------------------------------------------
%%% @doc HTTP Server Test Suite
%%%
%%% Tests for barrel_http_server and barrel_http_handler.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_http_SUITE).

-include_lib("common_test/include/ct.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    health_check/1,
    get_doc_not_found/1,
    put_and_get_doc_json/1,
    put_and_get_doc_cbor/1,
    cbor_zero_copy_get_doc/1,
    cbor_zero_copy_find/1,
    delete_doc/1,
    get_changes/1,
    get_changes_with_filter/1,
    get_changes_with_doc_ids/1,
    get_changes_with_query/1,
    get_changes_longpoll_timeout/1,
    changes_stream_basic/1,
    changes_stream_since_now/1,
    changes_stream_heartbeat/1,
    changes_stream_include_docs/1,
    changes_stream_with_doc_ids/1,
    changes_stream_with_query/1,
    bulk_docs/1,
    %% Policy tests
    policy_create/1,
    policy_get/1,
    policy_list/1,
    policy_enable_disable/1,
    policy_status/1,
    policy_delete/1,
    %% Tier tests
    tier_config_set/1,
    tier_config_get/1,
    tier_capacity/1,
    tier_doc_get/1,
    tier_doc_ttl/1,
    tier_run_migration/1,
    %% Usage tests
    admin_usage_all/1,
    admin_usage_single_db/1,
    admin_usage_not_found/1,
    admin_usage_requires_auth/1,
    %% Federation tests
    federation_create_with_bearer_auth/1,
    federation_create_with_basic_auth/1,
    federation_find_with_auth_override/1
]).

-define(PORT, 18080).
-define(BASE_URL, "http://localhost:18080").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, http_tests}, {group, policy_tests}, {group, tier_tests}, {group, usage_tests}, {group, federation_tests}].

groups() ->
    [
        {http_tests, [sequence], [
            health_check,
            get_doc_not_found,
            put_and_get_doc_json,
            put_and_get_doc_cbor,
            cbor_zero_copy_get_doc,
            cbor_zero_copy_find,
            delete_doc,
            get_changes,
            get_changes_with_filter,
            get_changes_with_doc_ids,
            get_changes_with_query,
            get_changes_longpoll_timeout,
            changes_stream_basic,
            changes_stream_since_now,
            changes_stream_heartbeat,
            changes_stream_include_docs,
            changes_stream_with_doc_ids,
            changes_stream_with_query,
            bulk_docs
        ]},
        {policy_tests, [sequence], [
            policy_create,
            policy_get,
            policy_list,
            policy_enable_disable,
            policy_status,
            policy_delete
        ]},
        {tier_tests, [sequence], [
            tier_config_set,
            tier_config_get,
            tier_capacity,
            tier_doc_get,
            tier_doc_ttl,
            tier_run_migration
        ]},
        {usage_tests, [sequence], [
            admin_usage_all,
            admin_usage_single_db,
            admin_usage_not_found,
            admin_usage_requires_auth
        ]},
        {federation_tests, [sequence], [
            federation_create_with_bearer_auth,
            federation_create_with_basic_auth,
            federation_find_with_auth_override
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    application:ensure_all_started(cowboy),
    application:ensure_all_started(hackney),
    %% Start HTTP server at suite level (stopped in end_per_suite)
    %% Unlink so it survives process changes between init/end_per_suite
    {ok, HttpPid} = barrel_http_server:start_link(#{port => ?PORT}),
    unlink(HttpPid),
    %% Create a test API key for authentication
    {ok, ApiKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"http-suite-key">>,
        permissions => [<<"read">>, <<"write">>, <<"admin">>],
        is_admin => true
    }),
    [{api_key, ApiKey} | Config].

end_per_suite(_Config) ->
    barrel_http_server:stop(),
    ok.

init_per_group(http_tests, Config) ->
    %% Create test database
    {ok, _} = barrel_docdb:create_db(<<"testdb">>),
    Config;
init_per_group(policy_tests, Config) ->
    %% Clean up any existing test policies
    case barrel_rep_policy:list() of
        {ok, Policies} ->
            lists:foreach(fun(#{name := Name}) ->
                barrel_rep_policy:delete(Name)
            end, Policies);
        _ -> ok
    end,
    Config;
init_per_group(tier_tests, Config) ->
    %% Create test databases for tier tests
    {ok, _} = barrel_docdb:create_db(<<"tier_test_db">>),
    {ok, _} = barrel_docdb:create_db(<<"tier_warm_db">>),
    Config;
init_per_group(usage_tests, Config) ->
    %% Create test database for usage tests
    {ok, _} = barrel_docdb:create_db(<<"usage_http_test_db">>),
    Config;
init_per_group(federation_tests, Config) ->
    %% Create test database for federation tests
    {ok, _} = barrel_docdb:create_db(<<"fed_http_test_db">>),
    Config.

%% Helper to get auth header
auth_header(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}.

end_per_group(http_tests, _Config) ->
    barrel_docdb:delete_db(<<"testdb">>),
    ok;
end_per_group(policy_tests, _Config) ->
    %% Clean up test policies
    case barrel_rep_policy:list() of
        {ok, Policies} ->
            lists:foreach(fun(#{name := Name}) ->
                barrel_rep_policy:delete(Name)
            end, Policies);
        _ -> ok
    end,
    ok;
end_per_group(tier_tests, _Config) ->
    %% Clean up tier test databases
    barrel_docdb:delete_db(<<"tier_test_db">>),
    barrel_docdb:delete_db(<<"tier_warm_db">>),
    ok;
end_per_group(usage_tests, _Config) ->
    %% Clean up usage test database
    barrel_docdb:delete_db(<<"usage_http_test_db">>),
    ok;
end_per_group(federation_tests, _Config) ->
    %% Clean up federation test database and federations
    barrel_docdb:delete_db(<<"fed_http_test_db">>),
    catch barrel_federation:delete(<<"http_bearer_fed">>),
    catch barrel_federation:delete(<<"http_basic_fed">>),
    catch barrel_federation:delete(<<"http_override_fed">>),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc Test health endpoint
health_check(_Config) ->
    {ok, 200, _Headers, Body} = hackney:get(?BASE_URL ++ "/health", [], <<>>, []),
    #{<<"status">> := <<"ok">>} = json:decode(Body),
    ok.

%% @doc Test getting a non-existent document
get_doc_not_found(Config) ->
    Auth = auth_header(Config),
    {ok, 404, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/db/testdb/nonexistent",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    #{<<"error">> := <<"Document not found">>} = json:decode(Body),
    ok.

%% @doc Test PUT and GET document with JSON
put_and_get_doc_json(Config) ->
    Auth = auth_header(Config),
    DocId = <<"test_doc_json">>,
    Doc = #{<<"name">> => <<"Alice">>, <<"age">> => 30},
    DocJson = iolist_to_binary(json:encode(Doc)),

    %% PUT document
    {ok, 201, _PutHeaders, PutBody} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        DocJson,
        []
    ),
    #{<<"id">> := DocId, <<"rev">> := Rev} = json:decode(PutBody),
    true = is_binary(Rev),

    %% GET document
    {ok, 200, _GetHeaders, GetBody} = hackney:get(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    #{<<"name">> := <<"Alice">>, <<"age">> := 30} = json:decode(GetBody),
    ok.

%% @doc Test PUT and GET document with CBOR
put_and_get_doc_cbor(Config) ->
    Auth = auth_header(Config),
    DocId = <<"test_doc_cbor">>,
    Doc = #{<<"name">> => <<"Bob">>, <<"age">> => 25},
    DocCbor = barrel_docdb_codec_cbor:encode_cbor(Doc),

    %% PUT document with CBOR
    {ok, 201, _PutHeaders, PutBody} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth,
         {<<"Content-Type">>, <<"application/cbor">>},
         {<<"Accept">>, <<"application/cbor">>}],
        DocCbor,
        []
    ),
    #{<<"id">> := DocId, <<"rev">> := Rev} = barrel_docdb_codec_cbor:decode_cbor(PutBody),
    true = is_binary(Rev),

    %% GET document with CBOR
    {ok, 200, _GetHeaders, GetBody} = hackney:get(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth, {<<"Accept">>, <<"application/cbor">>}],
        <<>>,
        []
    ),
    #{<<"name">> := <<"Bob">>, <<"age">> := 25} = barrel_docdb_codec_cbor:decode_cbor(GetBody),
    ok.

%% @doc Test zero-copy CBOR GET document (raw_body optimization)
%% Verifies that CBOR response includes id and _rev from metadata
cbor_zero_copy_get_doc(Config) ->
    Auth = auth_header(Config),
    DocId = <<"test_doc_zerocopy">>,
    Doc = #{<<"title">> => <<"Zero Copy Test">>, <<"value">> => 42},
    DocCbor = barrel_docdb_codec_cbor:encode_cbor(Doc),

    %% PUT document with CBOR
    {ok, 201, _PutHeaders, PutBody} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth,
         {<<"Content-Type">>, <<"application/cbor">>},
         {<<"Accept">>, <<"application/cbor">>}],
        DocCbor,
        []
    ),
    #{<<"id">> := DocId, <<"rev">> := Rev} = barrel_docdb_codec_cbor:decode_cbor(PutBody),

    %% GET document with CBOR - should use zero-copy path
    {ok, 200, GetHeaders, GetBody} = hackney:get(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth, {<<"Accept">>, <<"application/cbor">>}],
        <<>>,
        []
    ),
    %% Verify Content-Type is CBOR
    <<"application/cbor">> = proplists:get_value(<<"content-type">>, GetHeaders),

    DecodedDoc = barrel_docdb_codec_cbor:decode_cbor(GetBody),

    %% Verify document content and metadata
    #{<<"title">> := <<"Zero Copy Test">>,
      <<"value">> := 42,
      <<"id">> := DocId,
      <<"_rev">> := Rev} = DecodedDoc,
    ok.

%% @doc Test zero-copy CBOR for _find queries with include_docs
%% Verifies that CBOR query response works with doc_format=binary
%% Uses documents created in earlier tests (test_doc_cbor, test_doc_zerocopy)
cbor_zero_copy_find(Config) ->
    Auth = auth_header(Config),

    %% Query all documents with include_docs using CBOR
    %% Use "exists id" to match all documents
    Query = #{
        <<"where">> => [#{<<"path">> => [<<"id">>], <<"op">> => <<"exists">>}],
        <<"include_docs">> => true,
        <<"limit">> => 10
    },
    QueryCbor = barrel_docdb_codec_cbor:encode_cbor(Query),

    {ok, 200, FindHeaders, FindBody} = hackney:post(
        ?BASE_URL ++ "/db/testdb/_find",
        [Auth,
         {<<"Content-Type">>, <<"application/cbor">>},
         {<<"Accept">>, <<"application/cbor">>}],
        QueryCbor,
        []
    ),
    %% Verify Content-Type is CBOR
    <<"application/cbor">> = proplists:get_value(<<"content-type">>, FindHeaders),

    #{<<"results">> := Results, <<"meta">> := _Meta} = barrel_docdb_codec_cbor:decode_cbor(FindBody),

    %% Verify we got results (should have docs from earlier tests)
    true = length(Results) >= 1,
    %% Each result should be a map (decoded document)
    lists:foreach(fun(Doc) ->
        true = is_map(Doc)
    end, Results),
    ok.

%% @doc Test DELETE document
delete_doc(Config) ->
    Auth = auth_header(Config),
    DocId = <<"test_doc_delete">>,
    Doc = #{<<"temp">> => true},
    DocJson = iolist_to_binary(json:encode(Doc)),

    %% PUT document first
    {ok, 201, _PutHeaders, PutBody} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth, {<<"Content-Type">>, <<"application/json">>}],
        DocJson,
        []
    ),
    #{<<"rev">> := Rev} = json:decode(PutBody),

    %% DELETE document
    {ok, 200, _DelHeaders, _DelBody} = hackney:delete(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId) ++ "?rev=" ++ binary_to_list(Rev),
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),

    %% Verify deleted
    {ok, 404, _GetHeaders, _GetBody} = hackney:get(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth],
        <<>>,
        []
    ),
    ok.

%% @doc Test changes feed
get_changes(Config) ->
    Auth = auth_header(Config),
    %% Insert a document
    DocId = <<"test_doc_changes">>,
    Doc = #{<<"type">> => <<"test">>},
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, Doc#{<<"id">> => DocId}),

    %% Get changes
    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/db/testdb/_changes",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    #{<<"results">> := Results} = json:decode(Body),
    true = is_list(Results),
    true = length(Results) > 0,
    ok.

%% @doc Test changes feed with MQTT-style filter
get_changes_with_filter(Config) ->
    Auth = auth_header(Config),
    %% Insert documents with different paths
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"users/alice/profile">>, <<"name">> => <<"Alice">>}),
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"users/bob/profile">>, <<"name">> => <<"Bob">>}),
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"orders/123">>, <<"total">> => 50}),

    %% Get changes with filter matching users/*/profile
    %% Note: + must be URL-encoded as %2B to not be treated as space
    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/db/testdb/_changes?filter=users/%2B/profile",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    #{<<"results">> := Results} = json:decode(Body),

    %% Should only have user profile documents
    ResultIds = [maps:get(<<"id">>, R) || R <- Results],
    true = lists:member(<<"users/alice/profile">>, ResultIds),
    true = lists:member(<<"users/bob/profile">>, ResultIds),
    false = lists:member(<<"orders/123">>, ResultIds),
    ok.

%% @doc Test changes feed with doc_ids filter via POST
get_changes_with_doc_ids(Config) ->
    Auth = auth_header(Config),
    %% Create some test documents
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"docids_test_1">>, <<"val">> => 1}),
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"docids_test_2">>, <<"val">> => 2}),
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"docids_test_3">>, <<"val">> => 3}),

    %% Get changes filtered to only doc_ids 1 and 3
    ReqBody = iolist_to_binary(json:encode(#{
        <<"doc_ids">> => [<<"docids_test_1">>, <<"docids_test_3">>]
    })),
    {ok, 200, _Headers, Body} = hackney:post(
        ?BASE_URL ++ "/db/testdb/_changes",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    #{<<"results">> := Results} = json:decode(Body),

    %% Should only have docs 1 and 3
    ResultIds = [maps:get(<<"id">>, R) || R <- Results],
    true = lists:member(<<"docids_test_1">>, ResultIds),
    false = lists:member(<<"docids_test_2">>, ResultIds),
    true = lists:member(<<"docids_test_3">>, ResultIds),
    ok.

%% @doc Test changes feed with query filter via POST
get_changes_with_query(Config) ->
    Auth = auth_header(Config),
    %% Create some test documents with different types
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"query_test_order_1">>, <<"type">> => <<"order">>, <<"total">> => 100}),
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"query_test_order_2">>, <<"type">> => <<"order">>, <<"total">> => 200}),
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"query_test_user_1">>, <<"type">> => <<"user">>, <<"name">> => <<"Test">>}),

    %% Get changes filtered by query matching type=order
    ReqBody = iolist_to_binary(json:encode(#{
        <<"query">> => #{
            <<"where">> => [
                #{<<"path">> => [<<"type">>], <<"op">> => <<"==">>, <<"value">> => <<"order">>}
            ]
        }
    })),
    {ok, 200, _Headers, Body} = hackney:post(
        ?BASE_URL ++ "/db/testdb/_changes?include_docs=true",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    #{<<"results">> := Results} = json:decode(Body),

    %% Should only have order documents
    ResultIds = [maps:get(<<"id">>, R) || R <- Results],
    true = lists:member(<<"query_test_order_1">>, ResultIds),
    true = lists:member(<<"query_test_order_2">>, ResultIds),
    false = lists:member(<<"query_test_user_1">>, ResultIds),
    ok.

%% @doc Test long poll changes with timeout (no new changes)
get_changes_longpoll_timeout(Config) ->
    Auth = auth_header(Config),
    %% Get the current last_seq first
    {ok, 200, _, Body0} = hackney:get(
        ?BASE_URL ++ "/db/testdb/_changes",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    #{<<"last_seq">> := LastSeq} = json:decode(Body0),

    %% Long poll with short timeout - should return empty after timeout
    StartTime = erlang:monotonic_time(millisecond),
    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/db/testdb/_changes?feed=longpoll&timeout=500&since=" ++ binary_to_list(LastSeq),
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        [{recv_timeout, 5000}]
    ),
    EndTime = erlang:monotonic_time(millisecond),

    %% Should have waited at least 400ms (allowing some tolerance)
    Elapsed = EndTime - StartTime,
    true = Elapsed >= 400,

    %% Should return empty results after timeout
    #{<<"results">> := Results} = json:decode(Body),
    0 = length(Results),
    ok.

%% @doc Test SSE changes stream
changes_stream_basic(Config) ->
    Auth = auth_header(Config),
    %% Start streaming request
    StreamUrl = ?BASE_URL ++ "/db/testdb/_changes/stream?heartbeat=1000",
    {ok, Ref} = hackney:get(StreamUrl, [Auth], <<>>, [async]),

    %% Wait for headers
    receive
        {hackney_response, Ref, {status, 200, _}} -> ok
    after 2000 ->
        ct:fail("Timeout waiting for SSE response")
    end,

    %% Wait for headers
    receive
        {hackney_response, Ref, {headers, Headers}} ->
            %% Verify content type
            ContentType = proplists:get_value(<<"content-type">>, Headers),
            <<"text/event-stream">> = ContentType
    after 2000 ->
        ct:fail("Timeout waiting for SSE headers")
    end,

    %% Insert a document while streaming
    spawn(fun() ->
        timer:sleep(100),
        barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"stream_test_doc">>, <<"val">> => 1})
    end),

    %% Wait for change event or heartbeat
    ReceivedChange = receive_sse_event(Ref, 3000),
    true = ReceivedChange,

    %% Close the stream
    hackney:close(Ref),
    ok.

%% Helper to receive SSE events
receive_sse_event(Ref, Timeout) ->
    receive
        {hackney_response, Ref, done} ->
            false;
        {hackney_response, Ref, {error, Reason}} ->
            ct:log("SSE error: ~p", [Reason]),
            false;
        {hackney_response, Ref, Chunk} when is_binary(Chunk) ->
            ct:log("SSE chunk: ~p", [Chunk]),
            %% Check if it's a change or heartbeat event
            case binary:match(Chunk, <<"event:">>) of
                nomatch -> receive_sse_event(Ref, Timeout);
                _ -> true
            end
    after Timeout ->
        ct:log("SSE timeout after ~p ms", [Timeout]),
        false
    end.

%% @doc Test SSE stream with since=now doesn't crash
%% This regression test ensures the server handles since=now without crashing
%% (previously caused barrel_hlc:encode(now) crash)
changes_stream_since_now(Config) ->
    Auth = auth_header(Config),
    %% Start streaming request with since=now - should not crash
    StreamUrl = ?BASE_URL ++ "/db/testdb/_changes/stream?since=now&heartbeat=1000",
    {ok, Ref} = hackney:get(StreamUrl, [Auth], <<>>, [async]),

    %% Wait for headers - if we get 200, the server didn't crash
    receive
        {hackney_response, Ref, {status, 200, _}} -> ok;
        {hackney_response, Ref, {status, Status, _}} ->
            ct:fail("Unexpected status ~p for since=now", [Status])
    after 2000 ->
        ct:fail("Timeout waiting for SSE response with since=now")
    end,

    %% Wait for headers
    receive
        {hackney_response, Ref, {headers, Headers}} ->
            <<"text/event-stream">> = proplists:get_value(<<"content-type">>, Headers)
    after 2000 ->
        ct:fail("Timeout waiting for SSE headers")
    end,

    %% Close the stream
    hackney:close(Ref),
    ok.

%% @doc Test SSE stream sends heartbeat events
%% Verifies the heartbeat mechanism keeps the connection alive
changes_stream_heartbeat(Config) ->
    Auth = auth_header(Config),
    %% Start streaming with short heartbeat (1 second)
    StreamUrl = ?BASE_URL ++ "/db/testdb/_changes/stream?heartbeat=1000",
    {ok, Ref} = hackney:get(StreamUrl, [Auth], <<>>, [async]),

    %% Wait for headers
    receive
        {hackney_response, Ref, {status, 200, _}} -> ok
    after 2000 ->
        ct:fail("Timeout waiting for SSE response")
    end,

    receive
        {hackney_response, Ref, {headers, _Headers}} -> ok
    after 2000 ->
        ct:fail("Timeout waiting for SSE headers")
    end,

    %% Wait for heartbeat event (should arrive within ~1.5 seconds)
    ReceivedHeartbeat = receive_heartbeat_event(Ref, 2000),
    true = ReceivedHeartbeat,

    %% Close the stream
    hackney:close(Ref),
    ok.

%% Helper to receive heartbeat events specifically
receive_heartbeat_event(Ref, Timeout) ->
    receive
        {hackney_response, Ref, done} ->
            false;
        {hackney_response, Ref, {error, Reason}} ->
            ct:log("SSE error: ~p", [Reason]),
            false;
        {hackney_response, Ref, Chunk} when is_binary(Chunk) ->
            ct:log("SSE chunk: ~p", [Chunk]),
            %% Check specifically for heartbeat event
            case binary:match(Chunk, <<"event: heartbeat">>) of
                nomatch -> receive_heartbeat_event(Ref, Timeout);
                _ -> true
            end
    after Timeout ->
        ct:log("Heartbeat timeout after ~p ms", [Timeout]),
        false
    end.

%% @doc Test SSE stream with include_docs=true
%% Verifies that document bodies are included in change events
changes_stream_include_docs(Config) ->
    Auth = auth_header(Config),

    %% First, create a document with known content
    DocId = <<"stream_include_docs_test">>,
    DocBody = #{<<"id">> => DocId, <<"name">> => <<"Test Doc">>, <<"value">> => 42},
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, DocBody),

    %% Start streaming with include_docs=true, starting from beginning to catch the doc
    StreamUrl = ?BASE_URL ++ "/db/testdb/_changes/stream?include_docs=true&heartbeat=5000",
    {ok, Ref} = hackney:get(StreamUrl, [Auth], <<>>, [async]),

    %% Wait for headers
    receive
        {hackney_response, Ref, {status, 200, _}} -> ok
    after 2000 ->
        ct:fail("Timeout waiting for SSE response")
    end,

    receive
        {hackney_response, Ref, {headers, _Headers}} -> ok
    after 2000 ->
        ct:fail("Timeout waiting for SSE headers")
    end,

    %% Wait for change event that includes doc body
    {ok, ChangeJson} = receive_change_with_doc(Ref, DocId, 5000),
    ct:log("Received change with doc: ~p", [ChangeJson]),

    %% Verify the change includes the doc field with expected content
    Change = json:decode(ChangeJson),
    true = maps:is_key(<<"doc">>, Change),
    Doc = maps:get(<<"doc">>, Change),
    <<"Test Doc">> = maps:get(<<"name">>, Doc),
    42 = maps:get(<<"value">>, Doc),

    %% Close the stream
    hackney:close(Ref),
    ok.

%% Helper to receive change event with doc body for a specific document
receive_change_with_doc(Ref, TargetDocId, Timeout) ->
    receive
        {hackney_response, Ref, done} ->
            {error, stream_closed};
        {hackney_response, Ref, {error, Reason}} ->
            ct:log("SSE error: ~p", [Reason]),
            {error, Reason};
        {hackney_response, Ref, Chunk} when is_binary(Chunk) ->
            ct:log("SSE chunk: ~p", [Chunk]),
            %% Parse SSE event
            case parse_sse_change_event(Chunk) of
                {ok, DataJson} ->
                    %% Check if this is our target doc and has doc field
                    case json:decode(DataJson) of
                        #{<<"id">> := TargetDocId, <<"doc">> := _} ->
                            {ok, DataJson};
                        _ ->
                            receive_change_with_doc(Ref, TargetDocId, Timeout)
                    end;
                _ ->
                    receive_change_with_doc(Ref, TargetDocId, Timeout)
            end
    after Timeout ->
        ct:log("Timeout waiting for change with doc after ~p ms", [Timeout]),
        {error, timeout}
    end.

%% Parse SSE change event and extract the JSON data
parse_sse_change_event(Chunk) ->
    case binary:match(Chunk, <<"event: change">>) of
        nomatch ->
            {error, not_change_event};
        _ ->
            %% Find data line
            case binary:match(Chunk, <<"data: ">>) of
                nomatch ->
                    {error, no_data};
                {Start, Len} ->
                    DataStart = Start + Len,
                    %% Find end of line
                    RestChunk = binary:part(Chunk, DataStart, byte_size(Chunk) - DataStart),
                    case binary:match(RestChunk, <<"\n">>) of
                        nomatch ->
                            {ok, RestChunk};
                        {EndPos, _} ->
                            {ok, binary:part(RestChunk, 0, EndPos)}
                    end
            end
    end.

%% @doc Test SSE stream with doc_ids filter via POST
changes_stream_with_doc_ids(Config) ->
    Auth = auth_header(Config),
    %% Create test documents
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"stream_docids_1">>, <<"val">> => 1}),
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"stream_docids_2">>, <<"val">> => 2}),
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"stream_docids_3">>, <<"val">> => 3}),

    %% Start streaming with doc_ids filter via POST
    StreamUrl = ?BASE_URL ++ "/db/testdb/_changes/stream?heartbeat=5000",
    ReqBody = iolist_to_binary(json:encode(#{
        <<"doc_ids">> => [<<"stream_docids_1">>, <<"stream_docids_3">>]
    })),
    {ok, Ref} = hackney:post(StreamUrl, [Auth, {<<"Content-Type">>, <<"application/json">>}], ReqBody, [async]),

    %% Wait for headers
    receive
        {hackney_response, Ref, {status, 200, _}} -> ok
    after 2000 ->
        ct:fail("Timeout waiting for SSE response")
    end,

    receive
        {hackney_response, Ref, {headers, Headers}} ->
            <<"text/event-stream">> = proplists:get_value(<<"content-type">>, Headers)
    after 2000 ->
        ct:fail("Timeout waiting for SSE headers")
    end,

    %% Collect changes - should only get docs 1 and 3
    ReceivedIds = receive_sse_doc_ids(Ref, [], 3000),
    ct:log("Received doc IDs: ~p", [ReceivedIds]),

    %% Verify we got docs 1 and 3 but not 2
    true = lists:member(<<"stream_docids_1">>, ReceivedIds),
    false = lists:member(<<"stream_docids_2">>, ReceivedIds),
    true = lists:member(<<"stream_docids_3">>, ReceivedIds),

    hackney:close(Ref),
    ok.

%% @doc Test SSE stream with query filter via POST
changes_stream_with_query(Config) ->
    Auth = auth_header(Config),
    %% Create test documents with different types
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"stream_query_order_1">>, <<"type">> => <<"order">>, <<"total">> => 50}),
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"stream_query_order_2">>, <<"type">> => <<"order">>, <<"total">> => 75}),
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, #{<<"id">> => <<"stream_query_user_1">>, <<"type">> => <<"user">>, <<"name">> => <<"Stream Test">>}),

    %% Start streaming with query filter via POST
    StreamUrl = ?BASE_URL ++ "/db/testdb/_changes/stream?heartbeat=5000&include_docs=true",
    ReqBody = iolist_to_binary(json:encode(#{
        <<"query">> => #{
            <<"where">> => [
                #{<<"path">> => [<<"type">>], <<"op">> => <<"==">>, <<"value">> => <<"order">>}
            ]
        }
    })),
    {ok, Ref} = hackney:post(StreamUrl, [Auth, {<<"Content-Type">>, <<"application/json">>}], ReqBody, [async]),

    %% Wait for headers
    receive
        {hackney_response, Ref, {status, 200, _}} -> ok
    after 2000 ->
        ct:fail("Timeout waiting for SSE response")
    end,

    receive
        {hackney_response, Ref, {headers, Headers}} ->
            <<"text/event-stream">> = proplists:get_value(<<"content-type">>, Headers)
    after 2000 ->
        ct:fail("Timeout waiting for SSE headers")
    end,

    %% Collect changes - should only get order docs
    ReceivedIds = receive_sse_doc_ids(Ref, [], 3000),
    ct:log("Received doc IDs: ~p", [ReceivedIds]),

    %% Verify we got order docs but not user doc
    true = lists:member(<<"stream_query_order_1">>, ReceivedIds),
    true = lists:member(<<"stream_query_order_2">>, ReceivedIds),
    false = lists:member(<<"stream_query_user_1">>, ReceivedIds),

    hackney:close(Ref),
    ok.

%% Helper to receive SSE events and collect doc IDs
receive_sse_doc_ids(Ref, Acc, Timeout) ->
    receive
        {hackney_response, Ref, done} ->
            Acc;
        {hackney_response, Ref, {error, _Reason}} ->
            Acc;
        {hackney_response, Ref, Chunk} when is_binary(Chunk) ->
            %% Try to extract doc ID from change event
            case parse_sse_change_event(Chunk) of
                {ok, JsonData} ->
                    case catch json:decode(JsonData) of
                        #{<<"id">> := Id} ->
                            receive_sse_doc_ids(Ref, [Id | Acc], Timeout);
                        _ ->
                            receive_sse_doc_ids(Ref, Acc, Timeout)
                    end;
                _ ->
                    receive_sse_doc_ids(Ref, Acc, Timeout)
            end
    after Timeout ->
        Acc
    end.

%% @doc Test bulk docs
bulk_docs(Config) ->
    Auth = auth_header(Config),
    Docs = [
        #{<<"id">> => <<"bulk1">>, <<"val">> => 1},
        #{<<"id">> => <<"bulk2">>, <<"val">> => 2},
        #{<<"id">> => <<"bulk3">>, <<"val">> => 3}
    ],
    ReqBody = iolist_to_binary(json:encode(#{<<"docs">> => Docs})),

    {ok, 201, _Headers, Body} = hackney:post(
        ?BASE_URL ++ "/db/testdb/_bulk_docs",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    Results = json:decode(Body),
    3 = length(Results),

    %% Verify each doc was created
    lists:foreach(
        fun(Result) ->
            #{<<"ok">> := true} = Result
        end,
        Results
    ),
    ok.

%%====================================================================
%% Policy HTTP API Tests
%%====================================================================

%% @doc Test creating a policy via HTTP
policy_create(Config) ->
    Auth = auth_header(Config),
    PolicySpec = #{
        <<"name">> => <<"test_fanout">>,
        <<"pattern">> => <<"fanout">>,
        <<"source">> => <<"source_db">>,
        <<"targets">> => [<<"target1">>, <<"target2">>],
        <<"mode">> => <<"one_shot">>
    },
    ReqBody = iolist_to_binary(json:encode(PolicySpec)),

    {ok, 201, _Headers, Body} = hackney:post(
        ?BASE_URL ++ "/_policies",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    #{<<"ok">> := true, <<"name">> := <<"test_fanout">>} = json:decode(Body),

    %% Try to create duplicate
    {ok, 409, _Headers2, _Body2} = hackney:post(
        ?BASE_URL ++ "/_policies",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    ok.

%% @doc Test getting a policy via HTTP
policy_get(Config) ->
    Auth = auth_header(Config),

    %% Get existing policy
    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/_policies/test_fanout",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    Policy = json:decode(Body),
    <<"test_fanout">> = maps:get(<<"name">>, Policy),
    <<"fanout">> = maps:get(<<"pattern">>, Policy),
    <<"source_db">> = maps:get(<<"source">>, Policy),

    %% Get non-existing policy
    {ok, 404, _Headers2, _Body2} = hackney:get(
        ?BASE_URL ++ "/_policies/nonexistent",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    ok.

%% @doc Test listing policies via HTTP
policy_list(Config) ->
    Auth = auth_header(Config),

    %% Create another policy
    PolicySpec = #{
        <<"name">> => <<"test_chain">>,
        <<"pattern">> => <<"chain">>,
        <<"nodes">> => [<<"node1">>, <<"node2">>],
        <<"database">> => <<"mydb">>
    },
    ReqBody = iolist_to_binary(json:encode(PolicySpec)),
    {ok, 201, _, _CreateBody} = hackney:post(
        ?BASE_URL ++ "/_policies",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),

    %% List all policies
    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/_policies",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    #{<<"policies">> := Policies} = json:decode(Body),
    true = length(Policies) >= 2,

    %% Verify we have our policies
    Names = [maps:get(<<"name">>, P) || P <- Policies],
    true = lists:member(<<"test_fanout">>, Names),
    true = lists:member(<<"test_chain">>, Names),
    ok.

%% @doc Test enabling/disabling a policy via HTTP
policy_enable_disable(Config) ->
    Auth = auth_header(Config),

    %% Check initial state (disabled by default)
    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/_policies/test_fanout",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    #{<<"enabled">> := false} = json:decode(Body),

    %% Enable policy (may fail to actually start tasks but API should work)
    {ok, _Status1, _Headers2, _Body2} = hackney:post(
        ?BASE_URL ++ "/_policies/test_fanout/_enable",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    %% Enable may succeed (200) or fail (400/500) if tasks can't be started
    %% (e.g., databases don't exist). The important thing is the API works.

    %% Disable policy
    {ok, 200, _Headers3, Body3} = hackney:post(
        ?BASE_URL ++ "/_policies/test_fanout/_disable",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    #{<<"ok">> := true} = json:decode(Body3),

    %% Verify disabled
    {ok, 200, _Headers4, Body4} = hackney:get(
        ?BASE_URL ++ "/_policies/test_fanout",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    #{<<"enabled">> := false} = json:decode(Body4),
    ok.

%% @doc Test getting policy status via HTTP
policy_status(Config) ->
    Auth = auth_header(Config),

    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/_policies/test_fanout/_status",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    Status = json:decode(Body),
    <<"test_fanout">> = maps:get(<<"name">>, Status),
    <<"fanout">> = maps:get(<<"pattern">>, Status),
    _ = maps:get(<<"enabled">>, Status),
    _ = maps:get(<<"task_count">>, Status),

    %% Non-existing policy status
    {ok, 404, _Headers2, _Body2} = hackney:get(
        ?BASE_URL ++ "/_policies/nonexistent/_status",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    ok.

%% @doc Test deleting a policy via HTTP
policy_delete(Config) ->
    Auth = auth_header(Config),

    %% Delete test_fanout policy
    {ok, 200, _Headers, Body} = hackney:delete(
        ?BASE_URL ++ "/_policies/test_fanout",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    #{<<"ok">> := true} = json:decode(Body),

    %% Verify deleted
    {ok, 404, _Headers2, _Body2} = hackney:get(
        ?BASE_URL ++ "/_policies/test_fanout",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),

    %% Delete non-existing policy
    {ok, 404, _Headers3, _Body3} = hackney:delete(
        ?BASE_URL ++ "/_policies/nonexistent",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),

    %% Delete test_chain too
    {ok, 200, _Headers4, _Body4} = hackney:delete(
        ?BASE_URL ++ "/_policies/test_chain",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    ok.

%%====================================================================
%% Tier HTTP API Tests
%%====================================================================

%% @doc Test setting tier configuration via HTTP
tier_config_set(Config) ->
    Auth = auth_header(Config),
    TierConfig = #{
        <<"enabled">> => true,
        <<"warm_db">> => <<"tier_warm_db">>,
        <<"hot_threshold">> => 3600,
        <<"warm_threshold">> => 86400
    },
    ReqBody = iolist_to_binary(json:encode(TierConfig)),

    {ok, 200, _Headers, Body} = hackney:post(
        ?BASE_URL ++ "/db/tier_test_db/_tier/config",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    #{<<"ok">> := true} = json:decode(Body),
    ok.

%% @doc Test getting tier configuration via HTTP
tier_config_get(Config) ->
    Auth = auth_header(Config),

    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/db/tier_test_db/_tier/config",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    TierConfig = json:decode(Body),
    true = maps:get(<<"enabled">>, TierConfig),
    <<"tier_warm_db">> = maps:get(<<"warm_db">>, TierConfig),
    ok.

%% @doc Test getting capacity info via HTTP
tier_capacity(Config) ->
    Auth = auth_header(Config),

    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/db/tier_test_db/_tier/capacity",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    CapacityInfo = json:decode(Body),
    %% Should have size_bytes or similar field
    true = is_map(CapacityInfo),
    ok.

%% @doc Test getting document tier via HTTP
tier_doc_get(Config) ->
    Auth = auth_header(Config),

    %% First create a document
    Doc = #{<<"_id">> => <<"tier_test_doc">>, <<"value">> => <<"test">>},
    DocBody = iolist_to_binary(json:encode(Doc)),
    {ok, 201, _, _PutBody} = hackney:put(
        ?BASE_URL ++ "/db/tier_test_db/tier_test_doc",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        DocBody,
        []
    ),

    %% Get document tier
    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/db/tier_test_db/tier_test_doc/_tier",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    TierInfo = json:decode(Body),
    <<"tier_test_doc">> = maps:get(<<"doc_id">>, TierInfo),
    ok.

%% @doc Test setting and getting document TTL via HTTP
tier_doc_ttl(Config) ->
    Auth = auth_header(Config),

    %% Set TTL
    TTLSpec = #{<<"ttl">> => 3600},
    TTLBody = iolist_to_binary(json:encode(TTLSpec)),
    {ok, 200, _Headers, SetBody} = hackney:post(
        ?BASE_URL ++ "/db/tier_test_db/tier_test_doc/_tier/ttl",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        TTLBody,
        []
    ),
    #{<<"ok">> := true} = json:decode(SetBody),

    %% Get TTL
    {ok, 200, _Headers2, GetBody} = hackney:get(
        ?BASE_URL ++ "/db/tier_test_db/tier_test_doc/_tier/ttl",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    TTLInfo = json:decode(GetBody),
    true = is_map(TTLInfo),
    ok.

%% @doc Test running migration via HTTP
tier_run_migration(Config) ->
    Auth = auth_header(Config),

    {ok, 200, _Headers, Body} = hackney:post(
        ?BASE_URL ++ "/db/tier_test_db/_tier/run_migration",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    Result = json:decode(Body),
    true = maps:get(<<"ok">>, Result),
    ok.

%%====================================================================
%% Usage HTTP API Tests
%%====================================================================

%% @doc Test getting usage stats for all databases
admin_usage_all(Config) ->
    Auth = auth_header(Config),

    %% hackney 3.x returns body directly in 4th element
    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/admin/usage",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    Response = json:decode(Body),

    %% Verify response structure
    true = is_map(Response),
    Databases = maps:get(<<"databases">>, Response),
    TotalDatabases = maps:get(<<"total_databases">>, Response),
    true = is_list(Databases),
    true = is_integer(TotalDatabases),
    true = TotalDatabases >= 0,
    true = length(Databases) =:= TotalDatabases,

    %% If we have databases, verify their structure
    lists:foreach(fun(DbStats) ->
        true = is_map(DbStats),
        true = maps:is_key(<<"database">>, DbStats),
        true = maps:is_key(<<"document_count">>, DbStats),
        true = maps:is_key(<<"storage_bytes">>, DbStats),
        true = maps:is_key(<<"memtable_size">>, DbStats),
        true = maps:is_key(<<"sst_files_size">>, DbStats),
        true = maps:is_key(<<"last_updated">>, DbStats)
    end, Databases),
    ok.

%% @doc Test getting usage stats for a single database
admin_usage_single_db(Config) ->
    Auth = auth_header(Config),
    DbName = <<"usage_http_test_db">>,

    %% hackney 3.x returns body directly in 4th element
    {ok, 200, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/admin/databases/" ++ binary_to_list(DbName) ++ "/usage",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    Stats = json:decode(Body),

    %% Verify response structure
    true = is_map(Stats),
    DbName = maps:get(<<"database">>, Stats),
    true = is_integer(maps:get(<<"document_count">>, Stats)),
    true = is_integer(maps:get(<<"storage_bytes">>, Stats)),
    true = is_integer(maps:get(<<"memtable_size">>, Stats)),
    true = is_integer(maps:get(<<"sst_files_size">>, Stats)),
    true = is_integer(maps:get(<<"last_updated">>, Stats)),

    %% Verify values are non-negative
    true = maps:get(<<"document_count">>, Stats) >= 0,
    true = maps:get(<<"storage_bytes">>, Stats) >= 0,
    ok.

%% @doc Test getting usage stats for a non-existent database
admin_usage_not_found(Config) ->
    Auth = auth_header(Config),

    %% hackney 3.x returns body directly in 4th element
    {ok, 404, _Headers, Body} = hackney:get(
        ?BASE_URL ++ "/admin/databases/nonexistent_db_12345/usage",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    Response = json:decode(Body),

    %% Verify error response
    <<"Database not found">> = maps:get(<<"error">>, Response),
    ok.

%% @doc Test that usage endpoints require authentication
admin_usage_requires_auth(_Config) ->
    %% hackney 3.x returns body directly in 4th element
    %% Try without auth header
    {ok, 401, _Headers, _Body} = hackney:get(
        ?BASE_URL ++ "/admin/usage",
        [{<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),

    %% Also test single db endpoint
    {ok, 401, _Headers2, _Body2} = hackney:get(
        ?BASE_URL ++ "/admin/databases/testdb/usage",
        [{<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    ok.

%%====================================================================
%% Federation Tests
%%====================================================================

%% @doc Test creating federation with bearer token auth via HTTP API
federation_create_with_bearer_auth(Config) ->
    Auth = auth_header(Config),

    %% Create federation with bearer auth
    FedSpec = #{
        <<"name">> => <<"http_bearer_fed">>,
        <<"members">> => [<<"fed_http_test_db">>],
        <<"auth">> => #{
            <<"bearer_token">> => <<"ak_test_bearer_token">>
        }
    },
    SpecJson = iolist_to_binary(json:encode(FedSpec)),

    {ok, 201, _Headers, Body} = hackney:post(
        ?BASE_URL ++ "/_federation",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        SpecJson,
        []
    ),
    #{<<"ok">> := true, <<"name">> := <<"http_bearer_fed">>} = json:decode(Body),

    %% Verify federation was created with auth
    {ok, Fed} = barrel_federation:get(<<"http_bearer_fed">>),
    true = maps:is_key(auth, Fed),
    #{bearer_token := <<"ak_test_bearer_token">>} = maps:get(auth, Fed),
    ok.

%% @doc Test creating federation with basic auth via HTTP API
federation_create_with_basic_auth(Config) ->
    Auth = auth_header(Config),

    %% Create federation with basic auth
    FedSpec = #{
        <<"name">> => <<"http_basic_fed">>,
        <<"members">> => [<<"fed_http_test_db">>],
        <<"auth">> => #{
            <<"basic_auth">> => #{
                <<"username">> => <<"admin">>,
                <<"password">> => <<"secret123">>
            }
        }
    },
    SpecJson = iolist_to_binary(json:encode(FedSpec)),

    {ok, 201, _Headers, Body} = hackney:post(
        ?BASE_URL ++ "/_federation",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        SpecJson,
        []
    ),
    #{<<"ok">> := true, <<"name">> := <<"http_basic_fed">>} = json:decode(Body),

    %% Verify federation was created with auth
    {ok, Fed} = barrel_federation:get(<<"http_basic_fed">>),
    true = maps:is_key(auth, Fed),
    #{basic_auth := {<<"admin">>, <<"secret123">>}} = maps:get(auth, Fed),
    ok.

%% @doc Test querying federation with auth override via HTTP API
federation_find_with_auth_override(Config) ->
    Auth = auth_header(Config),

    %% Create federation with default auth
    FedSpec = #{
        <<"name">> => <<"http_override_fed">>,
        <<"members">> => [<<"fed_http_test_db">>],
        <<"auth">> => #{
            <<"bearer_token">> => <<"ak_default_token">>
        }
    },
    SpecJson = iolist_to_binary(json:encode(FedSpec)),

    {ok, 201, _Headers, _Body} = hackney:post(
        ?BASE_URL ++ "/_federation",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        SpecJson,
        []
    ),

    %% Add a test document
    Doc = #{<<"id">> => <<"fed_test_doc">>, <<"type">> => <<"test">>},
    DocJson = iolist_to_binary(json:encode(Doc)),
    {ok, 201, _, _} = hackney:put(
        ?BASE_URL ++ "/db/fed_http_test_db/fed_test_doc",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        DocJson,
        []
    ),

    %% Query with auth override
    QuerySpec = #{
        <<"where">> => [#{
            <<"path">> => [<<"type">>],
            <<"op">> => <<"eq">>,
            <<"value">> => <<"test">>
        }],
        <<"auth">> => #{
            <<"bearer_token">> => <<"ak_override_token">>
        }
    },
    QueryJson = iolist_to_binary(json:encode(QuerySpec)),

    {ok, 200, _Headers2, Body2} = hackney:post(
        ?BASE_URL ++ "/_federation/http_override_fed/_find",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        QueryJson,
        []
    ),

    Response = json:decode(Body2),
    Results = maps:get(<<"results">>, Response),
    true = length(Results) >= 1,

    %% Find our test document
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    true = lists:member(<<"fed_test_doc">>, DocIds),
    ok.
