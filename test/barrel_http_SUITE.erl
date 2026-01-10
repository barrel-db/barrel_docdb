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
    get_changes_longpoll_timeout/1,
    changes_stream_basic/1,
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
    tier_run_migration/1
]).

-define(PORT, 18080).
-define(BASE_URL, "http://localhost:18080").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, http_tests}, {group, policy_tests}, {group, tier_tests}].

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
            get_changes_longpoll_timeout,
            changes_stream_basic,
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
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    application:ensure_all_started(cowboy),
    application:ensure_all_started(hackney),
    %% Start HTTP server at suite level (not stopped until end_per_suite)
    {ok, _Pid} = barrel_http_server:start_link(#{port => ?PORT}),
    %% Create a test API key for authentication
    {ok, ApiKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"http-suite-key">>,
        permissions => [<<"read">>, <<"write">>, <<"admin">>],
        is_admin => true
    }),
    [{api_key, ApiKey} | Config].

end_per_suite(_Config) ->
    %% Stop HTTP server at suite level
    catch barrel_http_server:stop(),
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
    {ok, 200, _Headers, Ref} = hackney:get(?BASE_URL ++ "/health", [], <<>>, []),
    {ok, Body} = hackney:body(Ref),
    #{<<"status">> := <<"ok">>} = json:decode(Body),
    ok.

%% @doc Test getting a non-existent document
get_doc_not_found(Config) ->
    Auth = auth_header(Config),
    {ok, 404, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/db/testdb/nonexistent",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    #{<<"error">> := <<"Document not found">>} = json:decode(Body),
    ok.

%% @doc Test PUT and GET document with JSON
put_and_get_doc_json(Config) ->
    Auth = auth_header(Config),
    DocId = <<"test_doc_json">>,
    Doc = #{<<"name">> => <<"Alice">>, <<"age">> => 30},
    DocJson = iolist_to_binary(json:encode(Doc)),

    %% PUT document
    {ok, 201, _PutHeaders, PutRef} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        DocJson,
        []
    ),
    {ok, PutBody} = hackney:body(PutRef),
    #{<<"id">> := DocId, <<"rev">> := Rev} = json:decode(PutBody),
    true = is_binary(Rev),

    %% GET document
    {ok, 200, _GetHeaders, GetRef} = hackney:get(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, GetBody} = hackney:body(GetRef),
    #{<<"name">> := <<"Alice">>, <<"age">> := 30} = json:decode(GetBody),
    ok.

%% @doc Test PUT and GET document with CBOR
put_and_get_doc_cbor(Config) ->
    Auth = auth_header(Config),
    DocId = <<"test_doc_cbor">>,
    Doc = #{<<"name">> => <<"Bob">>, <<"age">> => 25},
    DocCbor = barrel_docdb_codec_cbor:encode_cbor(Doc),

    %% PUT document with CBOR
    {ok, 201, _PutHeaders, PutRef} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth,
         {<<"Content-Type">>, <<"application/cbor">>},
         {<<"Accept">>, <<"application/cbor">>}],
        DocCbor,
        []
    ),
    {ok, PutBody} = hackney:body(PutRef),
    #{<<"id">> := DocId, <<"rev">> := Rev} = barrel_docdb_codec_cbor:decode_cbor(PutBody),
    true = is_binary(Rev),

    %% GET document with CBOR
    {ok, 200, _GetHeaders, GetRef} = hackney:get(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth, {<<"Accept">>, <<"application/cbor">>}],
        <<>>,
        []
    ),
    {ok, GetBody} = hackney:body(GetRef),
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
    {ok, 201, _PutHeaders, PutRef} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth,
         {<<"Content-Type">>, <<"application/cbor">>},
         {<<"Accept">>, <<"application/cbor">>}],
        DocCbor,
        []
    ),
    {ok, PutBody} = hackney:body(PutRef),
    #{<<"id">> := DocId, <<"rev">> := Rev} = barrel_docdb_codec_cbor:decode_cbor(PutBody),

    %% GET document with CBOR - should use zero-copy path
    {ok, 200, GetHeaders, GetRef} = hackney:get(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth, {<<"Accept">>, <<"application/cbor">>}],
        <<>>,
        []
    ),
    %% Verify Content-Type is CBOR
    <<"application/cbor">> = proplists:get_value(<<"content-type">>, GetHeaders),

    {ok, GetBody} = hackney:body(GetRef),
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

    {ok, 200, FindHeaders, FindRef} = hackney:post(
        ?BASE_URL ++ "/db/testdb/_find",
        [Auth,
         {<<"Content-Type">>, <<"application/cbor">>},
         {<<"Accept">>, <<"application/cbor">>}],
        QueryCbor,
        []
    ),
    %% Verify Content-Type is CBOR
    <<"application/cbor">> = proplists:get_value(<<"content-type">>, FindHeaders),

    {ok, FindBody} = hackney:body(FindRef),
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
    {ok, 201, _PutHeaders, PutRef} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [Auth, {<<"Content-Type">>, <<"application/json">>}],
        DocJson,
        []
    ),
    {ok, PutBody} = hackney:body(PutRef),
    #{<<"rev">> := Rev} = json:decode(PutBody),

    %% DELETE document
    {ok, 200, _DelHeaders, DelRef} = hackney:delete(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId) ++ "?rev=" ++ binary_to_list(Rev),
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, _DelBody} = hackney:body(DelRef),

    %% Verify deleted
    {ok, 404, _GetHeaders, _GetRef} = hackney:get(
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
    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/db/testdb/_changes",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
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
    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/db/testdb/_changes?filter=users/%2B/profile",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    #{<<"results">> := Results} = json:decode(Body),

    %% Should only have user profile documents
    ResultIds = [maps:get(<<"id">>, R) || R <- Results],
    true = lists:member(<<"users/alice/profile">>, ResultIds),
    true = lists:member(<<"users/bob/profile">>, ResultIds),
    false = lists:member(<<"orders/123">>, ResultIds),
    ok.

%% @doc Test long poll changes with timeout (no new changes)
get_changes_longpoll_timeout(Config) ->
    Auth = auth_header(Config),
    %% Get the current last_seq first
    {ok, 200, _, Ref0} = hackney:get(
        ?BASE_URL ++ "/db/testdb/_changes",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body0} = hackney:body(Ref0),
    #{<<"last_seq">> := LastSeq} = json:decode(Body0),

    %% Long poll with short timeout - should return empty after timeout
    StartTime = erlang:monotonic_time(millisecond),
    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/db/testdb/_changes?feed=longpoll&timeout=500&since=" ++ binary_to_list(LastSeq),
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        [{recv_timeout, 5000}]
    ),
    EndTime = erlang:monotonic_time(millisecond),
    {ok, Body} = hackney:body(Ref),

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

%% @doc Test bulk docs
bulk_docs(Config) ->
    Auth = auth_header(Config),
    Docs = [
        #{<<"id">> => <<"bulk1">>, <<"val">> => 1},
        #{<<"id">> => <<"bulk2">>, <<"val">> => 2},
        #{<<"id">> => <<"bulk3">>, <<"val">> => 3}
    ],
    ReqBody = iolist_to_binary(json:encode(#{<<"docs">> => Docs})),

    {ok, 201, _Headers, Ref} = hackney:post(
        ?BASE_URL ++ "/db/testdb/_bulk_docs",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    {ok, Body} = hackney:body(Ref),
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

    {ok, 201, _Headers, Ref} = hackney:post(
        ?BASE_URL ++ "/_policies",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    #{<<"ok">> := true, <<"name">> := <<"test_fanout">>} = json:decode(Body),

    %% Try to create duplicate
    {ok, 409, _Headers2, Ref2} = hackney:post(
        ?BASE_URL ++ "/_policies",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    {ok, _} = hackney:body(Ref2),
    ok.

%% @doc Test getting a policy via HTTP
policy_get(Config) ->
    Auth = auth_header(Config),

    %% Get existing policy
    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/_policies/test_fanout",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    Policy = json:decode(Body),
    <<"test_fanout">> = maps:get(<<"name">>, Policy),
    <<"fanout">> = maps:get(<<"pattern">>, Policy),
    <<"source_db">> = maps:get(<<"source">>, Policy),

    %% Get non-existing policy
    {ok, 404, _Headers2, Ref2} = hackney:get(
        ?BASE_URL ++ "/_policies/nonexistent",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, _} = hackney:body(Ref2),
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
    {ok, 201, _, CreateRef} = hackney:post(
        ?BASE_URL ++ "/_policies",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    {ok, _} = hackney:body(CreateRef),

    %% List all policies
    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/_policies",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
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
    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/_policies/test_fanout",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    #{<<"enabled">> := false} = json:decode(Body),

    %% Enable policy (may fail to actually start tasks but API should work)
    {ok, _Status1, _Headers2, Ref2} = hackney:post(
        ?BASE_URL ++ "/_policies/test_fanout/_enable",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, _} = hackney:body(Ref2),
    %% Enable may succeed (200) or fail (400/500) if tasks can't be started
    %% (e.g., databases don't exist). The important thing is the API works.

    %% Disable policy
    {ok, 200, _Headers3, Ref3} = hackney:post(
        ?BASE_URL ++ "/_policies/test_fanout/_disable",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body3} = hackney:body(Ref3),
    #{<<"ok">> := true} = json:decode(Body3),

    %% Verify disabled
    {ok, 200, _Headers4, Ref4} = hackney:get(
        ?BASE_URL ++ "/_policies/test_fanout",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body4} = hackney:body(Ref4),
    #{<<"enabled">> := false} = json:decode(Body4),
    ok.

%% @doc Test getting policy status via HTTP
policy_status(Config) ->
    Auth = auth_header(Config),

    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/_policies/test_fanout/_status",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    Status = json:decode(Body),
    <<"test_fanout">> = maps:get(<<"name">>, Status),
    <<"fanout">> = maps:get(<<"pattern">>, Status),
    _ = maps:get(<<"enabled">>, Status),
    _ = maps:get(<<"task_count">>, Status),

    %% Non-existing policy status
    {ok, 404, _Headers2, Ref2} = hackney:get(
        ?BASE_URL ++ "/_policies/nonexistent/_status",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, _} = hackney:body(Ref2),
    ok.

%% @doc Test deleting a policy via HTTP
policy_delete(Config) ->
    Auth = auth_header(Config),

    %% Delete test_fanout policy
    {ok, 200, _Headers, Ref} = hackney:delete(
        ?BASE_URL ++ "/_policies/test_fanout",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    #{<<"ok">> := true} = json:decode(Body),

    %% Verify deleted
    {ok, 404, _Headers2, Ref2} = hackney:get(
        ?BASE_URL ++ "/_policies/test_fanout",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, _} = hackney:body(Ref2),

    %% Delete non-existing policy
    {ok, 404, _Headers3, Ref3} = hackney:delete(
        ?BASE_URL ++ "/_policies/nonexistent",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, _} = hackney:body(Ref3),

    %% Delete test_chain too
    {ok, 200, _Headers4, Ref4} = hackney:delete(
        ?BASE_URL ++ "/_policies/test_chain",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, _} = hackney:body(Ref4),
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

    {ok, 200, _Headers, Ref} = hackney:post(
        ?BASE_URL ++ "/db/tier_test_db/_tier/config",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        ReqBody,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    #{<<"ok">> := true} = json:decode(Body),
    ok.

%% @doc Test getting tier configuration via HTTP
tier_config_get(Config) ->
    Auth = auth_header(Config),

    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/db/tier_test_db/_tier/config",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    TierConfig = json:decode(Body),
    true = maps:get(<<"enabled">>, TierConfig),
    <<"tier_warm_db">> = maps:get(<<"warm_db">>, TierConfig),
    ok.

%% @doc Test getting capacity info via HTTP
tier_capacity(Config) ->
    Auth = auth_header(Config),

    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/db/tier_test_db/_tier/capacity",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
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
    {ok, 201, _, PutRef} = hackney:put(
        ?BASE_URL ++ "/db/tier_test_db/tier_test_doc",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        DocBody,
        []
    ),
    {ok, _} = hackney:body(PutRef),

    %% Get document tier
    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/db/tier_test_db/tier_test_doc/_tier",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    TierInfo = json:decode(Body),
    <<"tier_test_doc">> = maps:get(<<"doc_id">>, TierInfo),
    ok.

%% @doc Test setting and getting document TTL via HTTP
tier_doc_ttl(Config) ->
    Auth = auth_header(Config),

    %% Set TTL
    TTLSpec = #{<<"ttl">> => 3600},
    TTLBody = iolist_to_binary(json:encode(TTLSpec)),
    {ok, 200, _Headers, SetRef} = hackney:post(
        ?BASE_URL ++ "/db/tier_test_db/tier_test_doc/_tier/ttl",
        [Auth,
         {<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>}],
        TTLBody,
        []
    ),
    {ok, SetBody} = hackney:body(SetRef),
    #{<<"ok">> := true} = json:decode(SetBody),

    %% Get TTL
    {ok, 200, _Headers2, GetRef} = hackney:get(
        ?BASE_URL ++ "/db/tier_test_db/tier_test_doc/_tier/ttl",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, GetBody} = hackney:body(GetRef),
    TTLInfo = json:decode(GetBody),
    true = is_map(TTLInfo),
    ok.

%% @doc Test running migration via HTTP
tier_run_migration(Config) ->
    Auth = auth_header(Config),

    {ok, 200, _Headers, Ref} = hackney:post(
        ?BASE_URL ++ "/db/tier_test_db/_tier/run_migration",
        [Auth, {<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    Result = json:decode(Body),
    true = maps:get(<<"ok">>, Result),
    ok.
