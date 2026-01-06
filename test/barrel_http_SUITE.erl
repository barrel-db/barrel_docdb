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
    delete_doc/1,
    get_changes/1,
    get_changes_with_filter/1,
    get_changes_longpoll_timeout/1,
    changes_stream_basic/1,
    bulk_docs/1
]).

-define(PORT, 18080).
-define(BASE_URL, "http://localhost:18080").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, http_tests}].

groups() ->
    [{http_tests, [sequence], [
        health_check,
        get_doc_not_found,
        put_and_get_doc_json,
        put_and_get_doc_cbor,
        delete_doc,
        get_changes,
        get_changes_with_filter,
        get_changes_longpoll_timeout,
        changes_stream_basic,
        bulk_docs
    ]}].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    application:ensure_all_started(cowboy),
    application:ensure_all_started(hackney),
    %% Create a test API key for authentication
    {ok, ApiKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"http-suite-key">>,
        permissions => [<<"read">>, <<"write">>, <<"admin">>],
        is_admin => true
    }),
    [{api_key, ApiKey} | Config].

end_per_suite(_Config) ->
    ok.

init_per_group(http_tests, Config) ->
    %% Start HTTP server
    {ok, _Pid} = barrel_http_server:start_link(#{port => ?PORT}),
    %% Create test database
    {ok, _} = barrel_docdb:create_db(<<"testdb">>),
    Config.

%% Helper to get auth header
auth_header(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}.

end_per_group(http_tests, _Config) ->
    barrel_docdb:delete_db(<<"testdb">>),
    catch barrel_http_server:stop(),
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
