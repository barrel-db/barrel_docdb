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
        bulk_docs
    ]}].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    application:ensure_all_started(cowboy),
    application:ensure_all_started(hackney),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(http_tests, Config) ->
    %% Start HTTP server
    {ok, _Pid} = barrel_http_server:start_link(#{port => ?PORT}),
    %% Create test database
    {ok, _} = barrel_docdb:create_db(<<"testdb">>),
    Config.

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
get_doc_not_found(_Config) ->
    {ok, 404, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/db/testdb/nonexistent",
        [{<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    #{<<"error">> := <<"Document not found">>} = json:decode(Body),
    ok.

%% @doc Test PUT and GET document with JSON
put_and_get_doc_json(_Config) ->
    DocId = <<"test_doc_json">>,
    Doc = #{<<"name">> => <<"Alice">>, <<"age">> => 30},
    DocJson = iolist_to_binary(json:encode(Doc)),

    %% PUT document
    {ok, 201, _PutHeaders, PutRef} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [{<<"Content-Type">>, <<"application/json">>},
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
        [{<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, GetBody} = hackney:body(GetRef),
    #{<<"name">> := <<"Alice">>, <<"age">> := 30} = json:decode(GetBody),
    ok.

%% @doc Test PUT and GET document with CBOR
put_and_get_doc_cbor(_Config) ->
    DocId = <<"test_doc_cbor">>,
    Doc = #{<<"name">> => <<"Bob">>, <<"age">> => 25},
    DocCbor = barrel_docdb_codec_cbor:encode_cbor(Doc),

    %% PUT document with CBOR
    {ok, 201, _PutHeaders, PutRef} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [{<<"Content-Type">>, <<"application/cbor">>},
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
        [{<<"Accept">>, <<"application/cbor">>}],
        <<>>,
        []
    ),
    {ok, GetBody} = hackney:body(GetRef),
    #{<<"name">> := <<"Bob">>, <<"age">> := 25} = barrel_docdb_codec_cbor:decode_cbor(GetBody),
    ok.

%% @doc Test DELETE document
delete_doc(_Config) ->
    DocId = <<"test_doc_delete">>,
    Doc = #{<<"temp">> => true},
    DocJson = iolist_to_binary(json:encode(Doc)),

    %% PUT document first
    {ok, 201, _PutHeaders, PutRef} = hackney:put(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [{<<"Content-Type">>, <<"application/json">>}],
        DocJson,
        []
    ),
    {ok, PutBody} = hackney:body(PutRef),
    #{<<"rev">> := Rev} = json:decode(PutBody),

    %% DELETE document
    {ok, 200, _DelHeaders, DelRef} = hackney:delete(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId) ++ "?rev=" ++ binary_to_list(Rev),
        [{<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, _DelBody} = hackney:body(DelRef),

    %% Verify deleted
    {ok, 404, _GetHeaders, _GetRef} = hackney:get(
        ?BASE_URL ++ "/db/testdb/" ++ binary_to_list(DocId),
        [],
        <<>>,
        []
    ),
    ok.

%% @doc Test changes feed
get_changes(_Config) ->
    %% Insert a document
    DocId = <<"test_doc_changes">>,
    Doc = #{<<"type">> => <<"test">>},
    {ok, _} = barrel_docdb:put_doc(<<"testdb">>, Doc#{<<"id">> => DocId}),

    %% Get changes
    {ok, 200, _Headers, Ref} = hackney:get(
        ?BASE_URL ++ "/db/testdb/_changes",
        [{<<"Accept">>, <<"application/json">>}],
        <<>>,
        []
    ),
    {ok, Body} = hackney:body(Ref),
    #{<<"results">> := Results} = json:decode(Body),
    true = is_list(Results),
    true = length(Results) > 0,
    ok.

%% @doc Test bulk docs
bulk_docs(_Config) ->
    Docs = [
        #{<<"id">> => <<"bulk1">>, <<"val">> => 1},
        #{<<"id">> => <<"bulk2">>, <<"val">> => 2},
        #{<<"id">> => <<"bulk3">>, <<"val">> => 3}
    ],
    ReqBody = iolist_to_binary(json:encode(#{<<"docs">> => Docs})),

    {ok, 201, _Headers, Ref} = hackney:post(
        ?BASE_URL ++ "/db/testdb/_bulk_docs",
        [{<<"Content-Type">>, <<"application/json">>},
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
