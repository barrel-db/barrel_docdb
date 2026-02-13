%%%-------------------------------------------------------------------
%%% @doc Tests for VDB HTTP API
%%%
%%% Tests HTTP endpoints for virtual database operations.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb_http_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PORT, 18080).
-define(BASE_URL, "http://localhost:" ++ integer_to_list(?PORT)).

%%====================================================================
%% Common Test callbacks
%%====================================================================

all() ->
    [
        {group, lifecycle},
        {group, document_ops},
        {group, query_ops}
    ].

groups() ->
    [
        {lifecycle, [], [
            create_vdb,
            create_vdb_already_exists,
            list_vdbs,
            get_vdb_info,
            get_vdb_shards,
            delete_vdb,
            delete_vdb_not_found
        ]},
        {document_ops, [], [
            put_doc,
            get_doc,
            get_doc_not_found,
            delete_doc,
            bulk_docs
        ]},
        {query_ops, [], [
            find_all,
            find_with_selector,
            get_changes
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    application:ensure_all_started(hackney),
    %% Start HTTP server on test port
    %% Unlink so it survives process changes between init/end_per_suite
    {ok, HttpPid} = barrel_http_server:start_link(#{port => ?PORT}),
    unlink(HttpPid),
    %% Create API key for tests
    {ok, ApiKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"vdb-http-suite-key">>,
        permissions => [<<"read">>, <<"write">>, <<"admin">>],
        is_admin => true
    }),
    [{api_key, ApiKey} | Config].

end_per_suite(_Config) ->
    barrel_http_server:stop(),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    cleanup_test_vdbs(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    cleanup_test_vdbs(),
    ok.

cleanup_test_vdbs() ->
    {ok, VDBs} = barrel_vdb:list(),
    lists:foreach(fun(Name) ->
        case binary:match(Name, <<"test_">>) of
            {0, _} -> barrel_vdb:delete(Name);
            _ -> ok
        end
    end, VDBs).

%%====================================================================
%% Test Cases - Lifecycle
%%====================================================================

create_vdb(Config) ->
    Url = ?BASE_URL ++ "/vdb",
    Body = json:encode(#{
        <<"name">> => <<"test_http_create">>,
        <<"shard_count">> => 4
    }),
    {ok, 201, _Headers, RespBody} = hackney:post(Url, json_headers(Config), Body, []),
    Result = json:decode(RespBody),
    ?assertEqual(true, maps:get(<<"ok">>, Result)),
    ?assertEqual(<<"test_http_create">>, maps:get(<<"name">>, Result)),
    %% Verify VDB was created
    ?assert(barrel_vdb:exists(<<"test_http_create">>)).

create_vdb_already_exists(Config) ->
    %% Create VDB first
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_dup">>, #{shard_count => 2})),
    %% Try to create again via HTTP
    Url = ?BASE_URL ++ "/vdb",
    Body = json:encode(#{<<"name">> => <<"test_http_dup">>}),
    {ok, 409, _Headers, RespBody} = hackney:post(Url, json_headers(Config), Body, []),
    Result = json:decode(RespBody),
    ?assert(maps:is_key(<<"error">>, Result)).

list_vdbs(Config) ->
    %% Create some VDBs
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_list_a">>, #{shard_count => 2})),
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_list_b">>, #{shard_count => 2})),
    %% List via HTTP
    Url = ?BASE_URL ++ "/vdb",
    {ok, 200, _Headers, RespBody} = hackney:get(Url, json_headers(Config), <<>>, []),
    Result = json:decode(RespBody),
    VdbList = maps:get(<<"vdbs">>, Result),
    ?assert(lists:member(<<"test_http_list_a">>, VdbList)),
    ?assert(lists:member(<<"test_http_list_b">>, VdbList)).

get_vdb_info(Config) ->
    %% Create VDB
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_info">>, #{shard_count => 4})),
    %% Get info via HTTP
    Url = ?BASE_URL ++ "/vdb/test_http_info",
    {ok, 200, _Headers, RespBody} = hackney:get(Url, json_headers(Config), <<>>, []),
    Result = json:decode(RespBody),
    ?assertEqual(<<"test_http_info">>, maps:get(<<"name">>, Result)),
    ?assertEqual(4, maps:get(<<"shard_count">>, Result)),
    ?assert(maps:is_key(<<"total_docs">>, Result)),
    ?assert(maps:is_key(<<"shards">>, Result)).

get_vdb_shards(Config) ->
    %% Create VDB
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_shards">>, #{shard_count => 3})),
    %% Get shards via HTTP
    Url = ?BASE_URL ++ "/vdb/test_http_shards/_shards",
    {ok, 200, _Headers, RespBody} = hackney:get(Url, json_headers(Config), <<>>, []),
    Result = json:decode(RespBody),
    Shards = maps:get(<<"shards">>, Result),
    Ranges = maps:get(<<"ranges">>, Result),
    ?assertEqual(3, length(Shards)),
    ?assertEqual(3, length(Ranges)).

delete_vdb(Config) ->
    %% Create VDB
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_delete">>, #{shard_count => 2})),
    ?assert(barrel_vdb:exists(<<"test_http_delete">>)),
    %% Delete via HTTP
    Url = ?BASE_URL ++ "/vdb/test_http_delete",
    {ok, 200, _Headers, RespBody} = hackney:delete(Url, json_headers(Config), <<>>, []),
    Result = json:decode(RespBody),
    ?assertEqual(true, maps:get(<<"ok">>, Result)),
    %% Verify VDB was deleted
    ?assertNot(barrel_vdb:exists(<<"test_http_delete">>)).

delete_vdb_not_found(Config) ->
    Url = ?BASE_URL ++ "/vdb/nonexistent",
    {ok, 404, _Headers, _Ref} = hackney:delete(Url, json_headers(Config), <<>>, []).

%%====================================================================
%% Test Cases - Document Operations
%%====================================================================

put_doc(Config) ->
    %% Create VDB
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_put">>, #{shard_count => 4})),
    %% Put document via HTTP
    Url = ?BASE_URL ++ "/vdb/test_http_put/mydoc",
    Body = json:encode(#{<<"name">> => <<"Test Document">>}),
    {ok, 201, _Headers, RespBody} = hackney:put(Url, json_headers(Config), Body, []),
    Result = json:decode(RespBody),
    ?assertEqual(<<"mydoc">>, maps:get(<<"id">>, Result)),
    ?assert(is_binary(maps:get(<<"rev">>, Result))).

get_doc(Config) ->
    %% Create VDB and document
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_get">>, #{shard_count => 4})),
    {ok, _} = barrel_vdb:put_doc(<<"test_http_get">>, #{
        <<"id">> => <<"getme">>,
        <<"value">> => 42
    }),
    %% Get document via HTTP
    Url = ?BASE_URL ++ "/vdb/test_http_get/getme",
    {ok, 200, _Headers, RespBody} = hackney:get(Url, json_headers(Config), <<>>, []),
    Result = json:decode(RespBody),
    ?assertEqual(<<"getme">>, maps:get(<<"id">>, Result)),
    ?assertEqual(42, maps:get(<<"value">>, Result)).

get_doc_not_found(Config) ->
    %% Create VDB
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_get404">>, #{shard_count => 2})),
    %% Get non-existent document
    Url = ?BASE_URL ++ "/vdb/test_http_get404/missing",
    {ok, 404, _Headers, _Ref} = hackney:get(Url, json_headers(Config), <<>>, []).

delete_doc(Config) ->
    %% Create VDB and document
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_deldoc">>, #{shard_count => 2})),
    {ok, _} = barrel_vdb:put_doc(<<"test_http_deldoc">>, #{<<"id">> => <<"todelete">>}),
    %% Delete document via HTTP
    Url = ?BASE_URL ++ "/vdb/test_http_deldoc/todelete",
    {ok, 200, _Headers, RespBody} = hackney:delete(Url, json_headers(Config), <<>>, []),
    Result = json:decode(RespBody),
    ?assertEqual(<<"todelete">>, maps:get(<<"id">>, Result)),
    %% Verify document was deleted
    ?assertEqual({error, not_found}, barrel_vdb:get_doc(<<"test_http_deldoc">>, <<"todelete">>)).

bulk_docs(Config) ->
    %% Create VDB
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_bulk">>, #{shard_count => 4})),
    %% Bulk insert via HTTP
    Url = ?BASE_URL ++ "/vdb/test_http_bulk/_bulk_docs",
    Body = json:encode(#{
        <<"docs">> => [
            #{<<"id">> => <<"bulk1">>, <<"n">> => 1},
            #{<<"id">> => <<"bulk2">>, <<"n">> => 2},
            #{<<"id">> => <<"bulk3">>, <<"n">> => 3}
        ]
    }),
    {ok, 201, _Headers, RespBody} = hackney:post(Url, json_headers(Config), Body, []),
    Result = json:decode(RespBody),
    ?assertEqual(3, length(Result)),
    %% Verify documents exist
    {ok, _} = barrel_vdb:get_doc(<<"test_http_bulk">>, <<"bulk1">>),
    {ok, _} = barrel_vdb:get_doc(<<"test_http_bulk">>, <<"bulk2">>),
    {ok, _} = barrel_vdb:get_doc(<<"test_http_bulk">>, <<"bulk3">>).

%%====================================================================
%% Test Cases - Query Operations
%%====================================================================

find_all(Config) ->
    %% Create VDB and documents
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_find">>, #{shard_count => 4})),
    lists:foreach(fun(I) ->
        DocId = list_to_binary("find" ++ integer_to_list(I)),
        {ok, _} = barrel_vdb:put_doc(<<"test_http_find">>, #{<<"id">> => DocId, <<"i">> => I})
    end, lists:seq(1, 10)),
    %% Find via HTTP
    Url = ?BASE_URL ++ "/vdb/test_http_find/_find",
    Body = json:encode(#{}),
    {ok, 200, _Headers, RespBody} = hackney:post(Url, json_headers(Config), Body, []),
    Result = json:decode(RespBody),
    Docs = maps:get(<<"docs">>, Result),
    ?assertEqual(10, length(Docs)).

find_with_selector(Config) ->
    %% Create VDB and documents
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_findsel">>, #{shard_count => 2})),
    {ok, _} = barrel_vdb:put_doc(<<"test_http_findsel">>, #{<<"id">> => <<"a">>, <<"type">> => <<"x">>}),
    {ok, _} = barrel_vdb:put_doc(<<"test_http_findsel">>, #{<<"id">> => <<"b">>, <<"type">> => <<"y">>}),
    {ok, _} = barrel_vdb:put_doc(<<"test_http_findsel">>, #{<<"id">> => <<"c">>, <<"type">> => <<"x">>}),
    %% Find with selector via HTTP
    Url = ?BASE_URL ++ "/vdb/test_http_findsel/_find",
    Body = json:encode(#{<<"selector">> => #{<<"type">> => <<"x">>}}),
    {ok, 200, _Headers, RespBody} = hackney:post(Url, json_headers(Config), Body, []),
    Result = json:decode(RespBody),
    Docs = maps:get(<<"docs">>, Result),
    ?assertEqual(2, length(Docs)),
    Types = [maps:get(<<"type">>, D) || D <- Docs],
    ?assert(lists:all(fun(T) -> T =:= <<"x">> end, Types)).

get_changes(Config) ->
    %% Create VDB and documents
    ?assertEqual(ok, barrel_vdb:create(<<"test_http_changes">>, #{shard_count => 2})),
    lists:foreach(fun(I) ->
        DocId = list_to_binary("chg" ++ integer_to_list(I)),
        {ok, _} = barrel_vdb:put_doc(<<"test_http_changes">>, #{<<"id">> => DocId})
    end, lists:seq(1, 5)),
    %% Get changes via HTTP
    Url = ?BASE_URL ++ "/vdb/test_http_changes/_changes",
    {ok, 200, _Headers, RespBody} = hackney:get(Url, json_headers(Config), <<>>, []),
    Result = json:decode(RespBody),
    ?assert(maps:is_key(<<"changes">>, Result)),
    ?assert(maps:is_key(<<"last_seq">>, Result)),
    Changes = maps:get(<<"changes">>, Result),
    ?assertEqual(5, length(Changes)).

%%====================================================================
%% Helper functions
%%====================================================================

json_headers(Config) ->
    ApiKey = proplists:get_value(api_key, Config),
    [{<<"Content-Type">>, <<"application/json">>},
     {<<"Accept">>, <<"application/json">>},
     {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}].
