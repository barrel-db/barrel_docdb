%%%-------------------------------------------------------------------
%%% @doc HTTP permission enforcement regression tests.
%%%
%%% Verifies that the `permissions' field on an API key actually gates
%%% requests at the handler boundary. Before 0.6.4 the handler discarded
%%% the validated key map; any valid key could write, delete, replicate.
%%% Also covers the query-op atom whitelist.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_http_auth_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    read_only_key_can_read/1,
    read_only_key_cannot_write/1,
    read_only_key_cannot_delete/1,
    read_only_key_cannot_replicate/1,
    read_only_key_cannot_bulk/1,
    write_key_can_write/1,
    admin_key_can_write/1,
    invalid_query_op_400/1
]).

-define(PORT, 18091).
-define(BASE, "http://localhost:18091").
-define(DB, <<"auth_test_db">>).

all() ->
    [
        read_only_key_can_read,
        read_only_key_cannot_write,
        read_only_key_cannot_delete,
        read_only_key_cannot_replicate,
        read_only_key_cannot_bulk,
        write_key_can_write,
        admin_key_can_write,
        invalid_query_op_400
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    {ok, _} = application:ensure_all_started(cowboy),
    {ok, _} = application:ensure_all_started(hackney),
    {ok, HttpPid} = barrel_http_server:start_link(#{port => ?PORT}),
    unlink(HttpPid),
    {ok, ReadKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"ro-key">>, permissions => [<<"read">>]}),
    {ok, WriteKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"rw-key">>, permissions => [<<"read">>, <<"write">>]}),
    {ok, AdminKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"admin-key">>,
        permissions => [<<"read">>, <<"write">>, <<"admin">>],
        is_admin => true}),
    _ = barrel_docdb:delete_db(?DB),
    {ok, _} = barrel_docdb:create_db(?DB),
    [{ro, ReadKey}, {rw, WriteKey}, {admin, AdminKey} | Config].

end_per_suite(_Config) ->
    barrel_http_server:stop(),
    barrel_docdb:delete_db(?DB),
    ok = application:stop(barrel_docdb),
    ok.

h(Config, Tag) ->
    Key = proplists:get_value(Tag, Config),
    [{<<"Authorization">>, <<"Bearer ", Key/binary>>},
     {<<"Content-Type">>, <<"application/json">>},
     {<<"Accept">>, <<"application/json">>}].

url(Path) -> ?BASE ++ Path.

read_only_key_can_read(Config) ->
    {ok, 200, _, _} = hackney:get(url("/db/" ++ binary_to_list(?DB)),
                                  h(Config, ro), <<>>, []),
    ok.

read_only_key_cannot_write(Config) ->
    {ok, 403, _, RespBody} = hackney:put(
        url("/db/" ++ binary_to_list(?DB) ++ "/ro_write_attempt"),
        h(Config, ro), <<"{\"v\":1}">>, []),
    Err = maps:get(<<"error">>, json:decode(RespBody)),
    true = binary:match(Err, <<"Permission denied">>) =/= nomatch,
    ok.

read_only_key_cannot_delete(Config) ->
    {ok, 201, _, _} = hackney:put(
        url("/db/" ++ binary_to_list(?DB) ++ "/ro_delete_attempt"),
        h(Config, rw), <<"{\"v\":1}">>, []),
    {ok, 403, _, _} = hackney:delete(
        url("/db/" ++ binary_to_list(?DB) ++ "/ro_delete_attempt"),
        h(Config, ro), <<>>, []),
    ok.

read_only_key_cannot_replicate(Config) ->
    Body = iolist_to_binary(json:encode(#{<<"target">> => <<"another_db">>})),
    {ok, 403, _, _} = hackney:post(
        url("/db/" ++ binary_to_list(?DB) ++ "/_replicate"),
        h(Config, ro), Body, []),
    ok.

read_only_key_cannot_bulk(Config) ->
    Body = iolist_to_binary(json:encode(#{<<"docs">> => [#{<<"v">> => 1}]})),
    {ok, 403, _, _} = hackney:post(
        url("/db/" ++ binary_to_list(?DB) ++ "/_bulk_docs"),
        h(Config, ro), Body, []),
    ok.

write_key_can_write(Config) ->
    {ok, 201, _, _} = hackney:put(
        url("/db/" ++ binary_to_list(?DB) ++ "/rw_doc"),
        h(Config, rw), <<"{\"v\":42}">>, []),
    ok.

admin_key_can_write(Config) ->
    {ok, 201, _, _} = hackney:put(
        url("/db/" ++ binary_to_list(?DB) ++ "/admin_doc"),
        h(Config, admin), <<"{\"v\":1}">>, []),
    ok.

invalid_query_op_400(Config) ->
    Body = iolist_to_binary(json:encode(#{
        <<"where">> => [#{<<"path">> => [<<"x">>],
                          <<"op">> => <<"nonsense_op_xyz_unlikely_to_collide">>,
                          <<"value">> => 1}]})),
    {ok, 400, _, RespBody} = hackney:post(
        url("/db/" ++ binary_to_list(?DB) ++ "/_find"),
        h(Config, rw), Body, []),
    Err = maps:get(<<"error">>, json:decode(RespBody)),
    true = binary:match(Err, <<"Unsupported query operator">>) =/= nomatch,
    ok.
