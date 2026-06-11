%%%-------------------------------------------------------------------
%%% @doc CT for the peer-registry SSRF gate and admin routes.
%%%
%%% Exercises the new `/peers' admin API plus the `/_replicate'
%%% gate with `replication_require_registered_peer = true' (the
%%% default).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_peer_registry_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    register_with_explicit_pubkey/1,
    list_and_get_and_delete/1,
    replicate_unregistered_target_rejected/1,
    replicate_missing_target_or_peer_id/1,
    inbound_revsdiff_without_signature_rejected/1
]).

-define(PORT, 18181).
-define(BASE_URL, "http://localhost:18181").

all() ->
    [
        register_with_explicit_pubkey,
        list_and_get_and_delete,
        replicate_unregistered_target_rejected,
        replicate_missing_target_or_peer_id,
        inbound_revsdiff_without_signature_rejected
    ].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    application:ensure_all_started(livery),
    application:ensure_all_started(hackney),
    %% Strict mode ON — the whole point of this suite.
    application:set_env(barrel_docdb, replication_require_registered_peer, true),
    {ok, HttpPid} = barrel_http_server:start_link(#{port => ?PORT}),
    unlink(HttpPid),
    {ok, AdminKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"peer-registry-suite">>,
        permissions => [<<"read">>, <<"write">>, <<"admin">>],
        is_admin => true
    }),
    {ok, _} = barrel_docdb:create_db(<<"peerdb">>),
    [{api_key, AdminKey} | Config].

end_per_suite(_Config) ->
    try barrel_docdb:delete_db(<<"peerdb">>) catch _:_ -> ok end,
    barrel_http_server:stop(),
    %% Best-effort cleanup of any peer registry rows left over.
    try
        {ok, Peers} = barrel_peer_registry:list(),
        [barrel_peer_registry:delete(maps:get(peer_id, P)) || P <- Peers]
    catch
        _:_ -> ok
    end,
    ok.

%%====================================================================
%% Helpers
%%====================================================================

auth(Config) ->
    Key = proplists:get_value(api_key, Config),
    {<<"Authorization">>, <<"Bearer ", Key/binary>>}.

post_json(Path, Headers, BodyMap) ->
    hackney:post(
        ?BASE_URL ++ Path,
        [{<<"Content-Type">>, <<"application/json">>},
         {<<"Accept">>, <<"application/json">>} | Headers],
        iolist_to_binary(json:encode(BodyMap)),
        []).

get_json(Path, Headers) ->
    hackney:get(?BASE_URL ++ Path,
                [{<<"Accept">>, <<"application/json">>} | Headers],
                <<>>, []).

delete_(Path, Headers) ->
    hackney:delete(?BASE_URL ++ Path, Headers, <<>>, []).

fake_pubkey() ->
    base64:encode(crypto:strong_rand_bytes(32)).

%%====================================================================
%% Cases
%%====================================================================

register_with_explicit_pubkey(Config) ->
    Auth = auth(Config),
    Spec = #{<<"name">>      => <<"east-1">>,
             <<"url">>       => <<"https://east-1.example:8443">>,
             <<"peer_id">>   => <<"node-east-1">>,
             <<"public_key">> => fake_pubkey()},
    {ok, 201, _Headers, Body} = post_json("/peers", [Auth], Spec),
    Decoded = json:decode(Body),
    ?assertEqual(<<"node-east-1">>, maps:get(<<"peer_id">>, Decoded)),
    ?assertEqual(<<"east-1">>,     maps:get(<<"name">>,     Decoded)),
    %% Trailing slash should be stripped (normalised).
    ?assertEqual(<<"https://east-1.example:8443">>,
                 maps:get(<<"url">>, Decoded)),
    ok.

list_and_get_and_delete(Config) ->
    Auth = auth(Config),
    %% Seed
    SeedSpec = #{<<"name">>       => <<"west-1">>,
                 <<"url">>        => <<"https://west-1.example:8443/">>,
                 <<"peer_id">>    => <<"node-west-1">>,
                 <<"public_key">> => fake_pubkey()},
    {ok, 201, _, _} = post_json("/peers", [Auth], SeedSpec),
    %% List
    {ok, 200, _, ListBody} = get_json("/peers", [Auth]),
    Peers = json:decode(ListBody),
    true = lists:any(fun(P) -> maps:get(<<"peer_id">>, P) =:= <<"node-west-1">> end, Peers),
    %% Get one
    {ok, 200, _, GetBody} = get_json("/peers/node-west-1", [Auth]),
    Got = json:decode(GetBody),
    ?assertEqual(<<"node-west-1">>, maps:get(<<"peer_id">>, Got)),
    %% Delete
    {ok, 200, _, _} = delete_("/peers/node-west-1", [Auth]),
    {ok, 404, _, _} = get_json("/peers/node-west-1", [Auth]),
    ok.

%% SSRF gate: an authenticated admin POSTing to /_replicate with a
%% target URL that isn't in the registry must get 403.
replicate_unregistered_target_rejected(Config) ->
    Auth = auth(Config),
    Spec = #{<<"target">> => <<"http://169.254.169.254/latest/meta-data/">>},
    {ok, 403, _Headers, Body} = post_json("/db/peerdb/_replicate", [Auth], Spec),
    Decoded = json:decode(Body),
    ?assertMatch(<<"unregistered_peer", _/binary>>,
                 maps:get(<<"error">>, Decoded)),
    ok.

replicate_missing_target_or_peer_id(Config) ->
    Auth = auth(Config),
    {ok, 400, _, Body} = post_json("/db/peerdb/_replicate", [Auth], #{}),
    Decoded = json:decode(Body),
    ?assertEqual(<<"missing target or peer_id">>,
                 maps:get(<<"error">>, Decoded)),
    ok.

%% Inbound replication-receiving endpoint without X-Peer-* headers
%% must be rejected with 401 once strict mode is on.
inbound_revsdiff_without_signature_rejected(Config) ->
    Auth = auth(Config),
    {ok, 401, _Headers, Body} = post_json(
        "/db/peerdb/_revsdiff",
        [Auth],
        #{<<"id">> => <<"doc1">>, <<"revs">> => [<<"1-a">>]}),
    Decoded = json:decode(Body),
    ?assertEqual(<<"peer_signature_required">>,
                 maps:get(<<"error">>, Decoded)),
    ok.
