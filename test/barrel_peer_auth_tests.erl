%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_peer_auth module.
%%%
%%% Tests Ed25519 peer authentication for P2P replication.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_peer_auth_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test fixtures
%%====================================================================

peer_auth_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
        {"key generation", fun test_key_generation/0},
        {"sign and verify request", fun test_sign_verify_request/0},
        {"verify with wrong key fails", fun test_verify_wrong_key/0},
        {"verify with expired timestamp fails", fun test_verify_expired_timestamp/0},
        {"verify with tampered body fails", fun test_verify_tampered_body/0},
        {"build signed headers", fun test_build_signed_headers/0},
        {"extract auth headers", fun test_extract_auth_headers/0},
        {"encode decode public key", fun test_encode_decode_public_key/0}
     ]}.

setup() ->
    %% Start required applications
    application:ensure_all_started(crypto),

    %% Set test data path
    TestDataDir = filename:join(["/tmp", "barrel_peer_auth_test_" ++ integer_to_list(erlang:system_time())]),
    ok = filelib:ensure_dir(filename:join(TestDataDir, "dummy")),
    application:set_env(barrel_docdb, data_dir, TestDataDir),

    %% Mock barrel_discovery:node_id/0
    meck:new(barrel_discovery, [passthrough, non_strict]),
    meck:expect(barrel_discovery, node_id, fun() -> {ok, <<"test-node-123">>} end),

    %% Start peer auth
    {ok, Pid} = barrel_peer_auth:start_link(),
    ok = barrel_peer_auth:init_keys(),

    #{pid => Pid, data_dir => TestDataDir}.

teardown(#{pid := Pid, data_dir := DataDir}) ->
    %% Stop peer auth
    gen_server:stop(Pid),

    %% Clean up meck
    meck:unload(barrel_discovery),

    %% Clean up test data directory
    os:cmd("rm -rf " ++ DataDir),
    ok.

%%====================================================================
%% Tests
%%====================================================================

test_key_generation() ->
    %% Should have public key after init
    {ok, PubKey} = barrel_peer_auth:get_public_key(),
    ?assertEqual(32, byte_size(PubKey)),

    %% Should have base64 public key
    {ok, PubKeyB64} = barrel_peer_auth:get_public_key_base64(),
    ?assert(is_binary(PubKeyB64)),
    ?assert(byte_size(PubKeyB64) > 32),

    %% Decode should give same key
    {ok, Decoded} = barrel_peer_auth:decode_public_key(PubKeyB64),
    ?assertEqual(PubKey, Decoded).

test_sign_verify_request() ->
    {ok, PubKey} = barrel_peer_auth:get_public_key(),

    Method = <<"POST">>,
    Path = <<"/db/mydb/_changes">>,
    Body = <<"{\"since\":\"now\"}">>,
    Timestamp = erlang:system_time(millisecond),

    %% Sign request
    {ok, Signature} = barrel_peer_auth:sign_request(Method, Path, Body, Timestamp),
    ?assert(is_binary(Signature)),

    %% Build headers map
    Headers = #{
        <<"x-peer-id">> => <<"test-node-123">>,
        <<"x-peer-timestamp">> => integer_to_binary(Timestamp),
        <<"x-peer-signature">> => Signature
    },

    %% Verify should succeed
    ?assertEqual(ok, barrel_peer_auth:verify_request(PubKey, Method, Path, Body, Headers)).

test_verify_wrong_key() ->
    %% Generate a different key pair
    {WrongPubKey, _WrongPrivKey} = crypto:generate_key(eddsa, ed25519),

    Method = <<"GET">>,
    Path = <<"/db/test">>,
    Body = <<>>,
    Timestamp = erlang:system_time(millisecond),

    {ok, Signature} = barrel_peer_auth:sign_request(Method, Path, Body, Timestamp),

    Headers = #{
        <<"x-peer-id">> => <<"test-node-123">>,
        <<"x-peer-timestamp">> => integer_to_binary(Timestamp),
        <<"x-peer-signature">> => Signature
    },

    %% Verify with wrong key should fail
    ?assertEqual({error, invalid_signature},
                 barrel_peer_auth:verify_request(WrongPubKey, Method, Path, Body, Headers)).

test_verify_expired_timestamp() ->
    {ok, PubKey} = barrel_peer_auth:get_public_key(),

    Method = <<"GET">>,
    Path = <<"/db/test">>,
    Body = <<>>,
    %% Timestamp from 10 minutes ago (beyond 5 minute window)
    Timestamp = erlang:system_time(millisecond) - 600000,

    {ok, Signature} = barrel_peer_auth:sign_request(Method, Path, Body, Timestamp),

    Headers = #{
        <<"x-peer-id">> => <<"test-node-123">>,
        <<"x-peer-timestamp">> => integer_to_binary(Timestamp),
        <<"x-peer-signature">> => Signature
    },

    %% Verify with expired timestamp should fail
    ?assertEqual({error, timestamp_expired},
                 barrel_peer_auth:verify_request(PubKey, Method, Path, Body, Headers)).

test_verify_tampered_body() ->
    {ok, PubKey} = barrel_peer_auth:get_public_key(),

    Method = <<"POST">>,
    Path = <<"/db/test/_changes">>,
    OriginalBody = <<"{\"since\":\"now\"}">>,
    TamperedBody = <<"{\"since\":\"0\"}">>,
    Timestamp = erlang:system_time(millisecond),

    %% Sign with original body
    {ok, Signature} = barrel_peer_auth:sign_request(Method, Path, OriginalBody, Timestamp),

    Headers = #{
        <<"x-peer-id">> => <<"test-node-123">>,
        <<"x-peer-timestamp">> => integer_to_binary(Timestamp),
        <<"x-peer-signature">> => Signature
    },

    %% Verify with tampered body should fail
    ?assertEqual({error, invalid_signature},
                 barrel_peer_auth:verify_request(PubKey, Method, Path, TamperedBody, Headers)).

test_build_signed_headers() ->
    Headers = barrel_peer_auth:build_signed_headers(<<"GET">>, <<"/test">>, <<>>),

    %% Should have peer auth headers
    ?assert(lists:keymember(<<"X-Peer-Id">>, 1, Headers)),
    ?assert(lists:keymember(<<"X-Peer-Timestamp">>, 1, Headers)),
    ?assert(lists:keymember(<<"X-Peer-Signature">>, 1, Headers)),

    %% Check values
    {_, PeerId} = lists:keyfind(<<"X-Peer-Id">>, 1, Headers),
    ?assertEqual(<<"test-node-123">>, PeerId),

    {_, Timestamp} = lists:keyfind(<<"X-Peer-Timestamp">>, 1, Headers),
    ?assert(is_binary(Timestamp)),

    {_, Signature} = lists:keyfind(<<"X-Peer-Signature">>, 1, Headers),
    ?assert(is_binary(Signature)),
    ?assert(byte_size(Signature) > 50).  %% Base64 encoded signature

test_extract_auth_headers() ->
    Timestamp = erlang:system_time(millisecond),

    Headers = #{
        <<"x-peer-id">> => <<"node-abc">>,
        <<"x-peer-timestamp">> => integer_to_binary(Timestamp),
        <<"x-peer-signature">> => <<"sig123">>
    },

    Result = barrel_peer_auth:extract_auth_headers(Headers),

    ?assertEqual(<<"node-abc">>, maps:get(peer_id, Result)),
    ?assertEqual(Timestamp, maps:get(timestamp, Result)),
    ?assertEqual(<<"sig123">>, maps:get(signature, Result)).

test_encode_decode_public_key() ->
    %% Valid 32-byte key
    Key = crypto:strong_rand_bytes(32),
    Encoded = barrel_peer_auth:encode_public_key(Key),
    ?assert(is_binary(Encoded)),

    {ok, Decoded} = barrel_peer_auth:decode_public_key(Encoded),
    ?assertEqual(Key, Decoded),

    %% Invalid key (wrong size)
    ShortKey = crypto:strong_rand_bytes(16),
    ShortEncoded = base64:encode(ShortKey),
    ?assertEqual({error, invalid_key}, barrel_peer_auth:decode_public_key(ShortEncoded)),

    %% Invalid base64
    ?assertEqual({error, invalid_key}, barrel_peer_auth:decode_public_key(<<"not-valid-base64!!!">>)).

%%====================================================================
%% Standalone tests (no fixture needed)
%%====================================================================

canonical_string_test() ->
    %% Test that canonical string is built correctly
    %% This tests the internal function behavior indirectly

    %% Start a temporary peer auth for this test
    application:ensure_all_started(crypto),
    TestDataDir = filename:join(["/tmp", "barrel_peer_auth_canonical_test_" ++ integer_to_list(erlang:system_time())]),
    ok = filelib:ensure_dir(filename:join(TestDataDir, "dummy")),
    application:set_env(barrel_docdb, data_dir, TestDataDir),

    meck:new(barrel_discovery, [passthrough, non_strict]),
    meck:expect(barrel_discovery, node_id, fun() -> {ok, <<"canonical-test-node">>} end),

    {ok, Pid} = barrel_peer_auth:start_link(),
    ok = barrel_peer_auth:init_keys(),

    try
        {ok, _PubKey} = barrel_peer_auth:get_public_key(),

        %% Sign same request twice with same timestamp should give same signature
        Method = <<"POST">>,
        Path = <<"/db/test">>,
        Body = <<"test-body">>,
        Timestamp = 1707321600000,

        {ok, Sig1} = barrel_peer_auth:sign_request(Method, Path, Body, Timestamp),
        {ok, Sig2} = barrel_peer_auth:sign_request(Method, Path, Body, Timestamp),

        %% Ed25519 is deterministic, so signatures should be identical
        ?assertEqual(Sig1, Sig2),

        %% Different body should give different signature
        {ok, Sig3} = barrel_peer_auth:sign_request(Method, Path, <<"different">>, Timestamp),
        ?assertNotEqual(Sig1, Sig3),

        %% Different path should give different signature
        {ok, Sig4} = barrel_peer_auth:sign_request(Method, <<"/other">>, Body, Timestamp),
        ?assertNotEqual(Sig1, Sig4),

        %% Different method should give different signature
        {ok, Sig5} = barrel_peer_auth:sign_request(<<"GET">>, Path, Body, Timestamp),
        ?assertNotEqual(Sig1, Sig5)
    after
        gen_server:stop(Pid),
        meck:unload(barrel_discovery),
        os:cmd("rm -rf " ++ TestDataDir)
    end.

verify_with_lookup_fun_test() ->
    %% Test verify_request/6 with lookup function
    application:ensure_all_started(crypto),
    TestDataDir = filename:join(["/tmp", "barrel_peer_auth_lookup_test_" ++ integer_to_list(erlang:system_time())]),
    ok = filelib:ensure_dir(filename:join(TestDataDir, "dummy")),
    application:set_env(barrel_docdb, data_dir, TestDataDir),

    meck:new(barrel_discovery, [passthrough, non_strict]),
    meck:expect(barrel_discovery, node_id, fun() -> {ok, <<"lookup-test-node">>} end),

    {ok, Pid} = barrel_peer_auth:start_link(),
    ok = barrel_peer_auth:init_keys(),

    try
        {ok, PubKey} = barrel_peer_auth:get_public_key(),

        Method = <<"GET">>,
        Path = <<"/test">>,
        Body = <<>>,
        Timestamp = erlang:system_time(millisecond),

        {ok, Signature} = barrel_peer_auth:sign_request(Method, Path, Body, Timestamp),

        Headers = #{
            <<"x-peer-id">> => <<"lookup-test-node">>,
            <<"x-peer-timestamp">> => integer_to_binary(Timestamp),
            <<"x-peer-signature">> => Signature
        },

        %% Lookup function that returns the public key
        LookupOk = fun(<<"lookup-test-node">>) -> {ok, PubKey};
                      (_) -> {error, not_found}
                   end,

        %% Should verify successfully
        ?assertEqual(ok, barrel_peer_auth:verify_request(LookupOk, Method, Path, Body, Headers, #{})),

        %% Lookup function that returns not_found
        LookupFail = fun(_) -> {error, not_found} end,

        %% Should fail with not_found
        ?assertEqual({error, not_found},
                     barrel_peer_auth:verify_request(LookupFail, Method, Path, Body, Headers, #{}))
    after
        gen_server:stop(Pid),
        meck:unload(barrel_discovery),
        os:cmd("rm -rf " ++ TestDataDir)
    end.

proplist_headers_test() ->
    %% Test that verify_request works with proplist headers (as returned by hackney)
    application:ensure_all_started(crypto),
    TestDataDir = filename:join(["/tmp", "barrel_peer_auth_proplist_test_" ++ integer_to_list(erlang:system_time())]),
    ok = filelib:ensure_dir(filename:join(TestDataDir, "dummy")),
    application:set_env(barrel_docdb, data_dir, TestDataDir),

    meck:new(barrel_discovery, [passthrough, non_strict]),
    meck:expect(barrel_discovery, node_id, fun() -> {ok, <<"proplist-test-node">>} end),

    {ok, Pid} = barrel_peer_auth:start_link(),
    ok = barrel_peer_auth:init_keys(),

    try
        {ok, PubKey} = barrel_peer_auth:get_public_key(),

        Method = <<"POST">>,
        Path = <<"/db/test/_changes">>,
        Body = <<"{}">>,
        Timestamp = erlang:system_time(millisecond),

        {ok, Signature} = barrel_peer_auth:sign_request(Method, Path, Body, Timestamp),

        %% Headers as proplist (like hackney returns)
        Headers = [
            {<<"X-Peer-Id">>, <<"proplist-test-node">>},
            {<<"X-Peer-Timestamp">>, integer_to_binary(Timestamp)},
            {<<"X-Peer-Signature">>, Signature},
            {<<"Content-Type">>, <<"application/json">>}
        ],

        %% Should verify successfully
        ?assertEqual(ok, barrel_peer_auth:verify_request(PubKey, Method, Path, Body, Headers))
    after
        gen_server:stop(Pid),
        meck:unload(barrel_discovery),
        os:cmd("rm -rf " ++ TestDataDir)
    end.
