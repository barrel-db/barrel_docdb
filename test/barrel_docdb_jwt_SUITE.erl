%%%-------------------------------------------------------------------
%%% @doc JWT Validation Test Suite
%%%
%%% Tests for barrel_docdb_jwt module.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_jwt_SUITE).

-include_lib("common_test/include/ct.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    valid_token/1,
    expired_token/1,
    invalid_signature/1,
    wrong_type/1,
    workspace_null/1,
    workspace_specific/1,
    permission_check/1,
    missing_claims/1,
    invalid_prefix/1,
    not_configured/1
]).

-define(PT_TEST_KEYS, barrel_docdb_jwt_test_keys).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, jwt_validation}].

groups() ->
    [
        {jwt_validation, [sequence], [
            valid_token,
            expired_token,
            invalid_signature,
            wrong_type,
            workspace_null,
            workspace_specific,
            permission_check,
            missing_claims,
            invalid_prefix,
            not_configured
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(jose),
    application:ensure_all_started(barrel_docdb),

    %% Configure jose to use OTP json module
    jose:json_module(json),

    %% Generate test keypair
    {PrivateKey, PublicPem} = barrel_docdb_jwt:generate_test_keypair(),

    %% Store keys in persistent_term for tests
    persistent_term:put(?PT_TEST_KEYS, {PrivateKey, PublicPem}),

    %% Configure the public key for JWT validation
    application:set_env(barrel_docdb, console_public_key_pem, PublicPem),

    %% Initialize JWT module
    ok = barrel_docdb_jwt:init(),

    [{private_key, PrivateKey}, {public_pem, PublicPem} | Config].

end_per_suite(_Config) ->
    persistent_term:erase(?PT_TEST_KEYS),
    application:unset_env(barrel_docdb, console_public_key_pem),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

%% @doc Test valid JWT with typ=docdb validates correctly
valid_token(Config) ->
    PrivateKey = proplists:get_value(private_key, Config),

    Claims = #{
        <<"sub">> => <<"wkey_test-12345">>,
        <<"typ">> => <<"docdb">>,
        <<"wid">> => <<"inst_workspace1">>,
        <<"oid">> => <<"org_test123">>,
        <<"prm">> => [<<"read">>, <<"write">>],
        <<"iat">> => erlang:system_time(second),
        <<"exp">> => erlang:system_time(second) + 3600  %% 1 hour from now
    },

    Jwt = barrel_docdb_jwt:sign_test_jwt(PrivateKey, Claims),
    Token = <<"bdb_", Jwt/binary>>,

    {ok, AuthContext} = barrel_docdb_jwt:validate_token(Token),

    %% Verify auth context
    #{key_id := KeyId, org_id := OrgId, workspace_id := WorkspaceId,
      permissions := Permissions, is_admin := IsAdmin} = AuthContext,

    <<"wkey_test-12345">> = KeyId,
    <<"org_test123">> = OrgId,
    <<"inst_workspace1">> = WorkspaceId,
    [<<"read">>, <<"write">>] = Permissions,
    false = IsAdmin,

    ok.

%% @doc Test expired JWT is rejected
expired_token(Config) ->
    PrivateKey = proplists:get_value(private_key, Config),

    Claims = #{
        <<"sub">> => <<"wkey_test-12345">>,
        <<"typ">> => <<"docdb">>,
        <<"wid">> => null,
        <<"oid">> => <<"org_test123">>,
        <<"prm">> => [<<"read">>],
        <<"iat">> => erlang:system_time(second) - 7200,  %% 2 hours ago
        <<"exp">> => erlang:system_time(second) - 3600   %% 1 hour ago (expired)
    },

    Jwt = barrel_docdb_jwt:sign_test_jwt(PrivateKey, Claims),
    Token = <<"bdb_", Jwt/binary>>,

    {error, token_expired} = barrel_docdb_jwt:validate_token(Token),
    ok.

%% @doc Test JWT signed with wrong key is rejected
invalid_signature(_Config) ->
    %% Generate a different keypair (wrong key)
    {WrongPrivateKey, _} = barrel_docdb_jwt:generate_test_keypair(),

    Claims = #{
        <<"sub">> => <<"wkey_test-12345">>,
        <<"typ">> => <<"docdb">>,
        <<"wid">> => null,
        <<"oid">> => <<"org_test123">>,
        <<"prm">> => [<<"read">>],
        <<"exp">> => erlang:system_time(second) + 3600
    },

    %% Sign with wrong key
    Jwt = barrel_docdb_jwt:sign_test_jwt(WrongPrivateKey, Claims),
    Token = <<"bdb_", Jwt/binary>>,

    {error, invalid_signature} = barrel_docdb_jwt:validate_token(Token),
    ok.

%% @doc Test typ=vectordb is rejected for barrel_docdb
wrong_type(Config) ->
    PrivateKey = proplists:get_value(private_key, Config),

    Claims = #{
        <<"sub">> => <<"wkey_test-12345">>,
        <<"typ">> => <<"vectordb">>,  %% Wrong type!
        <<"wid">> => null,
        <<"oid">> => <<"org_test123">>,
        <<"prm">> => [<<"read">>],
        <<"exp">> => erlang:system_time(second) + 3600
    },

    Jwt = barrel_docdb_jwt:sign_test_jwt(PrivateKey, Claims),
    Token = <<"bdb_", Jwt/binary>>,

    {error, {invalid_type, <<"vectordb">>}} = barrel_docdb_jwt:validate_token(Token),
    ok.

%% @doc Test wid=null grants all workspace access
workspace_null(Config) ->
    PrivateKey = proplists:get_value(private_key, Config),

    Claims = #{
        <<"sub">> => <<"wkey_test-12345">>,
        <<"typ">> => <<"docdb">>,
        <<"wid">> => null,  %% null = all workspaces
        <<"oid">> => <<"org_test123">>,
        <<"prm">> => [<<"read">>, <<"write">>],
        <<"exp">> => erlang:system_time(second) + 3600
    },

    Jwt = barrel_docdb_jwt:sign_test_jwt(PrivateKey, Claims),
    Token = <<"bdb_", Jwt/binary>>,

    {ok, AuthContext} = barrel_docdb_jwt:validate_token(Token),

    %% workspace_id should be 'all' when wid is null
    #{workspace_id := all} = AuthContext,
    ok.

%% @doc Test wid=inst_xxx restricts to specific workspace
workspace_specific(Config) ->
    PrivateKey = proplists:get_value(private_key, Config),

    Claims = #{
        <<"sub">> => <<"wkey_test-12345">>,
        <<"typ">> => <<"docdb">>,
        <<"wid">> => <<"inst_my_workspace">>,  %% Specific workspace
        <<"oid">> => <<"org_test123">>,
        <<"prm">> => [<<"read">>],
        <<"exp">> => erlang:system_time(second) + 3600
    },

    Jwt = barrel_docdb_jwt:sign_test_jwt(PrivateKey, Claims),
    Token = <<"bdb_", Jwt/binary>>,

    {ok, AuthContext} = barrel_docdb_jwt:validate_token(Token),

    %% workspace_id should be the specific ID
    #{workspace_id := <<"inst_my_workspace">>} = AuthContext,
    ok.

%% @doc Test permission checking
permission_check(Config) ->
    PrivateKey = proplists:get_value(private_key, Config),

    %% Token with read and write permissions
    Claims1 = #{
        <<"sub">> => <<"wkey_test-12345">>,
        <<"typ">> => <<"docdb">>,
        <<"wid">> => null,
        <<"oid">> => <<"org_test123">>,
        <<"prm">> => [<<"read">>, <<"write">>],
        <<"exp">> => erlang:system_time(second) + 3600
    },

    Jwt1 = barrel_docdb_jwt:sign_test_jwt(PrivateKey, Claims1),
    Token1 = <<"bdb_", Jwt1/binary>>,

    {ok, AuthContext1} = barrel_docdb_jwt:validate_token(Token1),

    true = barrel_docdb_jwt:check_permission(AuthContext1, <<"read">>),
    true = barrel_docdb_jwt:check_permission(AuthContext1, <<"write">>),
    false = barrel_docdb_jwt:check_permission(AuthContext1, <<"admin">>),

    %% Token with admin permission
    Claims2 = #{
        <<"sub">> => <<"wkey_admin-key">>,
        <<"typ">> => <<"docdb">>,
        <<"wid">> => null,
        <<"oid">> => <<"org_test123">>,
        <<"prm">> => [<<"read">>, <<"write">>, <<"admin">>],
        <<"exp">> => erlang:system_time(second) + 3600
    },

    Jwt2 = barrel_docdb_jwt:sign_test_jwt(PrivateKey, Claims2),
    Token2 = <<"bdb_", Jwt2/binary>>,

    {ok, AuthContext2} = barrel_docdb_jwt:validate_token(Token2),

    true = barrel_docdb_jwt:check_permission(AuthContext2, <<"admin">>),
    true = AuthContext2 =:= #{} orelse maps:get(is_admin, AuthContext2, false),

    %% Verify is_admin flag is set correctly
    #{is_admin := true} = AuthContext2,

    ok.

%% @doc Test missing required claims are rejected
missing_claims(Config) ->
    PrivateKey = proplists:get_value(private_key, Config),

    %% Missing 'sub' claim
    Claims1 = #{
        <<"typ">> => <<"docdb">>,
        <<"oid">> => <<"org_test123">>,
        <<"prm">> => [<<"read">>],
        <<"exp">> => erlang:system_time(second) + 3600
    },

    Jwt1 = barrel_docdb_jwt:sign_test_jwt(PrivateKey, Claims1),
    Token1 = <<"bdb_", Jwt1/binary>>,

    {error, {missing_claims, MissingList}} = barrel_docdb_jwt:validate_token(Token1),
    true = lists:member(<<"sub">>, MissingList),

    %% Missing 'prm' claim
    Claims2 = #{
        <<"sub">> => <<"wkey_test-12345">>,
        <<"typ">> => <<"docdb">>,
        <<"oid">> => <<"org_test123">>,
        <<"exp">> => erlang:system_time(second) + 3600
    },

    Jwt2 = barrel_docdb_jwt:sign_test_jwt(PrivateKey, Claims2),
    Token2 = <<"bdb_", Jwt2/binary>>,

    {error, {missing_claims, MissingList2}} = barrel_docdb_jwt:validate_token(Token2),
    true = lists:member(<<"prm">>, MissingList2),

    ok.

%% @doc Test token without bdb_ prefix is rejected
invalid_prefix(_Config) ->
    %% Token without prefix
    {error, invalid_token_prefix} = barrel_docdb_jwt:validate_token(<<"some.jwt.token">>),

    %% Token with wrong prefix
    {error, invalid_token_prefix} = barrel_docdb_jwt:validate_token(<<"ak_sometoken">>),

    ok.

%% @doc Test behavior when JWT is not configured
not_configured(_Config) ->
    %% Temporarily remove the public key config
    {ok, OriginalPem} = application:get_env(barrel_docdb, console_public_key_pem),
    application:unset_env(barrel_docdb, console_public_key_pem),

    %% Erase the persistent_term
    OriginalJwk = persistent_term:get(barrel_docdb_jwt_public_key),
    persistent_term:erase(barrel_docdb_jwt_public_key),

    %% Now validation should return jwt_not_configured
    {error, jwt_not_configured} = barrel_docdb_jwt:validate_token(<<"bdb_any.jwt.token">>),

    %% is_configured should return false
    false = barrel_docdb_jwt:is_configured(),

    %% Restore config
    application:set_env(barrel_docdb, console_public_key_pem, OriginalPem),
    persistent_term:put(barrel_docdb_jwt_public_key, OriginalJwk),

    %% Now is_configured should return true
    true = barrel_docdb_jwt:is_configured(),

    ok.
