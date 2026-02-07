%%%-------------------------------------------------------------------
%%% @doc JWT validation for barreldb-console tokens
%%%
%%% Validates JWT tokens with the `bdb_` prefix, signed by barreldb-console
%%% using ES256 (ECDSA with P-256 and SHA-256).
%%%
%%% == Token Format ==
%%% Tokens have the format: `bdb_<base64-encoded-JWT>'
%%%
%%% == JWT Claims ==
%%% Required claims:
%%% - `sub': Workspace key ID (e.g., "wkey_xxx...")
%%% - `typ': Token type, must be "docdb" for barrel_docdb
%%% - `oid': Organization ID
%%% - `prm': Permissions list (e.g., ["read", "write"])
%%% - `exp': Expiry timestamp (Unix seconds)
%%%
%%% Optional claims:
%%% - `wid': Workspace ID (null = all workspaces, or specific "inst_xxx")
%%% - `iat': Issued at timestamp
%%%
%%% == Configuration ==
%%% Configure the console public key in sys.config:
%%% ```
%%% {barrel_docdb, [
%%%     {console_public_key, "/path/to/console_public_key.pem"},
%%%     %% or inline PEM:
%%%     {console_public_key_pem, <<"-----BEGIN PUBLIC KEY-----\n...">>}
%%% ]}
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_jwt).

%% API
-export([
    init/0,
    validate_token/1,
    validate_token/2,
    check_permission/2,
    is_configured/0
]).

%% For testing
-export([
    generate_test_keypair/0,
    sign_test_jwt/2
]).

-define(PT_PUBLIC_KEY, barrel_docdb_jwt_public_key).
-define(TOKEN_PREFIX, <<"bdb_">>).
-define(REQUIRED_TYP, <<"docdb">>).

%%====================================================================
%% API
%%====================================================================

%% @doc Initialize the JWT module by loading the public key.
%% Should be called at application startup.
-spec init() -> ok | {error, term()}.
init() ->
    case load_public_key() of
        {ok, JWK} ->
            persistent_term:put(?PT_PUBLIC_KEY, JWK),
            logger:info("barrel_docdb_jwt: Console public key loaded"),
            ok;
        {error, not_configured} ->
            logger:info("barrel_docdb_jwt: No console public key configured (JWT auth disabled)"),
            ok;
        {error, Reason} ->
            logger:error("barrel_docdb_jwt: Failed to load public key: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Check if JWT authentication is configured.
-spec is_configured() -> boolean().
is_configured() ->
    try
        _ = persistent_term:get(?PT_PUBLIC_KEY),
        true
    catch
        error:badarg -> false
    end.

%% @doc Validate a bdb_ token (without database context).
%% Returns auth context on success.
-spec validate_token(binary()) -> {ok, map()} | {error, term()}.
validate_token(Token) ->
    validate_token(Token, undefined).

%% @doc Validate a bdb_ token for a specific database.
%% Returns auth context on success.
-spec validate_token(binary(), binary() | undefined) -> {ok, map()} | {error, term()}.
validate_token(Token, _DbName) ->
    case strip_prefix(Token) of
        {ok, JwtBinary} ->
            case get_public_key() of
                {ok, JWK} ->
                    verify_and_decode(JwtBinary, JWK);
                {error, not_configured} ->
                    {error, jwt_not_configured}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Check if auth context has required permission.
-spec check_permission(map(), binary()) -> boolean().
check_permission(#{permissions := Permissions}, RequiredPermission) ->
    lists:member(RequiredPermission, Permissions);
check_permission(_, _) ->
    false.

%%====================================================================
%% Test Helpers
%%====================================================================

%% @doc Generate a test ES256 keypair.
%% Returns {PrivateKeyBinary, PublicKeyPEM}.
-spec generate_test_keypair() -> {binary(), binary()}.
generate_test_keypair() ->
    %% Generate EC key using crypto directly (compatible with OTP 28+)
    {PublicKey, PrivateKey} = crypto:generate_key(ecdh, secp256r1),

    %% Build PEM for public key
    %% secp256r1 = P-256, key size is 32 bytes for d, x, y
    <<4:8, X:32/binary, Y:32/binary>> = PublicKey,

    PublicJWKMap = #{
        <<"kty">> => <<"EC">>,
        <<"crv">> => <<"P-256">>,
        <<"x">> => base64url_encode(X),
        <<"y">> => base64url_encode(Y)
    },

    %% Convert to PEM via jose
    PublicJWK = jose_jwk:from_map(PublicJWKMap),
    {_, PublicPem} = jose_jwk:to_pem(PublicJWK),

    {PrivateKey, PublicPem}.

%% @private
%% Base64url encode without padding
base64url_encode(Data) when is_binary(Data) ->
    B64 = base64:encode(Data),
    %% Remove padding and replace + with - and / with _
    NoPad = binary:replace(B64, <<"=">>, <<>>, [global]),
    NoPad1 = binary:replace(NoPad, <<"+">>, <<"-">>, [global]),
    binary:replace(NoPad1, <<"/">>, <<"_">>, [global]);
base64url_encode(IoList) ->
    base64url_encode(iolist_to_binary(IoList)).

%% @doc Sign a JWT with test claims using raw EC private key.
%% PrivateKey should be the 32-byte private key binary.
%% Claims should be a map with sub, typ, wid, oid, prm, exp, etc.
-spec sign_test_jwt(binary(), map()) -> binary().
sign_test_jwt(PrivateKey, Claims) when is_binary(PrivateKey) ->
    %% Build JWT manually
    Header = #{<<"alg">> => <<"ES256">>, <<"typ">> => <<"JWT">>},
    HeaderB64 = base64url_encode(json:encode(Header)),
    PayloadB64 = base64url_encode(json:encode(Claims)),
    SigningInput = <<HeaderB64/binary, ".", PayloadB64/binary>>,

    %% Sign with crypto using P-256 (secp256r1)
    Signature = crypto:sign(ecdsa, sha256, SigningInput, [PrivateKey, secp256r1]),

    %% Convert DER signature to raw R||S format (64 bytes for P-256)
    RawSignature = der_to_raw_signature(Signature),
    SignatureB64 = base64url_encode(RawSignature),

    <<SigningInput/binary, ".", SignatureB64/binary>>.

%% @private
%% Convert DER-encoded ECDSA signature to raw R||S format
der_to_raw_signature(DerSig) ->
    %% DER format: 0x30 <len> 0x02 <r_len> <r> 0x02 <s_len> <s>
    <<16#30, _TotalLen, 16#02, RLen, R:RLen/binary, 16#02, SLen, S:SLen/binary>> = DerSig,
    %% Pad R and S to 32 bytes each
    RPadded = pad_to_32(R),
    SPadded = pad_to_32(S),
    <<RPadded/binary, SPadded/binary>>.

%% @private
%% Pad or trim to exactly 32 bytes
pad_to_32(Bin) when byte_size(Bin) =:= 32 ->
    Bin;
pad_to_32(Bin) when byte_size(Bin) < 32 ->
    Padding = 32 - byte_size(Bin),
    <<0:(Padding*8), Bin/binary>>;
pad_to_32(Bin) when byte_size(Bin) > 32 ->
    %% Remove leading zeros if longer than 32
    binary:part(Bin, byte_size(Bin) - 32, 32).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
%% Strip the bdb_ prefix from a token.
strip_prefix(<<"bdb_", Rest/binary>>) ->
    {ok, Rest};
strip_prefix(_) ->
    {error, invalid_token_prefix}.

%% @private
%% Get the public key from persistent_term.
get_public_key() ->
    try
        {ok, persistent_term:get(?PT_PUBLIC_KEY)}
    catch
        error:badarg ->
            {error, not_configured}
    end.

%% @private
%% Load the public key from configuration.
load_public_key() ->
    case application:get_env(barrel_docdb, console_public_key_pem) of
        {ok, PemBinary} when is_binary(PemBinary) ->
            parse_pem_key(PemBinary);
        _ ->
            case application:get_env(barrel_docdb, console_public_key) of
                {ok, FilePath} ->
                    load_key_from_file(FilePath);
                undefined ->
                    {error, not_configured}
            end
    end.

%% @private
%% Load key from a PEM file.
load_key_from_file(FilePath) ->
    case file:read_file(FilePath) of
        {ok, PemBinary} ->
            parse_pem_key(PemBinary);
        {error, Reason} ->
            {error, {file_read_error, FilePath, Reason}}
    end.

%% @private
%% Parse a PEM-encoded public key.
parse_pem_key(PemBinary) ->
    try
        JWK = jose_jwk:from_pem(PemBinary),
        {ok, JWK}
    catch
        _:Error ->
            {error, {pem_parse_error, Error}}
    end.

%% @private
%% Verify JWT signature and decode claims.
verify_and_decode(JwtBinary, JWK) ->
    try
        case jose_jwt:verify(JWK, JwtBinary) of
            {true, {jose_jwt, Claims}, _JWS} ->
                validate_claims(Claims);
            {false, _, _} ->
                {error, invalid_signature}
        end
    catch
        _:Error ->
            {error, {jwt_decode_error, Error}}
    end.

%% @private
%% Validate JWT claims.
validate_claims(Claims) ->
    case validate_required_claims(Claims) of
        ok ->
            case validate_expiry(Claims) of
                ok ->
                    case validate_type(Claims) of
                        ok ->
                            build_auth_context(Claims);
                        {error, _} = E -> E
                    end;
                {error, _} = E -> E
            end;
        {error, _} = E -> E
    end.

%% @private
%% Validate that all required claims are present.
validate_required_claims(Claims) ->
    Required = [<<"sub">>, <<"typ">>, <<"oid">>, <<"prm">>, <<"exp">>],
    Missing = [K || K <- Required, not maps:is_key(K, Claims)],
    case Missing of
        [] -> ok;
        _ -> {error, {missing_claims, Missing}}
    end.

%% @private
%% Validate that the token hasn't expired.
validate_expiry(#{<<"exp">> := Exp}) ->
    Now = erlang:system_time(second),
    if
        Exp > Now -> ok;
        true -> {error, token_expired}
    end.

%% @private
%% Validate the token type is "docdb".
validate_type(#{<<"typ">> := ?REQUIRED_TYP}) ->
    ok;
validate_type(#{<<"typ">> := Typ}) ->
    {error, {invalid_type, Typ}};
validate_type(_) ->
    {error, missing_type}.

%% @private
%% Build the auth context from validated claims.
build_auth_context(Claims) ->
    #{
        <<"sub">> := Sub,
        <<"oid">> := OrgId,
        <<"prm">> := Permissions
    } = Claims,

    %% wid can be null (all workspaces) or specific workspace ID
    WorkspaceId = case maps:get(<<"wid">>, Claims, null) of
        null -> all;
        Wid -> Wid
    end,

    AuthContext = #{
        key_id => Sub,
        org_id => OrgId,
        workspace_id => WorkspaceId,
        permissions => Permissions,
        is_admin => lists:member(<<"admin">>, Permissions),
        token_type => jwt
    },
    {ok, AuthContext}.
