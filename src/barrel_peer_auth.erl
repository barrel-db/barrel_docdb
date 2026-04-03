%%%-------------------------------------------------------------------
%%% @doc Ed25519 peer authentication for P2P replication.
%%%
%%% Provides cryptographic authentication for HTTP replication between peers.
%%% Each node has an Ed25519 key pair; public keys are exchanged during peer
%%% discovery via /.well-known/barrel; sync requests are signed.
%%%
%%% Key persistence:
%%% - Private key: data/barrel_docdb/peer_key (chmod 600)
%%% - Public key: data/barrel_docdb/peer_key.pub
%%%
%%% Signature format:
%%% - Canonical string: timestamp|peer_id|method|path|body_hash
%%% - Body hash: SHA-256 of request body (hex-encoded)
%%% - Signature: Ed25519 sign(canonical_string)
%%%
%%% HTTP Headers:
%%% - X-Peer-Id: Node identifier
%%% - X-Peer-Timestamp: Request timestamp (milliseconds)
%%% - X-Peer-Signature: Base64-encoded Ed25519 signature
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_peer_auth).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    init_keys/0,
    get_public_key/0,
    get_public_key_base64/0,
    get_peer_id/0,
    sign_request/4,
    verify_request/5,
    verify_request/6,
    encode_public_key/1,
    decode_public_key/1,
    %% Header helpers
    build_signed_headers/3,
    build_signed_headers/4,
    extract_auth_headers/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(KEY_FILE, "peer_key").
-define(PUB_KEY_FILE, "peer_key.pub").
-define(TIMESTAMP_WINDOW_MS, 300000).  %% 5 minutes

-record(state, {
    private_key :: binary() | undefined,
    public_key :: binary() | undefined,
    peer_id :: binary() | undefined
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the peer auth gen_server.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Initialize keys (called at startup).
%% Generates new key pair if not exists, or loads existing keys.
-spec init_keys() -> ok | {error, term()}.
init_keys() ->
    gen_server:call(?SERVER, init_keys).

%% @doc Get the local node's public key (binary).
-spec get_public_key() -> {ok, binary()} | {error, not_initialized}.
get_public_key() ->
    gen_server:call(?SERVER, get_public_key).

%% @doc Get the local node's public key as base64.
-spec get_public_key_base64() -> {ok, binary()} | {error, not_initialized}.
get_public_key_base64() ->
    case get_public_key() of
        {ok, PubKey} -> {ok, encode_public_key(PubKey)};
        Error -> Error
    end.

%% @doc Get the local peer ID.
-spec get_peer_id() -> {ok, binary()} | {error, not_initialized}.
get_peer_id() ->
    gen_server:call(?SERVER, get_peer_id).

%% @doc Sign an outgoing request.
%% Returns signature as base64.
-spec sign_request(Method :: binary(), Path :: binary(),
                   Body :: binary() | iolist(), Timestamp :: integer()) ->
    {ok, binary()} | {error, term()}.
sign_request(Method, Path, Body, Timestamp) ->
    gen_server:call(?SERVER, {sign_request, Method, Path, Body, Timestamp}).

%% @doc Verify an incoming request signature.
%% PeerPublicKey should be binary (raw), not base64.
-spec verify_request(PeerPublicKey :: binary(), Method :: binary(),
                     Path :: binary(), Body :: binary() | iolist(),
                     Headers :: map() | [{binary(), binary()}]) ->
    ok | {error, term()}.
verify_request(PeerPublicKey, Method, Path, Body, Headers) ->
    do_verify_request(PeerPublicKey, Method, Path, Body, Headers).

%% @doc Verify request with peer lookup function.
%% LookupFun takes PeerId and returns {ok, PublicKey} or {error, not_found}.
-spec verify_request(LookupFun :: fun((binary()) -> {ok, binary()} | {error, term()}),
                     Method :: binary(), Path :: binary(),
                     Body :: binary() | iolist(),
                     Headers :: map() | [{binary(), binary()}],
                     Opts :: map()) ->
    ok | {error, term()}.
verify_request(LookupFun, Method, Path, Body, Headers, _Opts) when is_function(LookupFun, 1) ->
    NormalizedHeaders = normalize_headers(Headers),
    case maps:get(<<"x-peer-id">>, NormalizedHeaders, undefined) of
        undefined ->
            {error, missing_peer_id};
        PeerId ->
            case LookupFun(PeerId) of
                {ok, PeerPublicKey} ->
                    do_verify_request(PeerPublicKey, Method, Path, Body, NormalizedHeaders);
                {error, _} = Err ->
                    Err
            end
    end.

%% @doc Encode a public key to base64.
-spec encode_public_key(binary()) -> binary().
encode_public_key(PubKey) when is_binary(PubKey) ->
    base64:encode(PubKey).

%% @doc Decode a base64 public key to binary.
-spec decode_public_key(binary()) -> {ok, binary()} | {error, invalid_key}.
decode_public_key(B64) when is_binary(B64) ->
    try
        Key = base64:decode(B64),
        %% Ed25519 public keys are 32 bytes
        case byte_size(Key) of
            32 -> {ok, Key};
            _ -> {error, invalid_key}
        end
    catch
        _:_ -> {error, invalid_key}
    end.

%% @doc Build signed headers for an outgoing request.
%% Returns list of {HeaderName, HeaderValue} tuples.
-spec build_signed_headers(Method :: binary(), Path :: binary(),
                           Body :: binary() | iolist()) ->
    [{binary(), binary()}].
build_signed_headers(Method, Path, Body) ->
    build_signed_headers(Method, Path, Body, []).

%% @doc Build signed headers with additional headers.
-spec build_signed_headers(Method :: binary(), Path :: binary(),
                           Body :: binary() | iolist(),
                           ExtraHeaders :: [{binary(), binary()}]) ->
    [{binary(), binary()}].
build_signed_headers(Method, Path, Body, ExtraHeaders) ->
    Timestamp = erlang:system_time(millisecond),
    case get_peer_id() of
        {ok, PeerId} ->
            SignatureHeaders = try sign_request(Method, Path, Body, Timestamp) of
                {ok, Signature} ->
                    [
                        {<<"X-Peer-Id">>, PeerId},
                        {<<"X-Peer-Timestamp">>, integer_to_binary(Timestamp)},
                        {<<"X-Peer-Signature">>, Signature}
                    ];
                {error, Reason} ->
                    logger:warning("Failed to sign request: ~p, sending unsigned", [Reason]),
                    [{<<"X-Peer-Id">>, PeerId}]
            catch
                exit:{noproc, _} ->
                    %% peer_auth gen_server not running
                    [{<<"X-Peer-Id">>, PeerId}];
                _:_ ->
                    [{<<"X-Peer-Id">>, PeerId}]
            end,
            SignatureHeaders ++ ExtraHeaders;
        {error, _} ->
            ExtraHeaders
    end.

%% @doc Extract auth headers from a header list or map.
%% Returns map with peer_id, timestamp, signature keys.
-spec extract_auth_headers(Headers :: map() | [{binary(), binary()}]) ->
    #{peer_id => binary(), timestamp => integer(), signature => binary()} | {error, term()}.
extract_auth_headers(Headers) ->
    NormalizedHeaders = normalize_headers(Headers),
    PeerId = maps:get(<<"x-peer-id">>, NormalizedHeaders, undefined),
    TimestampStr = maps:get(<<"x-peer-timestamp">>, NormalizedHeaders, undefined),
    Signature = maps:get(<<"x-peer-signature">>, NormalizedHeaders, undefined),

    case {PeerId, TimestampStr, Signature} of
        {undefined, _, _} -> {error, missing_peer_id};
        {_, undefined, _} -> {error, missing_timestamp};
        {_, _, undefined} -> {error, missing_signature};
        {_, _, _} ->
            #{
                peer_id => PeerId,
                timestamp => binary_to_integer(TimestampStr),
                signature => Signature
            }
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Keys will be initialized on first call to init_keys/0
    %% or automatically if auto_init is configured
    case application:get_env(barrel_docdb, peer_auth_auto_init, true) of
        true ->
            %% Initialize keys on startup
            self() ! auto_init_keys;
        false ->
            ok
    end,
    {ok, #state{}}.

handle_call(init_keys, _From, State) ->
    case do_init_keys() of
        {ok, PrivKey, PubKey, PeerId} ->
            logger:info("Peer auth keys initialized, public key: ~s",
                       [truncate_key(encode_public_key(PubKey))]),
            {reply, ok, State#state{private_key = PrivKey, public_key = PubKey, peer_id = PeerId}};
        {error, Reason} = Err ->
            logger:error("Failed to initialize peer auth keys: ~p", [Reason]),
            {reply, Err, State}
    end;

handle_call(get_public_key, _From, #state{public_key = undefined} = State) ->
    {reply, {error, not_initialized}, State};
handle_call(get_public_key, _From, #state{public_key = PubKey} = State) ->
    {reply, {ok, PubKey}, State};

handle_call(get_peer_id, _From, #state{peer_id = undefined} = State) ->
    {reply, {error, not_initialized}, State};
handle_call(get_peer_id, _From, #state{peer_id = PeerId} = State) ->
    {reply, {ok, PeerId}, State};

handle_call({sign_request, _Method, _Path, _Body, _Timestamp}, _From,
            #state{private_key = undefined} = State) ->
    {reply, {error, not_initialized}, State};
handle_call({sign_request, Method, Path, Body, Timestamp}, _From,
            #state{private_key = PrivKey, peer_id = PeerId} = State) ->
    %% Ensure Body is binary (json:encode returns iolist)
    BodyBin = iolist_to_binary(Body),
    CanonicalString = build_canonical_string(Timestamp, PeerId, Method, Path, BodyBin),
    Signature = crypto:sign(eddsa, none, CanonicalString, [PrivKey, ed25519]),
    {reply, {ok, base64:encode(Signature)}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(auto_init_keys, State) ->
    case State#state.private_key of
        undefined ->
            case do_init_keys() of
                {ok, PrivKey, PubKey, PeerId} ->
                    logger:info("Peer auth keys auto-initialized, public key: ~s",
                               [truncate_key(encode_public_key(PubKey))]),
                    {noreply, State#state{private_key = PrivKey, public_key = PubKey, peer_id = PeerId}};
                {error, Reason} ->
                    logger:error("Failed to auto-initialize peer auth keys: ~p", [Reason]),
                    {noreply, State}
            end;
        _ ->
            %% Already initialized
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Initialize or load keys.
do_init_keys() ->
    DataPath = get_data_path(),
    PrivKeyFile = filename:join(DataPath, ?KEY_FILE),
    PubKeyFile = filename:join(DataPath, ?PUB_KEY_FILE),

    %% Ensure data directory exists
    ok = filelib:ensure_dir(PrivKeyFile),

    case filelib:is_file(PrivKeyFile) of
        true ->
            %% Load existing keys
            load_keys(PrivKeyFile, PubKeyFile);
        false ->
            %% Generate new keys
            generate_and_save_keys(PrivKeyFile, PubKeyFile)
    end.

%% @doc Load existing keys from disk.
load_keys(PrivKeyFile, PubKeyFile) ->
    case {file:read_file(PrivKeyFile), file:read_file(PubKeyFile)} of
        {{ok, PrivKey}, {ok, PubKey}} when byte_size(PrivKey) =:= 32,
                                           byte_size(PubKey) =:= 32 ->
            logger:info("Loaded existing peer auth keys from ~s", [PrivKeyFile]),
            PeerId = get_local_peer_id(),
            {ok, PrivKey, PubKey, PeerId};
        {{ok, _}, {ok, _}} ->
            {error, invalid_key_size};
        {{error, Reason}, _} ->
            {error, {read_private_key, Reason}};
        {_, {error, Reason}} ->
            {error, {read_public_key, Reason}}
    end.

%% @doc Generate new Ed25519 key pair and save to disk.
generate_and_save_keys(PrivKeyFile, PubKeyFile) ->
    %% Generate Ed25519 key pair
    {PubKey, PrivKey} = crypto:generate_key(eddsa, ed25519),

    %% Save private key with restrictive permissions
    case file:write_file(PrivKeyFile, PrivKey) of
        ok ->
            %% Set file permissions to 600 (owner read/write only)
            case file:change_mode(PrivKeyFile, 8#600) of
                ok -> ok;
                {error, ChmodReason} ->
                    logger:warning("Could not set permissions on ~s: ~p", [PrivKeyFile, ChmodReason])
            end,

            %% Save public key
            case file:write_file(PubKeyFile, PubKey) of
                ok ->
                    logger:info("Generated new peer auth keys, saved to ~s", [PrivKeyFile]),
                    PeerId = get_local_peer_id(),
                    {ok, PrivKey, PubKey, PeerId};
                {error, PubKeyReason} ->
                    {error, {write_public_key, PubKeyReason}}
            end;
        {error, PrivKeyReason} ->
            {error, {write_private_key, PrivKeyReason}}
    end.

%% @doc Build canonical string for signing/verification.
%% Format: timestamp|peer_id|method|path|body_hash
build_canonical_string(Timestamp, PeerId, Method, Path, Body) ->
    BodyHash = crypto:hash(sha256, Body),
    BodyHashHex = binary:encode_hex(BodyHash, lowercase),
    TimestampBin = integer_to_binary(Timestamp),

    <<TimestampBin/binary, "|",
      PeerId/binary, "|",
      Method/binary, "|",
      Path/binary, "|",
      BodyHashHex/binary>>.

%% @doc Check if timestamp is within acceptable window.
check_timestamp(Timestamp) ->
    Now = erlang:system_time(millisecond),
    Diff = abs(Now - Timestamp),

    case Diff =< ?TIMESTAMP_WINDOW_MS of
        true -> ok;
        false -> {error, timestamp_expired}
    end.

%% @doc Perform the actual request verification.
do_verify_request(PeerPublicKey, Method, Path, Body, Headers) ->
    NormalizedHeaders = normalize_headers(Headers),

    %% Extract signature headers
    PeerId = maps:get(<<"x-peer-id">>, NormalizedHeaders, undefined),
    TimestampStr = maps:get(<<"x-peer-timestamp">>, NormalizedHeaders, undefined),
    SignatureB64 = maps:get(<<"x-peer-signature">>, NormalizedHeaders, undefined),

    case {PeerId, TimestampStr, SignatureB64} of
        {undefined, _, _} ->
            {error, missing_peer_id};
        {_, undefined, _} ->
            {error, missing_timestamp};
        {_, _, undefined} ->
            {error, missing_signature};
        {_, _, _} ->
            %% Parse timestamp
            Timestamp = binary_to_integer(TimestampStr),

            %% Check timestamp freshness
            case check_timestamp(Timestamp) of
                ok ->
                    %% Verify signature
                    Signature = base64:decode(SignatureB64),
                    BodyBin = iolist_to_binary(Body),
                    CanonicalString = build_canonical_string(Timestamp, PeerId, Method, Path, BodyBin),

                    case crypto:verify(eddsa, none, CanonicalString, Signature,
                                       [PeerPublicKey, ed25519]) of
                        true -> ok;
                        false -> {error, invalid_signature}
                    end;
                {error, _} = Err ->
                    Err
            end
    end.

%% @doc Normalize headers to lowercase key map.
normalize_headers(Headers) when is_map(Headers) ->
    maps:fold(
        fun(K, V, Acc) ->
            LowerK = string:lowercase(K),
            maps:put(LowerK, V, Acc)
        end,
        #{},
        Headers
    );
normalize_headers(Headers) when is_list(Headers) ->
    lists:foldl(
        fun({K, V}, Acc) ->
            LowerK = string:lowercase(K),
            maps:put(LowerK, V, Acc)
        end,
        #{},
        Headers
    ).

%% @doc Get the data directory path.
get_data_path() ->
    application:get_env(barrel_docdb, data_dir, "data/barrel_docdb").

%% @doc Get local peer ID.
%% Uses node_id from barrel_discovery if available.
get_local_peer_id() ->
  {ok, NodeId} = barrel_discovery:node_id(),
  NodeId.

%% @doc Truncate key for logging (show first 16 chars + "...").
truncate_key(Key) when byte_size(Key) > 16 ->
    <<Prefix:16/binary, _/binary>> = Key,
    <<Prefix/binary, "...">>;
truncate_key(Key) ->
    Key.
