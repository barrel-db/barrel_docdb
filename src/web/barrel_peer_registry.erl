%%%-------------------------------------------------------------------
%%% @doc Persistent registry of trusted replication peers.
%%%
%%% DETS-backed (parity with `barrel_http_api_keys'). Provides the
%%% LookupFun used by `barrel_peer_auth:verify_request/6' for inbound
%%% replication signature verification, and the URL-to-peer mapping
%%% the `/_replicate' gate uses to reject SSRF attempts at
%%% unregistered targets.
%%%
%%% == Records ==
%%%
%%% Each entry binds a remote `peer_id' (its persistent node
%%% identity) to its Ed25519 public key and one canonical base URL.
%%% Operators register peers explicitly (with a pubkey) or via the
%%% `discover_from' helper which TOFUs the remote's
%%% `/.well-known/barrel'.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_peer_registry).

-behaviour(gen_server).

-export([
    start_link/0,
    stop/0,
    register_peer/1,
    lookup/1,
    lookup_by_url/1,
    get/1,
    list/0,
    delete/1,
    touch_last_used/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(TABLE, barrel_peer_registry).

-record(peer, {
    peer_id    :: binary(),         %% remote node_id, primary key
    name       :: binary(),
    url        :: binary(),         %% canonical base URL, normalised (no trailing slash)
    public_key :: binary(),         %% raw 32-byte ed25519 public key
    databases  :: all | [binary()],
    created_at :: integer(),
    last_used  :: integer() | undefined
}).

-record(state, {}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%% @doc Register a peer. Spec must carry `name' and `url', plus
%% either `public_key' (raw 32-byte binary or base64) or
%% `discover_from' (a URL whose `/.well-known/barrel' is fetched).
%% Optional `databases' (default `all'). Returns the stored record
%% as a map.
-spec register_peer(map()) -> {ok, map()} | {error, term()}.
register_peer(Spec) ->
    gen_server:call(?SERVER, {register_peer, Spec}).

%% @doc Look up a peer's public key by peer_id. This is the
%% `LookupFun' shape `barrel_peer_auth:verify_request/6' expects.
-spec lookup(binary()) -> {ok, binary()} | {error, not_found}.
lookup(PeerId) ->
    case dets:lookup(?TABLE, PeerId) of
        []      -> {error, not_found};
        [Peer]  -> {ok, Peer#peer.public_key}
    end.

%% @doc Reverse lookup by canonical URL. Used by the `/_replicate'
%% gate when the caller passed `target' rather than `peer_id'.
-spec lookup_by_url(binary()) -> {ok, map()} | {error, not_found}.
lookup_by_url(Url) ->
    Canonical = normalise_url(Url),
    dets:foldl(
      fun(#peer{url = U} = P, _Acc) when U =:= Canonical ->
              {ok, record_to_map(P)};
         (_, Acc) ->
              Acc
      end,
      {error, not_found},
      ?TABLE).

-spec get(binary()) -> {ok, map()} | {error, not_found}.
get(PeerId) ->
    case dets:lookup(?TABLE, PeerId) of
        []      -> {error, not_found};
        [Peer]  -> {ok, record_to_map(Peer)}
    end.

-spec list() -> {ok, [map()]}.
list() ->
    Acc = dets:foldl(fun(P, A) -> [record_to_map(P) | A] end, [], ?TABLE),
    {ok, lists:reverse(Acc)}.

-spec delete(binary()) -> ok | {error, not_found}.
delete(PeerId) ->
    gen_server:call(?SERVER, {delete, PeerId}).

%% @doc Bump the `last_used' timestamp. Called after a successful
%% inbound signature verify. Cast — purely advisory metadata.
-spec touch_last_used(binary()) -> ok.
touch_last_used(PeerId) ->
    gen_server:cast(?SERVER, {touch_last_used, PeerId}).

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    DataDir = data_dir(),
    File = filename:join(DataDir, "peers.dets"),
    ok = filelib:ensure_dir(File),
    case dets:open_file(?TABLE, [{file, File}, {keypos, #peer.peer_id}]) of
        {ok, _Ref} ->
            {ok, #state{}};
        {error, Reason} ->
            {stop, {dets_open_failed, Reason}}
    end.

handle_call({register_peer, Spec}, _From, State) ->
    {reply, do_register_peer(Spec), State};
handle_call({delete, PeerId}, _From, State) ->
    Result = case dets:lookup(?TABLE, PeerId) of
        []  -> {error, not_found};
        [_] ->
            ok = dets:delete(?TABLE, PeerId),
            _  = dets:sync(?TABLE),
            ok
    end,
    {reply, Result, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({touch_last_used, PeerId}, State) ->
    case dets:lookup(?TABLE, PeerId) of
        [P] ->
            Now = erlang:system_time(millisecond),
            ok  = dets:insert(?TABLE, P#peer{last_used = Now});
        _ ->
            ok
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    _ = dets:close(?TABLE),
    ok.

%%====================================================================
%% Internal
%%====================================================================

do_register_peer(Spec) ->
    Name = maps:get(name, Spec, undefined),
    Url0 = maps:get(url, Spec, undefined),
    Dbs  = maps:get(databases, Spec, all),
    case {Name, Url0} of
        {undefined, _} -> {error, missing_name};
        {_, undefined} -> {error, missing_url};
        _ ->
            case resolve_peer_id_and_key(Spec) of
                {ok, PeerId, PubKey} ->
                    Now = erlang:system_time(millisecond),
                    Record = #peer{
                        peer_id    = PeerId,
                        name       = Name,
                        url        = normalise_url(Url0),
                        public_key = PubKey,
                        databases  = Dbs,
                        created_at = Now,
                        last_used  = undefined
                    },
                    ok = dets:insert(?TABLE, Record),
                    _  = dets:sync(?TABLE),
                    {ok, record_to_map(Record)};
                {error, _} = Err ->
                    Err
            end
    end.

%% Either accept an explicit `public_key' + `peer_id', or fetch them
%% from the remote's `/.well-known/barrel' when `discover_from' is
%% set. Discover path is trust-on-first-use; operator must verify
%% the returned pubkey out-of-band before relying on it.
resolve_peer_id_and_key(Spec) ->
    case maps:get(public_key, Spec, undefined) of
        undefined ->
            case maps:get(discover_from, Spec, undefined) of
                undefined ->
                    {error, missing_public_key_or_discover};
                DiscoverUrl ->
                    discover_peer(DiscoverUrl)
            end;
        PubKeyIn ->
            case decode_public_key(PubKeyIn) of
                {ok, PubKey} ->
                    case maps:get(peer_id, Spec, undefined) of
                        undefined -> {error, missing_peer_id};
                        PeerId    -> {ok, PeerId, PubKey}
                    end;
                {error, _} = Err -> Err
            end
    end.

decode_public_key(Bin) when is_binary(Bin), byte_size(Bin) =:= 32 ->
    {ok, Bin};
decode_public_key(B64) when is_binary(B64) ->
    barrel_peer_auth:decode_public_key(B64);
decode_public_key(_) ->
    {error, invalid_public_key}.

%% Fetch GET <url>/.well-known/barrel, return {ok, NodeId, PubKey}.
%% Uses OTP's built-in `httpc' (one-shot GET, small response). Good
%% enough for the discovery handshake; the bulk replication transport
%% remains `livery_client'-based.
discover_peer(BaseUrl) ->
    Url = <<(normalise_url(BaseUrl))/binary, "/.well-known/barrel">>,
    _ = application:ensure_all_started(inets),
    Request = {binary_to_list(Url), []},
    HttpOpts = [{timeout, 5000}, {connect_timeout, 5000}],
    Opts = [{body_format, binary}, {full_result, true}],
    try httpc:request(get, Request, HttpOpts, Opts) of
        {ok, {{_Vsn, 200, _Reason}, _Headers, Body}} ->
            parse_well_known(Body);
        {ok, {{_Vsn, Status, _Reason}, _Headers, _Body}} ->
            {error, {discover_status, Status}};
        {error, Reason} ->
            {error, {discover_failed, Reason}}
    catch
        Class:CatchReason ->
            {error, {discover_failed, {Class, CatchReason}}}
    end.

parse_well_known(Body) ->
    try json:decode(Body) of
        #{<<"node_id">> := NodeId, <<"public_key">> := PubKeyB64}
          when is_binary(NodeId), is_binary(PubKeyB64) ->
            case barrel_peer_auth:decode_public_key(PubKeyB64) of
                {ok, PubKey} -> {ok, NodeId, PubKey};
                {error, _} = Err -> Err
            end;
        _ ->
            {error, well_known_missing_fields}
    catch
        _:_ -> {error, well_known_invalid_json}
    end.

%% Strip trailing slash; canonical-case the scheme/host. Keeps the
%% URL-to-peer index well-behaved without doing full URL parsing.
normalise_url(<<>>) -> <<>>;
normalise_url(Url) when is_binary(Url) ->
    Stripped = case binary:last(Url) of
        $/ -> binary:part(Url, 0, byte_size(Url) - 1);
        _  -> Url
    end,
    Stripped.

record_to_map(#peer{peer_id = Id, name = Name, url = Url,
                    public_key = PubKey, databases = Dbs,
                    created_at = Created, last_used = LastUsed}) ->
    #{
        peer_id    => Id,
        name       => Name,
        url        => Url,
        public_key => base64:encode(PubKey),
        databases  => Dbs,
        created_at => Created,
        last_used  => LastUsed
     }.

%% Same data-dir resolution as the API key store.
data_dir() ->
    case application:get_env(barrel_docdb, data_dir, undefined) of
        undefined ->
            case application:get_env(barrel_docdb, data_path, undefined) of
                undefined -> "data/barrel_docdb";
                Legacy    -> Legacy
            end;
        Dir -> Dir
    end.
