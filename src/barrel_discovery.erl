%%%-------------------------------------------------------------------
%%% @doc Peer discovery for federation
%%%
%%% Implements a simple Mastodon-like discovery model:
%%% - Each node has a unique ID and knows about its peers
%%% - Peers are discovered through gossip (exchange of peer lists)
%%% - Node info available at /.well-known/barrel
%%%
%%% Example:
%%% ```
%%% %% Start discovery (usually done by application)
%%% barrel_discovery:start_link().
%%%
%%% %% Add a known peer
%%% ok = barrel_discovery:add_peer(<<"http://other-node:8080">>).
%%%
%%% %% Discover peers from a seed
%%% ok = barrel_discovery:discover_from(<<"http://seed-node:8080">>).
%%%
%%% %% Get all known peers
%%% {ok, Peers} = barrel_discovery:list_peers().
%%%
%%% %% Get this node's info
%%% {ok, Info} = barrel_discovery:node_info().
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_discovery).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    start_link/1,
    stop/0,
    node_info/0,
    node_id/0,
    %% Zone management
    get_zone/0,
    nodes_in_zone/1,
    list_zones/0,
    %% Peer management
    add_peer/1,
    add_peer/2,
    remove_peer/1,
    list_peers/0,
    list_peers/1,
    list_peers_json/0,
    get_peer/1,
    get_peer_json/1,
    get_peer_by_node_id/1,
    %% Discovery
    discover_from/1,
    discover_from_dns/1,
    refresh_peers/0,
    %% DNS domain management
    add_dns_domain/1,
    remove_dns_domain/1,
    list_dns_domains/0,
    %% Peer tagging
    tag_peer/2,
    untag_peer/2,
    list_tags/0,
    %% Federation helpers
    resolve_member/1,
    resolve_peers_with_db/1,
    %% Peer authentication
    get_peer_public_key/1,
    %% JSON encoding
    encode_peer_for_json/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_REFRESH_INTERVAL, 300000).  %% 5 minutes
-define(PEER_TIMEOUT, 10000).  %% 10 seconds for peer requests

-record(state, {
    node_id :: binary(),
    zone :: binary() | undefined,
    refresh_timer :: reference() | undefined,
    refresh_interval :: pos_integer()
}).

-type peer_info() :: #{
    url := binary(),
    node_id => binary(),
    public_key => binary(),           %% Ed25519 public key (raw binary)
    zone => binary(),
    version => binary(),
    databases => [binary()],
    federations => [binary()],
    tags => [binary()],
    last_seen => integer(),
    status => active | unreachable | pending
}.

%% Federation member reference types
-type member_ref() :: binary()                      % Direct URL or local db name
                    | {peer, binary()}              % {peer, NodeId} - any db on peer
                    | {peer, binary(), binary()}    % {peer, NodeId, DbName}
                    | {tag, binary()}               % All peers with tag
                    | {tag, binary(), binary()}     % Peers with tag + specific db
                    | {all_peers, binary()}.        % All peers with specific db

-export_type([peer_info/0, member_ref/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the discovery service with default options
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start the discovery service with options
%% Options:
%%   - node_id: Override auto-generated node ID
%%   - refresh_interval: How often to refresh peers (default: 5 min)
%%   - seed_peers: Initial peers to discover from
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

%% @doc Stop the discovery service
-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%% @doc Get this node's info (for /.well-known/barrel)
-spec node_info() -> {ok, map()}.
node_info() ->
    gen_server:call(?SERVER, node_info).

%% @doc Get this node's unique ID
-spec node_id() -> {ok, binary()}.
node_id() ->
    gen_server:call(?SERVER, node_id).

%% @doc Get this node's zone
%% Returns undefined if no zone is configured
-spec get_zone() -> {ok, binary() | undefined}.
get_zone() ->
    gen_server:call(?SERVER, get_zone).

%% @doc Get all peer URLs in a specific zone
%% Returns list of peer URLs that are in the given zone
-spec nodes_in_zone(binary()) -> {ok, [binary()]}.
nodes_in_zone(ZoneName) when is_binary(ZoneName) ->
    gen_server:call(?SERVER, {nodes_in_zone, ZoneName}).

%% @doc List all known zones from peers
-spec list_zones() -> {ok, [binary()]}.
list_zones() ->
    gen_server:call(?SERVER, list_zones).

%% @doc Add a peer to the registry (async by default)
%% Returns ok immediately; peer info is fetched in background.
%% Use get_peer/1 to check the final status.
-spec add_peer(binary()) -> ok | {error, term()}.
add_peer(Url) when is_binary(Url) ->
    gen_server:call(?SERVER, {add_peer, Url}).

%% @doc Remove a peer from the registry
-spec remove_peer(binary()) -> ok.
remove_peer(Url) when is_binary(Url) ->
    gen_server:call(?SERVER, {remove_peer, Url}).

%% @doc List all known peers
-spec list_peers() -> {ok, [peer_info()]}.
list_peers() ->
    gen_server:call(?SERVER, list_peers).

%% @doc Get info about a specific peer
-spec get_peer(binary()) -> {ok, peer_info()} | {error, not_found}.
get_peer(Url) when is_binary(Url) ->
    gen_server:call(?SERVER, {get_peer, Url}).

%% @doc Discover peers from a seed node
%% Fetches the seed's node info and adds its known peers
-spec discover_from(binary()) -> ok | {error, term()}.
discover_from(SeedUrl) when is_binary(SeedUrl) ->
    gen_server:call(?SERVER, {discover_from, SeedUrl}, 30000).

%% @doc Refresh info for all known peers
-spec refresh_peers() -> ok.
refresh_peers() ->
    gen_server:cast(?SERVER, refresh_peers).

%% @doc Add a peer with options
%% Options:
%%   - tags: List of tags to assign to this peer
%%   - sync: If true, wait for peer info fetch (default: false)
%%           Returns {ok, PeerInfo} or {ok, PeerInfo, {unreachable, Reason}}
-spec add_peer(binary(), map()) -> ok | {ok, map()} | {ok, map(), {unreachable, term()}} | {error, term()}.
add_peer(Url, Opts) when is_binary(Url), is_map(Opts) ->
    Timeout = case maps:get(sync, Opts, false) of
        true -> 30000;  %% 30s for sync mode (HTTP timeouts)
        false -> 5000   %% default
    end,
    gen_server:call(?SERVER, {add_peer, Url, Opts}, Timeout).

%% @doc List peers with optional filter
%% Filter options:
%%   - tag: Filter by tag
%%   - status: Filter by status (active | unreachable)
%%   - has_db: Filter peers that have a specific database
-spec list_peers(map()) -> {ok, [peer_info()]}.
list_peers(Filter) when is_map(Filter) ->
    gen_server:call(?SERVER, {list_peers, Filter}).

%% @doc List all known peers with JSON-safe encoding.
%% Binary public keys are encoded to base64 for JSON output.
-spec list_peers_json() -> {ok, [map()]}.
list_peers_json() ->
    {ok, Peers} = gen_server:call(?SERVER, list_peers),
    {ok, [encode_peer_for_json(P) || P <- Peers]}.

%% @doc Get peer info with JSON-safe encoding.
%% Binary public key is encoded to base64 for JSON output.
-spec get_peer_json(binary()) -> {ok, map()} | {error, not_found}.
get_peer_json(Url) when is_binary(Url) ->
    case gen_server:call(?SERVER, {get_peer, Url}) of
        {ok, Peer} -> {ok, encode_peer_for_json(Peer)};
        Error -> Error
    end.

%% @doc Get peer by node ID instead of URL
-spec get_peer_by_node_id(binary()) -> {ok, peer_info()} | {error, not_found}.
get_peer_by_node_id(NodeId) when is_binary(NodeId) ->
    gen_server:call(?SERVER, {get_peer_by_node_id, NodeId}).

%% @doc Discover peers from DNS SRV records
%% Looks up _barrel._tcp.Domain for SRV records
%% Example: `discover_from_dns(&lt;&lt;"example.com"&gt;&gt;)' looks up _barrel._tcp.example.com
-spec discover_from_dns(binary()) -> ok | {error, term()}.
discover_from_dns(Domain) when is_binary(Domain) ->
    gen_server:call(?SERVER, {discover_from_dns, Domain}, 30000).

%% @doc Add a tag to a peer
-spec tag_peer(binary(), binary()) -> ok | {error, not_found}.
tag_peer(Url, Tag) when is_binary(Url), is_binary(Tag) ->
    gen_server:call(?SERVER, {tag_peer, Url, Tag}).

%% @doc Remove a tag from a peer
-spec untag_peer(binary(), binary()) -> ok | {error, not_found}.
untag_peer(Url, Tag) when is_binary(Url), is_binary(Tag) ->
    gen_server:call(?SERVER, {untag_peer, Url, Tag}).

%% @doc List all tags in use
-spec list_tags() -> {ok, [binary()]}.
list_tags() ->
    gen_server:call(?SERVER, list_tags).

%% @doc Resolve a member reference to a list of URLs
%% Used by federation to expand references like {peer, NodeId} or {tag, Tag}
-spec resolve_member(member_ref()) -> {ok, [binary()]} | {error, term()}.
resolve_member(Ref) ->
    gen_server:call(?SERVER, {resolve_member, Ref}).

%% @doc Get all peer URLs that have a specific database
-spec resolve_peers_with_db(binary()) -> {ok, [binary()]}.
resolve_peers_with_db(DbName) when is_binary(DbName) ->
    gen_server:call(?SERVER, {resolve_peers_with_db, DbName}).

%% @doc Get a peer's public key by node ID.
%% Used for verifying signed requests from peers.
-spec get_peer_public_key(binary()) -> {ok, binary()} | {error, not_found | no_public_key}.
get_peer_public_key(PeerId) when is_binary(PeerId) ->
    case do_get_peer_by_node_id(PeerId) of
        {ok, #{public_key := PubKey}} when is_binary(PubKey), byte_size(PubKey) =:= 32 ->
            {ok, PubKey};
        {ok, _} ->
            {error, no_public_key};
        {error, not_found} ->
            {error, not_found}
    end.

%% @doc Add a DNS domain for periodic discovery
%% The domain will be queried via SRV records periodically
-spec add_dns_domain(binary()) -> ok.
add_dns_domain(Domain) when is_binary(Domain) ->
    gen_server:call(?SERVER, {add_dns_domain, Domain}).

%% @doc Remove a DNS domain from periodic discovery
-spec remove_dns_domain(binary()) -> ok.
remove_dns_domain(Domain) when is_binary(Domain) ->
    gen_server:call(?SERVER, {remove_dns_domain, Domain}).

%% @doc List all configured DNS domains
-spec list_dns_domains() -> {ok, [binary()]}.
list_dns_domains() ->
    gen_server:call(?SERVER, list_dns_domains).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    %% Get or generate node ID
    NodeId = case maps:get(node_id, Opts, undefined) of
        undefined -> get_or_create_node_id();
        Id -> Id
    end,

    %% Get zone from opts or application config
    %% Treat empty strings as undefined
    Zone = case maps:get(zone, Opts, undefined) of
        undefined ->
            case application:get_env(barrel_docdb, zone) of
                {ok, Z} when is_binary(Z), Z =/= <<>> -> Z;
                {ok, Z} when is_list(Z), Z =/= "" -> list_to_binary(Z);
                _ -> undefined
            end;
        Z when is_binary(Z), Z =/= <<>> -> Z;
        Z when is_list(Z), Z =/= "" -> list_to_binary(Z);
        _ -> undefined
    end,

    RefreshInterval = maps:get(refresh_interval, Opts, ?DEFAULT_REFRESH_INTERVAL),

    %% Start refresh timer
    TimerRef = erlang:send_after(RefreshInterval, self(), refresh_peers),

    %% Discover from seed peers if provided
    case maps:get(seed_peers, Opts, []) of
        [] -> ok;
        Seeds ->
            lists:foreach(fun(Seed) ->
                spawn(fun() -> discover_from(Seed) end)
            end, Seeds)
    end,

    {ok, #state{
        node_id = NodeId,
        zone = Zone,
        refresh_timer = TimerRef,
        refresh_interval = RefreshInterval
    }}.

handle_call(node_info, _From, #state{node_id = NodeId, zone = Zone} = State) ->
    Info = build_node_info(NodeId, Zone),
    {reply, {ok, Info}, State};

handle_call(node_id, _From, #state{node_id = NodeId} = State) ->
    {reply, {ok, NodeId}, State};

handle_call(get_zone, _From, #state{zone = Zone} = State) ->
    {reply, {ok, Zone}, State};

handle_call(nodes_in_zone, _From, State) ->
    Result = do_nodes_in_zone(all),
    {reply, Result, State};

handle_call({nodes_in_zone, ZoneName}, _From, State) ->
    Result = do_nodes_in_zone(ZoneName),
    {reply, Result, State};

handle_call(list_zones, _From, State) ->
    Result = do_list_zones(),
    {reply, Result, State};

handle_call({add_peer, Url}, _From, State) ->
    Result = do_add_peer(Url),
    {reply, Result, State};

handle_call({remove_peer, Url}, _From, State) ->
    _ = do_remove_peer(Url),
    {reply, ok, State};

handle_call(list_peers, _From, State) ->
    {ok, Peers} = do_list_peers(),
    {reply, {ok, Peers}, State};

handle_call({get_peer, Url}, _From, State) ->
    Result = do_get_peer(Url),
    {reply, Result, State};

handle_call({discover_from, SeedUrl}, _From, State) ->
    Result = do_discover_from(SeedUrl),
    {reply, Result, State};

handle_call({add_peer, Url, Opts}, _From, State) ->
    Result = do_add_peer(Url, Opts),
    {reply, Result, State};

handle_call({list_peers, Filter}, _From, State) ->
    {ok, AllPeers} = do_list_peers(),
    FilteredPeers = filter_peers(AllPeers, Filter),
    {reply, {ok, FilteredPeers}, State};

handle_call({get_peer_by_node_id, NodeId}, _From, State) ->
    Result = do_get_peer_by_node_id(NodeId),
    {reply, Result, State};

handle_call({discover_from_dns, Domain}, _From, State) ->
    Result = do_discover_from_dns(Domain),
    {reply, Result, State};

handle_call({tag_peer, Url, Tag}, _From, State) ->
    Result = do_tag_peer(Url, Tag),
    {reply, Result, State};

handle_call({untag_peer, Url, Tag}, _From, State) ->
    Result = do_untag_peer(Url, Tag),
    {reply, Result, State};

handle_call(list_tags, _From, State) ->
    Result = do_list_tags(),
    {reply, Result, State};

handle_call({resolve_member, Ref}, _From, State) ->
    Result = do_resolve_member(Ref),
    {reply, Result, State};

handle_call({resolve_peers_with_db, DbName}, _From, State) ->
    Result = do_resolve_peers_with_db(DbName),
    {reply, Result, State};

handle_call({add_dns_domain, Domain}, _From, State) ->
    Result = do_add_dns_domain(Domain),
    {reply, Result, State};

handle_call({remove_dns_domain, Domain}, _From, State) ->
    Result = do_remove_dns_domain(Domain),
    {reply, Result, State};

handle_call(list_dns_domains, _From, State) ->
    Result = do_list_dns_domains(),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(refresh_peers, State) ->
    do_refresh_peers(),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh_peers, #state{refresh_interval = Interval} = State) ->
    do_refresh_peers(),
    do_refresh_dns_domains(),
    TimerRef = erlang:send_after(Interval, self(), refresh_peers),
    {noreply, State#state{refresh_timer = TimerRef}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{refresh_timer = undefined}) ->
    ok;
terminate(_Reason, #state{refresh_timer = TimerRef}) ->
    _ = erlang:cancel_timer(TimerRef),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Get or create persistent node ID
get_or_create_node_id() ->
    DocId = <<"_node_id">>,
    case barrel_docdb:get_system_doc(DocId) of
        {ok, #{<<"node_id">> := NodeId}} ->
            NodeId;
        {error, not_found} ->
            %% Generate new ID
            NodeId = generate_node_id(),
            ok = barrel_docdb:put_system_doc(DocId, #{<<"node_id">> => NodeId}),
            NodeId
    end.

%% @private Generate a unique node ID
generate_node_id() ->
    %% Use hostname + random bytes
    {ok, Hostname} = inet:gethostname(),
    Random = base64:encode(crypto:strong_rand_bytes(8)),
    << (list_to_binary(Hostname))/binary, "-", Random/binary >>.

%% @private Build node info map
build_node_info(NodeId, Zone) ->
    %% Get list of databases (filter out internal system dbs)
    AllDbs = barrel_docdb:list_dbs(),
    Databases = [Db || Db <- AllDbs, not is_system_db(Db)],

    %% Get list of VDBs (virtual databases / sharded)
    {ok, Vdbs} = barrel_shard_map:list(),

    %% Get list of federations
    {ok, Federations} = barrel_federation:list(),
    FederationNames = [maps:get(name, F) || F <- Federations],

    %% Get known peers
    {ok, Peers} = do_list_peers(),
    PeerUrls = [maps:get(url, P) || P <- Peers, maps:get(status, P, active) =:= active],

    %% Get public key for P2P authentication
    PublicKey = case barrel_peer_auth:get_public_key_base64() of
        {ok, PubKeyB64} -> PubKeyB64;
        _ -> undefined
    end,

    BaseInfo = #{
        node_id => NodeId,
        public_key => PublicKey,
        version => barrel_version(),
        databases => Databases,
        vdbs => Vdbs,
        federations => FederationNames,
        known_peers => PeerUrls,
        timestamp => erlang:system_time(millisecond)
    },
    %% Add zone if configured
    case Zone of
        undefined -> BaseInfo;
        _ -> BaseInfo#{zone => Zone}
    end.

%% @private Get barrel version
barrel_version() ->
    case application:get_key(barrel_docdb, vsn) of
        {ok, Vsn} -> list_to_binary(Vsn);
        undefined -> <<"dev">>
    end.

%% @private Check if database is a system/internal database
is_system_db(<<"_", _/binary>>) -> true;
is_system_db(_) -> false.

%% @private Add a peer to registry
do_add_peer(Url) ->
    do_add_peer(Url, #{}).

%% @private Add peer with options
%% Options:
%%   - tags: list of tags to assign
%%   - sync: if true, wait for peer info fetch and return full result (default: false)
do_add_peer(Url, Opts) ->
    case barrel_federation:validate_remote_url(Url) of
        ok ->
            Tags = maps:get(tags, Opts, []),
            Sync = maps:get(sync, Opts, false),
            case Sync of
                true ->
                    %% Synchronous: fetch info and store with final status
                    do_add_peer_sync(Url, Tags);
                false ->
                    %% Async: store immediately, fetch in background
                    do_add_peer_async(Url, Tags)
            end;
        {error, _} = Err ->
            Err
    end.

%% @private Synchronous peer add - fetches info and returns result
do_add_peer_sync(Url, Tags) ->
    DocId = peer_doc_id(Url),
    case fetch_peer_info(Url) of
        {ok, Info} ->
            %% Decode public key from base64 if present
            PeerInfo0 = Info#{
                url => Url,
                tags => Tags,
                last_seen => erlang:system_time(millisecond),
                status => active
            },
            PeerInfo = decode_peer_public_key(PeerInfo0),
            ok = barrel_docdb:put_system_doc(DocId, PeerInfo),
            {ok, PeerInfo};
        {error, Reason} ->
            PeerInfo = #{
                url => Url,
                tags => Tags,
                last_seen => erlang:system_time(millisecond),
                status => unreachable
            },
            ok = barrel_docdb:put_system_doc(DocId, PeerInfo),
            {ok, PeerInfo, {unreachable, Reason}}
    end.

%% @private Async peer add - stores immediately, fetches in background
do_add_peer_async(Url, Tags) ->
    DocId = peer_doc_id(Url),
    InitialInfo = #{
        url => Url,
        tags => Tags,
        last_seen => erlang:system_time(millisecond),
        status => pending
    },
    ok = barrel_docdb:put_system_doc(DocId, InitialInfo),
    spawn(fun() -> async_fetch_peer_info(Url, Tags) end),
    ok.

%% @private Background fetch peer info and update stored record
async_fetch_peer_info(Url, Tags) ->
    DocId = peer_doc_id(Url),
    PeerInfo = case fetch_peer_info(Url) of
        {ok, Info} ->
            PeerInfo0 = Info#{
                url => Url,
                tags => Tags,
                last_seen => erlang:system_time(millisecond),
                status => active
            },
            decode_peer_public_key(PeerInfo0);
        {error, _} ->
            #{
                url => Url,
                tags => Tags,
                last_seen => erlang:system_time(millisecond),
                status => unreachable
            }
    end,
    barrel_docdb:put_system_doc(DocId, PeerInfo).

%% @private Remove a peer from registry
do_remove_peer(Url) ->
    DocId = peer_doc_id(Url),
    barrel_docdb:delete_system_doc(DocId).

%% @private List all peers
do_list_peers() ->
    barrel_docdb:fold_system_docs(
        <<"peer:">>,
        fun(_DocId, Peer, Acc) -> [Peer | Acc] end,
        []
    ).

%% @private Get a specific peer
do_get_peer(Url) ->
    DocId = peer_doc_id(Url),
    barrel_docdb:get_system_doc(DocId).

%% @private Discover peers from a seed node
do_discover_from(SeedUrl) ->
    case fetch_peer_info(SeedUrl) of
        {ok, #{<<"known_peers">> := KnownPeers} = Info} ->
            %% Add the seed itself
            SeedInfo = Info#{
                url => SeedUrl,
                last_seen => erlang:system_time(millisecond),
                status => active
            },
            DocId = peer_doc_id(SeedUrl),
            ok = barrel_docdb:put_system_doc(DocId, SeedInfo),

            %% Add known peers from seed (but don't recursively discover)
            lists:foreach(fun(PeerUrl) ->
                case do_get_peer(PeerUrl) of
                    {error, not_found} ->
                        %% New peer - add it
                        do_add_peer(PeerUrl);
                    {ok, _} ->
                        %% Already known
                        ok
                end
            end, KnownPeers),
            ok;
        {ok, Info} ->
            %% No known_peers in response, just add the seed
            SeedInfo = Info#{
                url => SeedUrl,
                last_seen => erlang:system_time(millisecond),
                status => active
            },
            DocId = peer_doc_id(SeedUrl),
            barrel_docdb:put_system_doc(DocId, SeedInfo);
        {error, _} = Err ->
            Err
    end.

%% @private Refresh all known peers
do_refresh_peers() ->
    {ok, Peers} = do_list_peers(),
    lists:foreach(fun(#{url := Url}) ->
        spawn(fun() ->
            case fetch_peer_info(Url) of
                {ok, Info} ->
                    UpdatedInfo = Info#{
                        url => Url,
                        last_seen => erlang:system_time(millisecond),
                        status => active
                    },
                    DocId = peer_doc_id(Url),
                    _ = barrel_docdb:put_system_doc(DocId, UpdatedInfo),
                    %% Learn about new peers
                    case maps:get(<<"known_peers">>, Info, []) of
                        KnownPeers when is_list(KnownPeers) ->
                            lists:foreach(fun(PeerUrl) ->
                                case do_get_peer(PeerUrl) of
                                    {error, not_found} -> do_add_peer(PeerUrl);
                                    _ -> ok
                                end
                            end, KnownPeers);
                        _ ->
                            ok
                    end;
                {error, _} ->
                    %% Mark as unreachable
                    DocId = peer_doc_id(Url),
                    case barrel_docdb:get_system_doc(DocId) of
                        {ok, PeerInfo} ->
                            barrel_docdb:put_system_doc(DocId, PeerInfo#{status => unreachable});
                        _ ->
                            ok
                    end
            end
        end)
    end, Peers).

%% @private Fetch peer info from remote node
fetch_peer_info(Url) ->
    InfoUrl = <<Url/binary, "/.well-known/barrel">>,
    Headers = [{<<"Accept">>, <<"application/json">>}],
    Options = [{recv_timeout, ?PEER_TIMEOUT}, {connect_timeout, 5000}],

    try
        case hackney:get(InfoUrl, Headers, <<>>, Options) of
            {ok, 200, RespHeaders, Body} ->
                _ = barrel_hlc:maybe_sync_from_header(
                    proplists:get_value(<<"x-barrel-hlc">>, RespHeaders)),
                try
                    {ok, json:decode(Body)}
                catch
                    _:_ -> {error, invalid_json}
                end;
            {ok, Status, RespHeaders, _Body} ->
                _ = barrel_hlc:maybe_sync_from_header(
                    proplists:get_value(<<"x-barrel-hlc">>, RespHeaders)),
                {error, {http_error, Status}};
            {error, Reason} ->
                {error, {connection_error, Reason}}
        end
    catch
        _:Error ->
            {error, {connection_error, Error}}
    end.

%% @private Generate doc ID for peer
peer_doc_id(Url) ->
    %% Hash the URL to get a consistent ID
    Hash = crypto:hash(sha256, Url),
    HashHex = binary:encode_hex(Hash),
    ShortHash = binary:part(HashHex, 0, 16),
    <<"peer:", ShortHash/binary>>.

%% @private Filter peers by criteria
filter_peers(Peers, Filter) ->
    lists:filter(fun(Peer) -> matches_filter(Peer, Filter) end, Peers).

matches_filter(Peer, Filter) ->
    TagMatch = case maps:get(tag, Filter, undefined) of
        undefined -> true;
        Tag -> lists:member(Tag, maps:get(tags, Peer, []))
    end,
    StatusMatch = case maps:get(status, Filter, undefined) of
        undefined -> true;
        Status -> maps:get(status, Peer, active) =:= Status
    end,
    DbMatch = case maps:get(has_db, Filter, undefined) of
        undefined -> true;
        DbName -> lists:member(DbName, maps:get(databases, Peer, []))
    end,
    ZoneMatch = case maps:get(zone, Filter, undefined) of
        undefined -> true;
        ZoneName -> maps:get(zone, Peer, undefined) =:= ZoneName
    end,
    TagMatch andalso StatusMatch andalso DbMatch andalso ZoneMatch.

%% @private Get peers in a specific zone
do_nodes_in_zone(all) ->
    %% Return all peers grouped by zone
    {ok, Peers} = do_list_peers(),
    ActivePeers = filter_peers(Peers, #{status => active}),
    Urls = [maps:get(url, P) || P <- ActivePeers],
    {ok, Urls};
do_nodes_in_zone(ZoneName) ->
    {ok, Peers} = do_list_peers(),
    ZonePeers = filter_peers(Peers, #{zone => ZoneName, status => active}),
    Urls = [maps:get(url, P) || P <- ZonePeers],
    {ok, Urls}.

%% @private List all known zones from peers
do_list_zones() ->
    {ok, Peers} = do_list_peers(),
    Zones = lists:foldl(
        fun(Peer, Acc) ->
            case maps:get(zone, Peer, undefined) of
                undefined -> Acc;
                Zone ->
                    case lists:member(Zone, Acc) of
                        true -> Acc;
                        false -> [Zone | Acc]
                    end
            end
        end,
        [],
        Peers
    ),
    {ok, lists:sort(Zones)}.

%% @private Get peer by node ID
do_get_peer_by_node_id(NodeId) ->
    {ok, Peers} = do_list_peers(),
    case lists:filter(
        fun(P) -> maps:get(node_id, P, undefined) =:= NodeId end,
        Peers
    ) of
        [Peer | _] -> {ok, Peer};
        [] -> {error, not_found}
    end.

%% @private Discover peers from DNS SRV records
do_discover_from_dns(Domain) ->
    SrvName = "_barrel._tcp." ++ binary_to_list(Domain),
    case inet_res:lookup(SrvName, in, srv) of
        [] ->
            {error, {no_srv_records, Domain}};
        Records ->
            %% SRV record format: {Priority, Weight, Port, Host}
            lists:foreach(fun({_Priority, _Weight, Port, Host}) ->
                Url = iolist_to_binary([
                    <<"http://">>,
                    list_to_binary(Host),
                    <<":">>,
                    integer_to_binary(Port)
                ]),
                do_add_peer(Url)
            end, Records),
            ok
    end.

%% @private Add tag to peer
do_tag_peer(Url, Tag) ->
    DocId = peer_doc_id(Url),
    case barrel_docdb:get_system_doc(DocId) of
        {ok, PeerInfo} ->
            ExistingTags = maps:get(tags, PeerInfo, []),
            NewTags = case lists:member(Tag, ExistingTags) of
                true -> ExistingTags;
                false -> [Tag | ExistingTags]
            end,
            barrel_docdb:put_system_doc(DocId, PeerInfo#{tags => NewTags});
        {error, not_found} ->
            {error, not_found}
    end.

%% @private Remove tag from peer
do_untag_peer(Url, Tag) ->
    DocId = peer_doc_id(Url),
    case barrel_docdb:get_system_doc(DocId) of
        {ok, PeerInfo} ->
            ExistingTags = maps:get(tags, PeerInfo, []),
            NewTags = lists:delete(Tag, ExistingTags),
            barrel_docdb:put_system_doc(DocId, PeerInfo#{tags => NewTags});
        {error, not_found} ->
            {error, not_found}
    end.

%% @private List all tags
do_list_tags() ->
    {ok, Peers} = do_list_peers(),
    AllTags = lists:foldl(
        fun(Peer, Acc) ->
            Tags = maps:get(tags, Peer, []),
            lists:umerge(lists:sort(Tags), Acc)
        end,
        [],
        Peers
    ),
    {ok, AllTags}.

%% @private Resolve member reference to list of URLs
%% Returns URLs for federation to query
do_resolve_member(Ref) when is_binary(Ref) ->
    %% Direct URL or local db name - return as-is
    {ok, [Ref]};

do_resolve_member({peer, NodeId}) ->
    %% Find peer by node ID, return its URL
    case do_get_peer_by_node_id(NodeId) of
        {ok, #{url := Url}} -> {ok, [Url]};
        {error, not_found} -> {error, {peer_not_found, NodeId}}
    end;

do_resolve_member({peer, NodeId, DbName}) ->
    %% Peer + specific database
    case do_get_peer_by_node_id(NodeId) of
        {ok, #{url := Url}} ->
            DbUrl = <<Url/binary, "/db/", DbName/binary>>,
            {ok, [DbUrl]};
        {error, not_found} ->
            {error, {peer_not_found, NodeId}}
    end;

do_resolve_member({tag, Tag}) ->
    %% All peers with this tag (regardless of status - federation handles failures)
    {ok, AllPeers} = do_list_peers(),
    Peers = filter_peers(AllPeers, #{tag => Tag}),
    Urls = [maps:get(url, P) || P <- Peers],
    {ok, Urls};

do_resolve_member({tag, Tag, DbName}) ->
    %% Peers with tag + specific database
    {ok, AllPeers} = do_list_peers(),
    Peers = filter_peers(AllPeers, #{tag => Tag, has_db => DbName}),
    Urls = [<<(maps:get(url, P))/binary, "/db/", DbName/binary>> || P <- Peers],
    {ok, Urls};

do_resolve_member({all_peers, DbName}) ->
    %% All peers that have this database
    {ok, AllPeers} = do_list_peers(),
    Peers = filter_peers(AllPeers, #{has_db => DbName}),
    Urls = [<<(maps:get(url, P))/binary, "/db/", DbName/binary>> || P <- Peers],
    {ok, Urls}.

%% @private Get all peer URLs that have a specific database
do_resolve_peers_with_db(DbName) ->
    {ok, AllPeers} = do_list_peers(),
    Peers = filter_peers(AllPeers, #{status => active, has_db => DbName}),
    Urls = [<<(maps:get(url, P))/binary, "/db/", DbName/binary>> || P <- Peers],
    {ok, Urls}.

%%====================================================================
%% DNS Domain Management
%%====================================================================

%% @private Add a DNS domain for periodic discovery
do_add_dns_domain(Domain) ->
    DocId = <<"_dns_domains">>,
    Domains = case barrel_docdb:get_system_doc(DocId) of
        {ok, #{<<"domains">> := Existing}} -> Existing;
        {error, not_found} -> []
    end,
    case lists:member(Domain, Domains) of
        true ->
            ok;  % Already registered
        false ->
            NewDomains = [Domain | Domains],
            _ = barrel_docdb:put_system_doc(DocId, #{<<"domains">> => NewDomains}),
            %% Trigger immediate discovery for new domain
            spawn(fun() -> _ = do_discover_from_dns(Domain) end),
            ok
    end.

%% @private Remove a DNS domain from periodic discovery
do_remove_dns_domain(Domain) ->
    DocId = <<"_dns_domains">>,
    case barrel_docdb:get_system_doc(DocId) of
        {ok, #{<<"domains">> := Domains}} ->
            NewDomains = lists:delete(Domain, Domains),
            barrel_docdb:put_system_doc(DocId, #{<<"domains">> => NewDomains});
        {error, not_found} ->
            ok
    end.

%% @private List all configured DNS domains
do_list_dns_domains() ->
    DocId = <<"_dns_domains">>,
    case barrel_docdb:get_system_doc(DocId) of
        {ok, #{<<"domains">> := Domains}} -> {ok, Domains};
        {error, not_found} -> {ok, []}
    end.

%% @private Refresh all configured DNS domains
do_refresh_dns_domains() ->
    case do_list_dns_domains() of
        {ok, []} ->
            ok;
        {ok, Domains} ->
            lists:foreach(fun(Domain) ->
                spawn(fun() -> do_discover_from_dns(Domain) end)
            end, Domains)
    end.

%% @private Decode public key from base64 in peer info.
%% The /.well-known/barrel endpoint returns public_key as base64.
%% We decode it to raw binary for storage and verification.
decode_peer_public_key(#{<<"public_key">> := PubKeyB64} = Info) when is_binary(PubKeyB64) ->
    case barrel_peer_auth:decode_public_key(PubKeyB64) of
        {ok, PubKey} ->
            Info#{public_key => PubKey};
        {error, _} ->
            maps:remove(<<"public_key">>, Info)
    end;
decode_peer_public_key(Info) ->
    Info.

%% @private Encode peer info for JSON output.
%% Raw 32-byte Ed25519 public keys cannot be JSON-encoded directly.
encode_peer_for_json(#{public_key := PubKey} = Info) when is_binary(PubKey), byte_size(PubKey) =:= 32 ->
    Info#{public_key => base64:encode(PubKey)};
encode_peer_for_json(Info) ->
    Info.
