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
    add_peer/1,
    remove_peer/1,
    list_peers/0,
    get_peer/1,
    discover_from/1,
    refresh_peers/0
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_REFRESH_INTERVAL, 300000).  %% 5 minutes
-define(PEER_TIMEOUT, 10000).  %% 10 seconds for peer requests

-record(state, {
    node_id :: binary(),
    refresh_timer :: reference() | undefined,
    refresh_interval :: pos_integer()
}).

-type peer_info() :: #{
    url := binary(),
    node_id => binary(),
    version => binary(),
    databases => [binary()],
    federations => [binary()],
    last_seen => integer(),
    status => active | unreachable
}.

-export_type([peer_info/0]).

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

%% @doc Add a peer to the registry
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

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    %% Get or generate node ID
    NodeId = case maps:get(node_id, Opts, undefined) of
        undefined -> get_or_create_node_id();
        Id -> Id
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
        refresh_timer = TimerRef,
        refresh_interval = RefreshInterval
    }}.

handle_call(node_info, _From, #state{node_id = NodeId} = State) ->
    Info = build_node_info(NodeId),
    {reply, {ok, Info}, State};

handle_call(node_id, _From, #state{node_id = NodeId} = State) ->
    {reply, {ok, NodeId}, State};

handle_call({add_peer, Url}, _From, State) ->
    Result = do_add_peer(Url),
    {reply, Result, State};

handle_call({remove_peer, Url}, _From, State) ->
    do_remove_peer(Url),
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

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(refresh_peers, State) ->
    do_refresh_peers(),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh_peers, #state{refresh_interval = Interval} = State) ->
    do_refresh_peers(),
    TimerRef = erlang:send_after(Interval, self(), refresh_peers),
    {noreply, State#state{refresh_timer = TimerRef}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{refresh_timer = TimerRef}) ->
    case TimerRef of
        undefined -> ok;
        _ -> erlang:cancel_timer(TimerRef)
    end,
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
    Hostname = case inet:gethostname() of
        {ok, H} -> list_to_binary(H);
        _ -> <<"unknown">>
    end,
    Random = base64:encode(crypto:strong_rand_bytes(8)),
    <<Hostname/binary, "-", Random/binary>>.

%% @private Build node info map
build_node_info(NodeId) ->
    %% Get list of databases (filter out internal system dbs)
    AllDbs = barrel_docdb:list_dbs(),
    Databases = [Db || Db <- AllDbs, not is_system_db(Db)],

    %% Get list of federations
    {ok, Federations} = barrel_federation:list(),
    FederationNames = [maps:get(name, F) || F <- Federations],

    %% Get known peers
    {ok, Peers} = do_list_peers(),
    PeerUrls = [maps:get(url, P) || P <- Peers, maps:get(status, P, active) =:= active],

    #{
        node_id => NodeId,
        version => barrel_version(),
        databases => Databases,
        federations => FederationNames,
        known_peers => PeerUrls,
        timestamp => erlang:system_time(millisecond)
    }.

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
    %% Validate URL
    case barrel_federation:validate_remote_url(Url) of
        ok ->
            %% Try to fetch peer info
            PeerInfo = case fetch_peer_info(Url) of
                {ok, Info} ->
                    Info#{
                        url => Url,
                        last_seen => erlang:system_time(millisecond),
                        status => active
                    };
                {error, _} ->
                    #{
                        url => Url,
                        last_seen => erlang:system_time(millisecond),
                        status => unreachable
                    }
            end,
            %% Store in system doc
            DocId = peer_doc_id(Url),
            barrel_docdb:put_system_doc(DocId, PeerInfo);
        {error, _} = Err ->
            Err
    end.

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
                    barrel_docdb:put_system_doc(DocId, UpdatedInfo),

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
            {ok, 200, _RespHeaders, ClientRef} ->
                {ok, Body} = hackney:body(ClientRef),
                try
                    {ok, json:decode(Body)}
                catch
                    _:_ -> {error, invalid_json}
                end;
            {ok, Status, _RespHeaders, ClientRef} ->
                hackney:body(ClientRef),  %% Consume body
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
