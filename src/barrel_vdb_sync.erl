%%%-------------------------------------------------------------------
%%% @doc VDB Configuration Synchronization
%%%
%%% Handles VDB metadata synchronization across nodes using three strategies:
%%%
%%% 1. **Replicated Metadata Database**: VDB configs stored in `_barrel_vdb_meta`
%%%    which is automatically replicated to all peers.
%%%
%%% 2. **Gossip via Discovery**: VDB names included in node_info, allowing
%%%    peers to discover which VDBs exist on each node.
%%%
%%% 3. **On-Demand Pull**: When a node receives a request for an unknown VDB,
%%%    it can fetch the configuration from peers.
%%%
%%% Example:
%%% ```
%%% %% Sync VDB config to all peers
%%% ok = barrel_vdb_sync:broadcast_config(<<"users">>).
%%%
%%% %% Pull VDB config from peers (if local doesn't exist)
%%% {ok, Config} = barrel_vdb_sync:ensure_config(<<"users">>).
%%%
%%% %% Get list of VDBs known across the cluster
%%% {ok, VDBs} = barrel_vdb_sync:list_cluster_vdbs().
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb_sync).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    %% Sync operations
    broadcast_config/1,
    ensure_config/1,
    pull_config/2,
    %% Cluster-wide queries
    list_cluster_vdbs/0,
    get_vdb_nodes/1,
    %% Manual sync
    sync_all/0,
    sync_from_peer/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(META_DB, <<"_barrel_vdb_meta">>).
-define(SYNC_INTERVAL, 60000).  %% 1 minute
-define(PULL_TIMEOUT, 10000).   %% 10 seconds

-record(state, {
    sync_timer :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the VDB sync service
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Broadcast VDB config to all peers
%% Stores config in replicated meta database
-spec broadcast_config(binary()) -> ok | {error, term()}.
broadcast_config(VdbName) when is_binary(VdbName) ->
    gen_server:call(?SERVER, {broadcast_config, VdbName}).

%% @doc Ensure VDB config exists locally, pulling from peers if needed
%% This is the main entry point for nodes that need VDB config
-spec ensure_config(binary()) -> {ok, map()} | {error, term()}.
ensure_config(VdbName) when is_binary(VdbName) ->
    gen_server:call(?SERVER, {ensure_config, VdbName}, ?PULL_TIMEOUT + 5000).

%% @doc Pull VDB config from a specific peer
-spec pull_config(binary(), binary()) -> {ok, map()} | {error, term()}.
pull_config(VdbName, PeerUrl) when is_binary(VdbName), is_binary(PeerUrl) ->
    gen_server:call(?SERVER, {pull_config, VdbName, PeerUrl}, ?PULL_TIMEOUT + 5000).

%% @doc List all VDBs known across the cluster
%% Combines local VDBs with those discovered from peers
-spec list_cluster_vdbs() -> {ok, [binary()]}.
list_cluster_vdbs() ->
    gen_server:call(?SERVER, list_cluster_vdbs).

%% @doc Get list of peer URLs that have a specific VDB
-spec get_vdb_nodes(binary()) -> {ok, [binary()]}.
get_vdb_nodes(VdbName) when is_binary(VdbName) ->
    gen_server:call(?SERVER, {get_vdb_nodes, VdbName}).

%% @doc Sync all VDB configs from peers
-spec sync_all() -> ok.
sync_all() ->
    gen_server:cast(?SERVER, sync_all).

%% @doc Sync VDB configs from a specific peer
-spec sync_from_peer(binary()) -> ok | {error, term()}.
sync_from_peer(PeerUrl) when is_binary(PeerUrl) ->
    gen_server:call(?SERVER, {sync_from_peer, PeerUrl}, 30000).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Ensure meta database exists and is replicated to peers
    ensure_meta_db(),
    setup_meta_replication(),
    %% Start periodic sync
    TimerRef = erlang:send_after(?SYNC_INTERVAL, self(), sync_tick),
    %% Initial sync in background
    spawn(fun() -> do_sync_all() end),
    {ok, #state{sync_timer = TimerRef}}.

handle_call({broadcast_config, VdbName}, _From, State) ->
    Result = do_broadcast_config(VdbName),
    {reply, Result, State};

handle_call({ensure_config, VdbName}, _From, State) ->
    Result = do_ensure_config(VdbName),
    {reply, Result, State};

handle_call({pull_config, VdbName, PeerUrl}, _From, State) ->
    Result = do_pull_config(VdbName, PeerUrl),
    {reply, Result, State};

handle_call(list_cluster_vdbs, _From, State) ->
    Result = do_list_cluster_vdbs(),
    {reply, Result, State};

handle_call({get_vdb_nodes, VdbName}, _From, State) ->
    Result = do_get_vdb_nodes(VdbName),
    {reply, Result, State};

handle_call({sync_from_peer, PeerUrl}, _From, State) ->
    Result = do_sync_from_peer(PeerUrl),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(sync_all, State) ->
    spawn(fun() -> do_sync_all() end),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sync_tick, State) ->
    %% Periodic sync and update meta replication for new peers
    spawn(fun() ->
        update_meta_replication(),
        do_sync_all()
    end),
    TimerRef = erlang:send_after(?SYNC_INTERVAL, self(), sync_tick),
    {noreply, State#state{sync_timer = TimerRef}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{sync_timer = TimerRef}) ->
    case TimerRef of
        undefined -> ok;
        _ -> erlang:cancel_timer(TimerRef)
    end,
    ok.

%%====================================================================
%% Internal Functions - Meta Database
%%====================================================================

%% @private Ensure the VDB meta database exists
ensure_meta_db() ->
    case barrel_docdb:db_info(?META_DB) of
        {ok, _} ->
            ok;
        {error, not_found} ->
            {ok, _} = barrel_docdb:create_db(?META_DB),
            ok
    end.

%% @private Setup bidirectional replication for meta database with all peers
%% This ensures VDB configuration is automatically synced across all nodes
setup_meta_replication() ->
    spawn(fun() ->
        timer:sleep(5000),  %% Wait for discovery to have peers
        update_meta_replication()
    end).

%% @private Update meta database replication to include all active peers
update_meta_replication() ->
    PolicyName = <<"_vdb_meta_sync">>,

    %% Get all active peers
    {ok, Peers} = barrel_discovery:list_peers(#{status => active}),
    PeerUrls = [maps:get(url, P) || P <- Peers],

    case PeerUrls of
        [] ->
            %% No peers yet - will be updated on next sync
            ok;
        _ ->
            %% Build member list: local + all peer URLs with db path
            Members = [?META_DB | [<<Url/binary, "/db/", ?META_DB/binary>> || Url <- PeerUrls]],

            %% Create or update group replication policy
            PolicyConfig = #{
                pattern => group,
                members => Members,
                mode => continuous,
                enabled => true
            },

            case barrel_rep_policy:get(PolicyName) of
                {ok, _} ->
                    %% Policy exists - update if members changed
                    barrel_rep_policy:disable(PolicyName),
                    barrel_rep_policy:delete(PolicyName),
                    create_meta_policy(PolicyName, PolicyConfig);
                {error, not_found} ->
                    create_meta_policy(PolicyName, PolicyConfig)
            end
    end.

create_meta_policy(PolicyName, PolicyConfig) ->
    case barrel_rep_policy:create(PolicyName, PolicyConfig) of
        ok ->
            barrel_rep_policy:enable(PolicyName),
            logger:info("VDB meta replication policy enabled with ~p members",
                       [length(maps:get(members, PolicyConfig))]);
        {error, Reason} ->
            logger:warning("Failed to create VDB meta replication policy: ~p", [Reason])
    end.

%% @private Store VDB config in meta database
store_config_in_meta(VdbName, Config) ->
    DocId = <<"vdb:", VdbName/binary>>,
    Doc = Config#{
        <<"id">> => DocId,
        <<"vdb_name">> => VdbName,
        <<"updated_at">> => erlang:system_time(millisecond)
    },
    case barrel_docdb:put_doc(?META_DB, Doc, #{}) of
        {ok, _} -> ok;
        {error, _} = Err -> Err
    end.

%% @private Get VDB config from meta database
get_config_from_meta(VdbName) ->
    DocId = <<"vdb:", VdbName/binary>>,
    case barrel_docdb:get_doc(?META_DB, DocId, #{}) of
        {ok, Doc} ->
            {ok, Doc};
        {error, not_found} ->
            {error, not_found}
    end.

%% @private List all VDBs in meta database
list_vdbs_from_meta() ->
    case barrel_docdb:find(?META_DB, #{where => []}, #{}) of
        {ok, Docs, _Meta} ->
            VdbNames = lists:filtermap(fun(Doc) ->
                case maps:get(<<"vdb_name">>, Doc, undefined) of
                    undefined -> false;
                    Name -> {true, Name}
                end
            end, Docs),
            {ok, VdbNames};
        {error, _} ->
            {ok, []}
    end.

%%====================================================================
%% Internal Functions - Broadcast
%%====================================================================

%% @private Broadcast VDB config to meta database
do_broadcast_config(VdbName) ->
    case barrel_shard_map:get_config(VdbName) of
        {ok, Config} ->
            %% Get additional info
            {ok, Ranges} = barrel_shard_map:get_ranges(VdbName),
            {ok, Assignments} = barrel_shard_map:get_all_assignments(VdbName),

            FullConfig = #{
                <<"config">> => encode_config(Config),
                <<"ranges">> => [encode_range(R) || R <- Ranges],
                <<"assignments">> => [encode_assignment(A) || A <- Assignments]
            },

            %% Store in meta database (will be replicated)
            store_config_in_meta(VdbName, FullConfig);
        {error, _} = Err ->
            Err
    end.

%% @private Encode config for storage
encode_config(Config) ->
    maps:fold(
        fun(K, V, Acc) when is_atom(K) ->
            Key = atom_to_binary(K, utf8),
            Acc#{Key => encode_value(V)};
           (K, V, Acc) ->
            Acc#{K => encode_value(V)}
        end,
        #{},
        Config
    ).

encode_value(V) when is_atom(V) -> atom_to_binary(V, utf8);
encode_value(V) when is_map(V) -> encode_config(V);
encode_value(V) when is_list(V) -> [encode_value(E) || E <- V];
encode_value(V) -> V.

encode_range(Range) ->
    maps:fold(
        fun(K, V, Acc) when is_atom(K) ->
            Acc#{atom_to_binary(K, utf8) => V};
           (K, V, Acc) ->
            Acc#{K => V}
        end,
        #{},
        Range
    ).

encode_assignment(Assignment) ->
    maps:fold(
        fun(K, V, Acc) when is_atom(K), is_atom(V) ->
            Acc#{atom_to_binary(K, utf8) => atom_to_binary(V, utf8)};
           (K, V, Acc) when is_atom(K) ->
            Acc#{atom_to_binary(K, utf8) => V};
           (K, V, Acc) when is_atom(V) ->
            Acc#{K => atom_to_binary(V, utf8)};
           (K, V, Acc) ->
            Acc#{K => V}
        end,
        #{},
        Assignment
    ).

%%====================================================================
%% Internal Functions - Ensure/Pull
%%====================================================================

%% @private Ensure config exists locally
do_ensure_config(VdbName) ->
    %% First check local shard map
    case barrel_shard_map:get_config(VdbName) of
        {ok, Config} ->
            {ok, Config};
        {error, not_found} ->
            %% Check meta database
            case get_config_from_meta(VdbName) of
                {ok, MetaConfig} ->
                    %% Import from meta to shard map
                    import_config_from_meta(VdbName, MetaConfig);
                {error, not_found} ->
                    %% Try to pull from peers
                    pull_from_any_peer(VdbName)
            end
    end.

%% @private Pull config from a specific peer
do_pull_config(VdbName, PeerUrl) ->
    Url = <<PeerUrl/binary, "/vdb/", VdbName/binary>>,
    Headers = [{<<"Accept">>, <<"application/json">>} | auth_headers()],
    Options = [{recv_timeout, ?PULL_TIMEOUT}, {connect_timeout, 5000}],

    try
        case hackney:get(Url, Headers, <<>>, Options) of
            {ok, 200, _RespHeaders, ClientRef} ->
                {ok, Body} = hackney:body(ClientRef),
                Config = json:decode(Body),
                %% Store locally
                import_config(VdbName, Config);
            {ok, 404, _RespHeaders, ClientRef} ->
                hackney:body(ClientRef),
                {error, not_found};
            {ok, Status, _RespHeaders, ClientRef} ->
                hackney:body(ClientRef),
                {error, {http_error, Status}};
            {error, Reason} ->
                {error, {connection_error, Reason}}
        end
    catch
        _:Error ->
            {error, {pull_failed, Error}}
    end.

%% @private Try to pull config from any available peer
pull_from_any_peer(VdbName) ->
    {ok, Peers} = barrel_discovery:list_peers(#{status => active}),
    pull_from_peers(VdbName, [maps:get(url, P) || P <- Peers]).

pull_from_peers(_VdbName, []) ->
    {error, not_found};
pull_from_peers(VdbName, [PeerUrl | Rest]) ->
    case do_pull_config(VdbName, PeerUrl) of
        {ok, Config} ->
            {ok, Config};
        {error, _} ->
            pull_from_peers(VdbName, Rest)
    end.

%% @private Import config from meta database format
import_config_from_meta(VdbName, MetaConfig) ->
    Config = maps:get(<<"config">>, MetaConfig, #{}),
    Ranges = maps:get(<<"ranges">>, MetaConfig, []),
    Assignments = maps:get(<<"assignments">>, MetaConfig, []),

    %% Decode and create local shard map
    DecodedConfig = decode_config(Config),
    ShardCount = maps:get(shard_count, DecodedConfig, 4),

    %% Create shard map entry
    case barrel_shard_map:create(VdbName, DecodedConfig) of
        ok ->
            %% Update ranges
            lists:foreach(fun(R) ->
                ShardId = maps:get(<<"shard_id">>, R),
                barrel_shard_map:set_range(VdbName, ShardId, decode_range(R))
            end, Ranges),

            %% Update assignments
            lists:foreach(fun(A) ->
                ShardId = maps:get(<<"shard_id">>, A),
                barrel_shard_map:set_assignment(VdbName, ShardId, decode_assignment(A))
            end, Assignments),

            %% Ensure physical shard databases exist
            lists:foreach(fun(ShardId) ->
                ShardDb = barrel_shard_map:physical_db_name(VdbName, ShardId),
                case barrel_docdb:db_info(ShardDb) of
                    {ok, _} -> ok;
                    {error, not_found} ->
                        barrel_docdb:create_db(ShardDb, #{})
                end
            end, lists:seq(0, ShardCount - 1)),

            %% Register with VDB registry
            barrel_vdb_registry:register_vdb(VdbName),

            barrel_shard_map:get_config(VdbName);
        {error, already_exists} ->
            barrel_shard_map:get_config(VdbName);
        {error, _} = Err ->
            Err
    end.

%% @private Import config from HTTP response format
import_config(VdbName, HttpConfig) ->
    %% HTTP response contains different format than meta
    Config = #{
        shard_count => maps:get(<<"shard_count">>, HttpConfig, 4),
        hash_function => binary_to_existing_atom(
            maps:get(<<"hash_function">>, HttpConfig, <<"phash2">>), utf8),
        placement => decode_placement(maps:get(<<"placement">>, HttpConfig, #{}))
    },

    case barrel_shard_map:exists(VdbName) of
        true ->
            barrel_shard_map:get_config(VdbName);
        false ->
            case barrel_shard_map:create(VdbName, Config) of
                ok ->
                    %% Create physical databases
                    ShardCount = maps:get(shard_count, Config),
                    lists:foreach(fun(ShardId) ->
                        ShardDb = barrel_shard_map:physical_db_name(VdbName, ShardId),
                        case barrel_docdb:db_info(ShardDb) of
                            {ok, _} -> ok;
                            {error, not_found} ->
                                barrel_docdb:create_db(ShardDb, #{})
                        end
                    end, lists:seq(0, ShardCount - 1)),

                    barrel_vdb_registry:register_vdb(VdbName),
                    barrel_shard_map:get_config(VdbName);
                {error, _} = Err ->
                    Err
            end
    end.

%% @private Decode config from storage format
decode_config(Config) ->
    maps:fold(
        fun(<<"shard_count">>, V, Acc) -> Acc#{shard_count => V};
           (<<"hash_function">>, V, Acc) when is_binary(V) ->
               Acc#{hash_function => binary_to_existing_atom(V, utf8)};
           (<<"placement">>, V, Acc) -> Acc#{placement => decode_placement(V)};
           (<<"created_at">>, V, Acc) -> Acc#{created_at => V};
           (<<"logical_db">>, V, Acc) -> Acc#{logical_db => V};
           (_, _, Acc) -> Acc
        end,
        #{},
        Config
    ).

decode_placement(Placement) ->
    maps:fold(
        fun(<<"replica_factor">>, V, Acc) -> Acc#{replica_factor => V};
           (<<"zones">>, V, Acc) -> Acc#{zones => V};
           (<<"constraints">>, V, Acc) -> Acc#{constraints => V};
           (_, _, Acc) -> Acc
        end,
        #{},
        Placement
    ).

decode_range(Range) ->
    #{
        shard_id => maps:get(<<"shard_id">>, Range),
        start_hash => maps:get(<<"start_hash">>, Range),
        end_hash => maps:get(<<"end_hash">>, Range)
    }.

decode_assignment(Assignment) ->
    Status = case maps:get(<<"status">>, Assignment, <<"active">>) of
        S when is_binary(S) -> binary_to_existing_atom(S, utf8);
        S when is_atom(S) -> S
    end,
    #{
        shard_id => maps:get(<<"shard_id">>, Assignment),
        primary => maps:get(<<"primary">>, Assignment, undefined),
        replicas => maps:get(<<"replicas">>, Assignment, []),
        status => Status
    }.

%%====================================================================
%% Internal Functions - Cluster Queries
%%====================================================================

%% @private List all VDBs across the cluster
do_list_cluster_vdbs() ->
    %% Get local VDBs
    {ok, LocalVdbs} = barrel_shard_map:list(),

    %% Get VDBs from meta database
    {ok, MetaVdbs} = list_vdbs_from_meta(),

    %% Get VDBs from peer node info
    {ok, Peers} = barrel_discovery:list_peers(#{status => active}),
    PeerVdbs = lists:flatmap(fun(Peer) ->
        maps:get(vdbs, Peer, maps:get(<<"vdbs">>, Peer, []))
    end, Peers),

    %% Combine and deduplicate
    AllVdbs = lists:usort(LocalVdbs ++ MetaVdbs ++ PeerVdbs),
    {ok, AllVdbs}.

%% @private Get nodes that have a specific VDB
do_get_vdb_nodes(VdbName) ->
    {ok, Peers} = barrel_discovery:list_peers(#{status => active}),
    NodesWithVdb = lists:filtermap(fun(Peer) ->
        Vdbs = maps:get(vdbs, Peer, maps:get(<<"vdbs">>, Peer, [])),
        case lists:member(VdbName, Vdbs) of
            true -> {true, maps:get(url, Peer)};
            false -> false
        end
    end, Peers),
    {ok, NodesWithVdb}.

%%====================================================================
%% Internal Functions - Sync
%%====================================================================

%% @private Sync all VDBs from peers
do_sync_all() ->
    %% Get all peers
    {ok, Peers} = barrel_discovery:list_peers(#{status => active}),

    %% Sync from each peer
    lists:foreach(fun(Peer) ->
        PeerUrl = maps:get(url, Peer),
        catch do_sync_from_peer(PeerUrl)
    end, Peers),

    %% Also broadcast our local VDBs to meta database
    {ok, LocalVdbs} = barrel_shard_map:list(),
    lists:foreach(fun(VdbName) ->
        catch do_broadcast_config(VdbName)
    end, LocalVdbs),

    ok.

%% @private Sync VDB configs from a specific peer
do_sync_from_peer(PeerUrl) ->
    %% Get peer's VDB list
    Url = <<PeerUrl/binary, "/vdb">>,
    Headers = [{<<"Accept">>, <<"application/json">>} | auth_headers()],
    Options = [{recv_timeout, ?PULL_TIMEOUT}, {connect_timeout, 5000}],

    try
        case hackney:get(Url, Headers, <<>>, Options) of
            {ok, 200, _RespHeaders, ClientRef} ->
                {ok, Body} = hackney:body(ClientRef),
                Response = json:decode(Body),
                VdbList = maps:get(<<"vdbs">>, Response, []),

                %% Pull config for each VDB we don't have locally
                lists:foreach(fun(VdbName) ->
                    case barrel_shard_map:exists(VdbName) of
                        true -> ok;
                        false ->
                            %% Pull and import config
                            catch do_pull_config(VdbName, PeerUrl)
                    end
                end, VdbList),
                ok;
            {ok, _Status, _RespHeaders, ClientRef} ->
                hackney:body(ClientRef),
                {error, peer_error};
            {error, Reason} ->
                {error, Reason}
        end
    catch
        _:Error ->
            {error, {sync_failed, Error}}
    end.

%%====================================================================
%% Internal Functions - Auth
%%====================================================================

%% @private Get auth headers for internal cluster communication
%% Uses the BARREL_DOCDB_ADMIN_KEY environment variable if set
auth_headers() ->
    case os:getenv("BARREL_DOCDB_ADMIN_KEY") of
        false -> [];
        "" -> [];
        Key when is_list(Key) ->
            Token = list_to_binary(Key),
            [{<<"Authorization">>, <<"Bearer ", Token/binary>>}]
    end.
