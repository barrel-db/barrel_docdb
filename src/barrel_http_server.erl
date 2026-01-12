%%%-------------------------------------------------------------------
%%% @doc HTTP Server for barrel_docdb P2P replication
%%%
%%% Provides HTTP endpoints for:
%%% - Health checks
%%% - Document CRUD
%%% - Changes feed
%%% - Replication
%%%
%%% Supports both JSON and CBOR content types via Accept/Content-Type
%%% headers.
%%%
%%% == HTTP/2 Support ==
%%%
%%% The server supports HTTP/2 with automatic degradation to HTTP/1.1:
%%%
%%% - **HTTPS mode (recommended)**: Uses ALPN to negotiate HTTP/2 or HTTP/1.1.
%%%   Requires TLS certificates.
%%% - **HTTP mode**: Supports HTTP/2 cleartext (h2c) via Upgrade mechanism
%%%   or HTTP/2 prior knowledge. Falls back to HTTP/1.1 for clients that
%%%   don't support h2c.
%%%
%%% == Configuration ==
%%%
%%% ```
%%% barrel_http_server:start_link(#{
%%%     port => 8080,
%%%     %% TLS options (enables HTTPS with HTTP/2 ALPN)
%%%     certfile => "/path/to/cert.pem",
%%%     keyfile => "/path/to/key.pem",
%%%     cacertfile => "/path/to/ca.pem",  %% optional
%%%     %% Protocol options
%%%     protocols => [http2, http],  %% default: [http2, http]
%%%     %% Connection options
%%%     max_connections => infinity,
%%%     num_acceptors => 100
%%% }).
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_http_server).

-behaviour(gen_server).

%% API
-export([start_link/0, start_link/1]).
-export([stop/0]).
-export([get_info/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_PORT, 8080).
-define(DEFAULT_LISTENERS, 100).

-record(state, {
    listener_pid :: pid() | undefined,
    port :: non_neg_integer(),
    tls :: boolean(),
    protocols :: [http | http2]
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the HTTP server with default options.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start the HTTP server with options.
%%
%% Options:
%% <ul>
%%   <li>`port' - Listen port (default: 8080)</li>
%%   <li>`num_acceptors' - Number of acceptor processes (default: 100)</li>
%%   <li>`max_connections' - Max concurrent connections (default: infinity)</li>
%%   <li>`protocols' - List of protocols: `[http2, http]' (default: [http2, http])</li>
%%   <li>`certfile' - Path to TLS certificate (enables HTTPS)</li>
%%   <li>`keyfile' - Path to TLS private key</li>
%%   <li>`cacertfile' - Path to CA certificate (optional)</li>
%%   <li>`verify' - TLS verification mode: `verify_none' | `verify_peer'</li>
%% </ul>
%%
%% When `certfile' and `keyfile' are provided, the server starts in HTTPS mode
%% with HTTP/2 ALPN negotiation. Otherwise, it starts in HTTP mode with
%% HTTP/2 cleartext (h2c) support.
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

%% @doc Stop the HTTP server.
-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%% @doc Get server information.
%% Returns port, TLS status, and supported protocols.
-spec get_info() -> {ok, map()} | {error, not_running}.
get_info() ->
    try
        gen_server:call(?SERVER, get_info)
    catch
        exit:{noproc, _} -> {error, not_running}
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    Port = maps:get(port, Opts, ?DEFAULT_PORT),
    NumAcceptors = maps:get(num_acceptors, Opts, ?DEFAULT_LISTENERS),
    MaxConnections = maps:get(max_connections, Opts, infinity),
    Protocols = maps:get(protocols, Opts, [http2, http]),

    Dispatch = build_dispatch(),

    %% Check if TLS is configured
    TlsEnabled = maps:is_key(certfile, Opts) andalso maps:is_key(keyfile, Opts),

    Result = case TlsEnabled of
        true ->
            start_tls_listener(Port, NumAcceptors, MaxConnections, Protocols, Dispatch, Opts);
        false ->
            start_clear_listener(Port, NumAcceptors, MaxConnections, Protocols, Dispatch)
    end,

    case Result of
        {ok, ListenerPid} ->
            Mode = if TlsEnabled -> "HTTPS"; true -> "HTTP" end,
            ProtocolStr = format_protocols(Protocols),
            logger:info("barrel_http_server started on port ~p (~s, ~s)",
                       [Port, Mode, ProtocolStr]),
            {ok, #state{listener_pid = ListenerPid, port = Port,
                       tls = TlsEnabled, protocols = Protocols}};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private
%% Build the dispatch routes
build_dispatch() ->
    cowboy_router:compile([
        {'_', [
            %% Health endpoint
            {"/health", barrel_http_handler, #{action => health}},

            %% Prometheus metrics endpoint
            {"/metrics", barrel_http_handler, #{action => metrics}},

            %% Discovery endpoint (Mastodon-like node info)
            {"/.well-known/barrel", barrel_http_handler, #{action => node_info}},

            %% Peer management
            {"/_peers", barrel_http_handler, #{action => peers}},
            {"/_peers/:peer_url", barrel_http_handler, #{action => peer}},

            %% API key management (admin only)
            {"/keys", barrel_http_handler, #{action => keys}},
            {"/keys/:key_prefix", barrel_http_handler, #{action => key}},

            %% Federation endpoints
            {"/_federation", barrel_http_handler, #{action => federations}},
            {"/_federation/:name", barrel_http_handler, #{action => federation}},
            {"/_federation/:name/members/:member", barrel_http_handler, #{action => federation_member}},
            {"/_federation/:name/_find", barrel_http_handler, #{action => federation_find}},

            %% Replication policy endpoints
            {"/_policies", barrel_http_handler, #{action => policies}},
            {"/_policies/:name", barrel_http_handler, #{action => policy}},
            {"/_policies/:name/_enable", barrel_http_handler, #{action => policy_enable}},
            {"/_policies/:name/_disable", barrel_http_handler, #{action => policy_disable}},
            {"/_policies/:name/_status", barrel_http_handler, #{action => policy_status}},

            %% VDB (Virtual Database / Sharded Database) endpoints
            {"/vdb", barrel_http_handler, #{action => vdb_list}},
            {"/vdb/:vdb", barrel_http_handler, #{action => vdb_info}},
            {"/vdb/:vdb/_shards", barrel_http_handler, #{action => vdb_shards}},
            {"/vdb/:vdb/_replication", barrel_http_handler, #{action => vdb_replication}},
            {"/vdb/:vdb/_changes", barrel_http_handler, #{action => vdb_changes}},
            {"/vdb/:vdb/_bulk_docs", barrel_http_handler, #{action => vdb_bulk_docs}},
            {"/vdb/:vdb/_find", barrel_http_handler, #{action => vdb_find}},
            {"/vdb/:vdb/_import", barrel_http_handler, #{action => vdb_import}},
            {"/vdb/:vdb/_shards/:shard/_split", barrel_http_handler, #{action => vdb_shard_split}},
            {"/vdb/:vdb/_shards/:shard/_merge", barrel_http_handler, #{action => vdb_shard_merge}},
            {"/vdb/:vdb/:doc_id", barrel_http_handler, #{action => vdb_doc}},

            %% Database operations
            {"/db/:db", barrel_http_handler, #{action => db_info}},

            %% Static paths BEFORE variable paths
            %% Changes feed
            {"/db/:db/_changes", barrel_http_handler, #{action => changes}},
            %% Changes SSE stream (separate handler for loop handling)
            {"/db/:db/_changes/stream", barrel_http_changes_stream, #{}},

            %% Bulk operations
            {"/db/:db/_bulk_docs", barrel_http_handler, #{action => bulk_docs}},

            %% Query endpoint
            {"/db/:db/_find", barrel_http_handler, #{action => find}},

            %% Materialized views
            {"/db/:db/_views", barrel_http_handler, #{action => views}},
            {"/db/:db/_views/:view_id", barrel_http_handler, #{action => view}},
            {"/db/:db/_views/:view_id/_query", barrel_http_handler, #{action => view_query}},
            {"/db/:db/_views/:view_id/_refresh", barrel_http_handler, #{action => view_refresh}},

            %% Replication endpoints
            {"/db/:db/_replicate", barrel_http_handler, #{action => replicate}},
            {"/db/:db/_revsdiff", barrel_http_handler, #{action => revsdiff}},
            {"/db/:db/_put_rev", barrel_http_handler, #{action => put_rev}},
            {"/db/:db/_sync_hlc", barrel_http_handler, #{action => sync_hlc}},

            %% Local documents (checkpoints)
            {"/db/:db/_local/:doc_id", barrel_http_handler, #{action => local_doc}},

            %% Attachments (before doc catch-all)
            {"/db/:db/:doc_id/_attachments", barrel_http_handler, #{action => attachments}},
            {"/db/:db/:doc_id/_attachments/:att_name", barrel_http_handler, #{action => attachment}},

            %% Tier management endpoints
            {"/db/:db/_tier/config", barrel_http_handler, #{action => tier_config}},
            {"/db/:db/_tier/capacity", barrel_http_handler, #{action => tier_capacity}},
            {"/db/:db/_tier/migrate", barrel_http_handler, #{action => tier_migrate}},
            {"/db/:db/_tier/run_migration", barrel_http_handler, #{action => tier_run_migration}},
            {"/db/:db/:doc_id/_tier/ttl", barrel_http_handler, #{action => doc_tier_ttl}},
            {"/db/:db/:doc_id/_tier", barrel_http_handler, #{action => doc_tier}},

            %% Document operations (variable path - must be last)
            {"/db/:db/:doc_id", barrel_http_handler, #{action => doc}}
        ]}
    ]).

%% @private
%% Start HTTP listener (cleartext with h2c support)
start_clear_listener(Port, NumAcceptors, MaxConnections, Protocols, Dispatch) ->
    TransOpts = #{
        socket_opts => [{port, Port}],
        num_acceptors => NumAcceptors,
        max_connections => MaxConnections
    },

    %% Protocol options with HTTP/2 cleartext (h2c) support
    %% Cowboy will accept:
    %% - HTTP/2 prior knowledge (client sends HTTP/2 preface directly)
    %% - HTTP/1.1 Upgrade to h2c
    %% - Plain HTTP/1.1
    ProtoOpts = #{
        env => #{dispatch => Dispatch},
        stream_handlers => [cowboy_stream_h],
        %% Enable HTTP/2 cleartext (h2c) with HTTP/1.1 fallback
        protocols => Protocols,
        %% HTTP/2 settings
        max_concurrent_streams => 100,
        initial_connection_window_size => 65535 * 4,
        initial_stream_window_size => 65535 * 2
    },

    cowboy:start_clear(barrel_http_listener, TransOpts, ProtoOpts).

%% @private
%% Start HTTPS listener with HTTP/2 ALPN negotiation
start_tls_listener(Port, NumAcceptors, MaxConnections, Protocols, Dispatch, Opts) ->
    CertFile = maps:get(certfile, Opts),
    KeyFile = maps:get(keyfile, Opts),

    %% Base TLS options
    TlsOpts0 = [
        {certfile, CertFile},
        {keyfile, KeyFile}
    ],

    %% Add optional CA certificate
    TlsOpts1 = case maps:get(cacertfile, Opts, undefined) of
        undefined -> TlsOpts0;
        CaFile -> [{cacertfile, CaFile} | TlsOpts0]
    end,

    %% Add verification mode
    TlsOpts2 = case maps:get(verify, Opts, verify_none) of
        verify_peer ->
            [{verify, verify_peer}, {fail_if_no_peer_cert, true} | TlsOpts1];
        _ ->
            [{verify, verify_none} | TlsOpts1]
    end,

    %% ALPN protocols for HTTP/2 negotiation
    %% Order matters: prefer HTTP/2 (h2) over HTTP/1.1
    AlpnProtocols = lists:filtermap(
        fun(http2) -> {true, <<"h2">>};
           (http) -> {true, <<"http/1.1">>};
           (_) -> false
        end, Protocols),

    TlsOpts = [
        {alpn_preferred_protocols, AlpnProtocols},
        {next_protocols_advertised, AlpnProtocols}
        | TlsOpts2
    ],

    TransOpts = #{
        socket_opts => [{port, Port} | TlsOpts],
        num_acceptors => NumAcceptors,
        max_connections => MaxConnections
    },

    ProtoOpts = #{
        env => #{dispatch => Dispatch},
        stream_handlers => [cowboy_stream_h],
        protocols => Protocols,
        %% HTTP/2 settings
        max_concurrent_streams => 100,
        initial_connection_window_size => 65535 * 4,
        initial_stream_window_size => 65535 * 2
    },

    cowboy:start_tls(barrel_http_listener, TransOpts, ProtoOpts).

%% @private
format_protocols(Protocols) ->
    Strs = lists:map(
        fun(http2) -> "HTTP/2";
           (http) -> "HTTP/1.1"
        end, Protocols),
    string:join(Strs, ", ").

handle_call(get_info, _From, State) ->
    #state{port = Port, tls = Tls, protocols = Protocols} = State,
    Info = #{
        port => Port,
        tls => Tls,
        protocols => Protocols,
        http2 => lists:member(http2, Protocols),
        http11 => lists:member(http, Protocols)
    },
    {reply, {ok, Info}, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    cowboy:stop_listener(barrel_http_listener),
    ok.
