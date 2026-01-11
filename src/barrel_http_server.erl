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
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_http_server).

-behaviour(gen_server).

%% API
-export([start_link/0, start_link/1]).
-export([stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_PORT, 8080).
-define(DEFAULT_LISTENERS, 100).

-record(state, {
    listener_pid :: pid() | undefined,
    port :: non_neg_integer()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the HTTP server with default options.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start the HTTP server with options.
%% Options:
%%   - port: Listen port (default: 8080)
%%   - num_acceptors: Number of acceptor processes (default: 100)
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

%% @doc Stop the HTTP server.
-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    Port = maps:get(port, Opts, ?DEFAULT_PORT),
    NumAcceptors = maps:get(num_acceptors, Opts, ?DEFAULT_LISTENERS),

    Dispatch = cowboy_router:compile([
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
            {"/vdb/:vdb/_changes", barrel_http_handler, #{action => vdb_changes}},
            {"/vdb/:vdb/_bulk_docs", barrel_http_handler, #{action => vdb_bulk_docs}},
            {"/vdb/:vdb/_find", barrel_http_handler, #{action => vdb_find}},
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
    ]),

    TransOpts = #{
        socket_opts => [{port, Port}],
        num_acceptors => NumAcceptors
    },

    ProtoOpts = #{
        env => #{dispatch => Dispatch},
        stream_handlers => [cowboy_stream_h]
    },

    case cowboy:start_clear(barrel_http_listener, TransOpts, ProtoOpts) of
        {ok, ListenerPid} ->
            logger:info("barrel_http_server started on port ~p", [Port]),
            {ok, #state{listener_pid = ListenerPid, port = Port}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    cowboy:stop_listener(barrel_http_listener),
    ok.
