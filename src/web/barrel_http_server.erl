%%%-------------------------------------------------------------------
%%% @doc HTTP server for barrel_docdb, backed by livery.
%%%
%%% Exposes the same on-wire API as the previous cowboy implementation
%%% (document CRUD, queries, changes feed, replication primitives,
%%% attachments, peer auth, admin endpoints). The framework swap is
%%% internal; tests and clients see no change.
%%%
%%% == HTTP/2 support ==
%%%
%%% livery serves HTTP/1.1 and HTTP/2 from the same TCP listener via
%%% h2c upgrade (or HTTP/2 prior knowledge). TLS adds HTTP/2 via ALPN
%%% negotiation. HTTP/3 (QUIC) is available out of the box and can be
%%% enabled by passing the `http3' option.
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
%%%     verify => verify_none | verify_peer
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

-record(state, {
    service_pid :: pid() | undefined,
    port :: non_neg_integer(),
    tls :: boolean()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

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
    TlsEnabled = maps:is_key(certfile, Opts) andalso maps:is_key(keyfile, Opts),
    Router = router(),
    ServiceOpts = service_opts(Port, TlsEnabled, Router, Opts),
    case livery:start_service(ServiceOpts) of
        {ok, ServicePid} ->
            Mode = case TlsEnabled of true -> "HTTPS"; false -> "HTTP" end,
            logger:info("barrel_http_server started on port ~p (~s, HTTP/1.1 + HTTP/2)",
                        [Port, Mode]),
            {ok, #state{service_pid = ServicePid, port = Port,
                        tls = TlsEnabled}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(get_info, _From, State) ->
    #state{port = Port, tls = Tls} = State,
    Info = #{
        port => Port,
        tls => Tls,
        protocols => [http2, http],
        http2 => true,
        http11 => true
    },
    {reply, {ok, Info}, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, #state{service_pid = ServicePid}) when is_pid(ServicePid) ->
    _ = try livery:stop_service(ServicePid)
        catch _:_ -> ok
        end,
    ok;
terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal: service opts
%%====================================================================

service_opts(Port, false, Router, _Opts) ->
    (base_service_opts())#{http => #{port => Port}, router => Router};
service_opts(Port, true, Router, Opts) ->
    HttpsListener0 = #{
        port => Port,
        cert => maps:get(certfile, Opts),
        key  => maps:get(keyfile, Opts)
    },
    HttpsListener1 = case maps:get(cacertfile, Opts, undefined) of
        undefined -> HttpsListener0;
        CaFile    -> HttpsListener0#{cacerts => [CaFile]}
    end,
    HttpsListener = case maps:get(verify, Opts, verify_none) of
        verify_peer ->
            HttpsListener1#{settings => #{verify => verify_peer}};
        _ ->
            HttpsListener1
    end,
    (base_service_opts())#{https => HttpsListener, router => Router}.

%% Service-level middleware shared by HTTP and HTTPS listeners.
%% Order follows livery's `log-requests' guide: request_id first so
%% every downstream layer can correlate, then the OpenTelemetry
%% middleware (trace + metrics) which wraps the handler, then access
%% log so it records the final status. instrument trace + metrics
%% feed the same `instrument' registry that barrel_metrics writes
%% domain metrics into; the `/metrics' route renders both surfaces.
base_service_opts() ->
    #{middleware => [
        {livery_request_id,        #{}},
        {livery_instrument_trace,  #{tracer => <<"barrel_docdb">>}},
        {livery_instrument_metrics, #{meter => <<"barrel_docdb">>}},
        {barrel_docdb_auth,        #{}},
        {livery_access_log,        #{}}
    ]}.

%%====================================================================
%% Internal: routes
%%====================================================================

%% Helper: build a closure that calls the central dispatch with an
%% action atom. Keeps the route table compact and avoids a per-action
%% wrapper function per HTTP verb.
-define(H(Action),
        (fun(__Req) -> barrel_http_handler:handle(Action, __Req) end)).

%% Compile the route table into a livery router. `/openapi.json' and
%% `/docs' are appended at the end so they're served alongside the
%% real routes (the OpenAPI doc is built from `routes/0' itself).
router() ->
    OpenApiDoc = openapi_doc(),
    Routes = routes() ++ [
        {<<"GET">>, <<"/openapi.json">>,
         livery_openapi:handler(OpenApiDoc),
         #{tags => [<<"Meta">>], operation_id => <<"openapi_spec">>,
           summary => <<"OpenAPI 3.1 document for this service">>}},
        {<<"GET">>, <<"/docs">>,
         livery_openapi:redoc_handler(),
         #{tags => [<<"Meta">>], operation_id => <<"docs">>,
           summary => <<"Redoc UI for the OpenAPI document">>}}
    ],
    livery_router:compile(Routes).

%% Build an OpenAPI 3.1 document from the route table. Called once
%% per service start; the result is captured in the `/openapi.json'
%% handler closure.
openapi_doc() ->
    Vsn = case application:get_key(barrel_docdb, vsn) of
        {ok, V} -> list_to_binary(V);
        undefined -> <<"0.0.0">>
    end,
    livery_openapi:build(#{
        info => #{
            title => <<"barrel_docdb HTTP API">>,
            version => Vsn,
            description => <<"Document database with MVCC, queries, ",
                             "changes feed, attachments, and replication. ",
                             "Authentication is via Bearer API keys (ak_*).">>
        },
        routes => routes()
    }).

%% The route table is a list of `{Method, Path, Handler, Meta}'.
%% `Meta' carries OpenAPI annotations (`operation_id', `summary',
%% `tags', `parameters', `request_body', `responses') that
%% `livery_openapi:build/1' consumes. The `livery_router:match/3'
%% path only reads `Meta.middleware', so adding documentation
%% fields is transparent to dispatch.
routes() ->
    [
        %% Meta / public
        {<<"GET">>, <<"/health">>, ?H(health),
         #{tags => [<<"Meta">>], operation_id => <<"health">>,
           summary => <<"Service health check">>}},
        {<<"GET">>, <<"/metrics">>, livery_metrics:handler(),
         #{tags => [<<"Meta">>], operation_id => <<"metrics">>,
           summary => <<"Prometheus metrics scrape endpoint">>}},
        {<<"GET">>, <<"/.well-known/barrel">>, ?H(node_info),
         #{tags => [<<"Meta">>], operation_id => <<"node_info">>,
           summary => <<"Node identity (id, version, peer public key)">>}},

        %% Admin: API keys
        {<<"GET">>, <<"/keys">>, ?H(keys),
         #{tags => [<<"Admin">>], operation_id => <<"list_keys">>,
           summary => <<"List API keys (admin only)">>}},
        {<<"POST">>, <<"/keys">>, ?H(keys),
         #{tags => [<<"Admin">>], operation_id => <<"create_key">>,
           summary => <<"Create an API key (admin only)">>}},
        {<<"GET">>, <<"/keys/:key_prefix">>, ?H(key),
         #{tags => [<<"Admin">>], operation_id => <<"get_key">>,
           summary => <<"Get API key by prefix (admin only)">>}},
        {<<"DELETE">>, <<"/keys/:key_prefix">>, ?H(key),
         #{tags => [<<"Admin">>], operation_id => <<"delete_key">>,
           summary => <<"Delete API key by prefix (admin only)">>}},

        %% Admin: usage stats
        {<<"GET">>, <<"/admin/usage">>, ?H(admin_usage),
         #{tags => [<<"Admin">>], operation_id => <<"admin_usage">>,
           summary => <<"Per-database usage stats (admin only)">>}},
        {<<"GET">>, <<"/admin/databases/:db/usage">>, ?H(admin_db_usage),
         #{tags => [<<"Admin">>], operation_id => <<"admin_db_usage">>,
           summary => <<"Usage stats for one database (admin only)">>}},

        %% Database lifecycle
        {<<"GET">>, <<"/db/:db">>, ?H(db_info),
         #{tags => [<<"Database">>], operation_id => <<"db_info">>,
           summary => <<"Get database information">>}},
        {<<"PUT">>, <<"/db/:db">>, ?H(db_info),
         #{tags => [<<"Database">>], operation_id => <<"create_db">>,
           summary => <<"Create a database">>}},
        {<<"DELETE">>, <<"/db/:db">>, ?H(db_info),
         #{tags => [<<"Database">>], operation_id => <<"delete_db">>,
           summary => <<"Delete a database and all its data">>}},
        {<<"POST">>, <<"/db/:db">>, ?H(db_info),
         #{tags => [<<"Documents">>], operation_id => <<"post_doc">>,
           summary => <<"Create a document with an auto-generated id">>}},

        %% Changes feed + SSE
        {<<"GET">>, <<"/db/:db/_changes">>, ?H(changes),
         #{tags => [<<"Changes">>], operation_id => <<"get_changes">>,
           summary => <<"Poll the changes feed">>}},
        {<<"POST">>, <<"/db/:db/_changes">>, ?H(changes),
         #{tags => [<<"Changes">>], operation_id => <<"post_changes">>,
           summary => <<"Poll the changes feed with a filter body">>}},
        {<<"GET">>, <<"/db/:db/_changes/stream">>,
         fun barrel_http_changes_stream:handle/1,
         #{tags => [<<"Changes">>], operation_id => <<"changes_stream">>,
           summary => <<"Stream the changes feed via Server-Sent Events">>}},
        {<<"POST">>, <<"/db/:db/_changes/stream">>,
         fun barrel_http_changes_stream:handle/1,
         #{tags => [<<"Changes">>], operation_id => <<"changes_stream_post">>,
           summary => <<"Stream changes via SSE with a filter body">>}},

        %% Bulk + query
        {<<"POST">>, <<"/db/:db/_bulk_docs">>, ?H(bulk_docs),
         #{tags => [<<"Documents">>], operation_id => <<"bulk_docs">>,
           summary => <<"Bulk create/update documents">>}},
        {<<"POST">>, <<"/db/:db/_find">>, ?H(find),
         #{tags => [<<"Queries">>], operation_id => <<"find">>,
           summary => <<"Query documents">>}},

        %% Replication primitives
        {<<"POST">>, <<"/db/:db/_replicate">>, ?H(replicate),
         #{tags => [<<"Replication">>], operation_id => <<"replicate">>,
           summary => <<"Trigger a one-shot replication">>}},
        {<<"POST">>, <<"/db/:db/_revsdiff">>, ?H(revsdiff),
         #{tags => [<<"Replication">>], operation_id => <<"revsdiff">>,
           summary => <<"Compute missing revisions (replication primitive)">>}},
        {<<"POST">>, <<"/db/:db/_put_rev">>, ?H(put_rev),
         #{tags => [<<"Replication">>], operation_id => <<"put_rev">>,
           summary => <<"Insert a document with explicit history (replication primitive)">>}},
        {<<"POST">>, <<"/db/:db/_sync_hlc">>, ?H(sync_hlc),
         #{tags => [<<"Replication">>], operation_id => <<"sync_hlc">>,
           summary => <<"Synchronise the hybrid logical clock with a peer">>}},

        %% Local documents (checkpoints)
        {<<"GET">>, <<"/db/:db/_local/:doc_id">>, ?H(local_doc),
         #{tags => [<<"Local">>], operation_id => <<"get_local_doc">>,
           summary => <<"Get a local (non-replicated) document">>}},
        {<<"PUT">>, <<"/db/:db/_local/:doc_id">>, ?H(local_doc),
         #{tags => [<<"Local">>], operation_id => <<"put_local_doc">>,
           summary => <<"Put a local (non-replicated) document">>}},
        {<<"DELETE">>, <<"/db/:db/_local/:doc_id">>, ?H(local_doc),
         #{tags => [<<"Local">>], operation_id => <<"delete_local_doc">>,
           summary => <<"Delete a local document">>}},

        %% Attachments (more specific paths first)
        {<<"GET">>, <<"/db/:db/:doc_id/_attachments">>, ?H(attachments),
         #{tags => [<<"Attachments">>], operation_id => <<"list_attachments">>,
           summary => <<"List attachments on a document">>}},
        {<<"GET">>, <<"/db/:db/:doc_id/_attachments/:att_name">>, ?H(attachment),
         #{tags => [<<"Attachments">>], operation_id => <<"get_attachment">>,
           summary => <<"Download an attachment">>}},
        {<<"PUT">>, <<"/db/:db/:doc_id/_attachments/:att_name">>, ?H(attachment),
         #{tags => [<<"Attachments">>], operation_id => <<"put_attachment">>,
           summary => <<"Upload an attachment">>}},
        {<<"DELETE">>, <<"/db/:db/:doc_id/_attachments/:att_name">>, ?H(attachment),
         #{tags => [<<"Attachments">>], operation_id => <<"delete_attachment">>,
           summary => <<"Delete an attachment">>}},

        %% Document operations (variable path - keep last)
        {<<"GET">>, <<"/db/:db/:doc_id">>, ?H(doc),
         #{tags => [<<"Documents">>], operation_id => <<"get_doc">>,
           summary => <<"Get a document">>}},
        {<<"PUT">>, <<"/db/:db/:doc_id">>, ?H(doc),
         #{tags => [<<"Documents">>], operation_id => <<"put_doc">>,
           summary => <<"Create or update a document">>}},
        {<<"DELETE">>, <<"/db/:db/:doc_id">>, ?H(doc),
         #{tags => [<<"Documents">>], operation_id => <<"delete_doc">>,
           summary => <<"Delete a document">>}}
    ].
