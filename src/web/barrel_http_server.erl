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

router() ->
    livery_router:compile([
        %% Public endpoints. `/metrics' is served by livery_metrics
        %% which renders the shared `instrument' registry as
        %% Prometheus text - same wire format the previous
        %% barrel_metrics:export_text/0 handler produced, just
        %% wired one layer up.
        {<<"GET">>, <<"/health">>,                ?H(health)},
        {<<"GET">>, <<"/metrics">>,               livery_metrics:handler()},
        {<<"GET">>, <<"/.well-known/barrel">>,    ?H(node_info)},

        %% API key management (admin only)
        {<<"GET">>,    <<"/keys">>,               ?H(keys)},
        {<<"POST">>,   <<"/keys">>,               ?H(keys)},
        {<<"GET">>,    <<"/keys/:key_prefix">>,   ?H(key)},
        {<<"DELETE">>, <<"/keys/:key_prefix">>,   ?H(key)},

        %% Admin usage
        {<<"GET">>, <<"/admin/usage">>,                  ?H(admin_usage)},
        {<<"GET">>, <<"/admin/databases/:db/usage">>,    ?H(admin_db_usage)},

        %% Database lifecycle
        {<<"GET">>,    <<"/db/:db">>, ?H(db_info)},
        {<<"PUT">>,    <<"/db/:db">>, ?H(db_info)},
        {<<"DELETE">>, <<"/db/:db">>, ?H(db_info)},
        {<<"POST">>,   <<"/db/:db">>, ?H(db_info)},

        %% Changes feed + SSE stream (separate handler module for SSE)
        {<<"GET">>,  <<"/db/:db/_changes">>,         ?H(changes)},
        {<<"POST">>, <<"/db/:db/_changes">>,         ?H(changes)},
        {<<"GET">>,  <<"/db/:db/_changes/stream">>,  fun barrel_http_changes_stream:handle/1},
        {<<"POST">>, <<"/db/:db/_changes/stream">>,  fun barrel_http_changes_stream:handle/1},

        %% Bulk + query
        {<<"POST">>, <<"/db/:db/_bulk_docs">>, ?H(bulk_docs)},
        {<<"POST">>, <<"/db/:db/_find">>,      ?H(find)},

        %% Replication primitives
        {<<"POST">>, <<"/db/:db/_replicate">>, ?H(replicate)},
        {<<"POST">>, <<"/db/:db/_revsdiff">>,  ?H(revsdiff)},
        {<<"POST">>, <<"/db/:db/_put_rev">>,   ?H(put_rev)},
        {<<"POST">>, <<"/db/:db/_sync_hlc">>,  ?H(sync_hlc)},

        %% Local documents (checkpoints)
        {<<"GET">>,    <<"/db/:db/_local/:doc_id">>, ?H(local_doc)},
        {<<"PUT">>,    <<"/db/:db/_local/:doc_id">>, ?H(local_doc)},
        {<<"DELETE">>, <<"/db/:db/_local/:doc_id">>, ?H(local_doc)},

        %% Attachments (more specific paths first)
        {<<"GET">>,    <<"/db/:db/:doc_id/_attachments">>,            ?H(attachments)},
        {<<"GET">>,    <<"/db/:db/:doc_id/_attachments/:att_name">>,  ?H(attachment)},
        {<<"PUT">>,    <<"/db/:db/:doc_id/_attachments/:att_name">>,  ?H(attachment)},
        {<<"DELETE">>, <<"/db/:db/:doc_id/_attachments/:att_name">>,  ?H(attachment)},

        %% Document operations (variable path - keep last)
        {<<"GET">>,    <<"/db/:db/:doc_id">>, ?H(doc)},
        {<<"PUT">>,    <<"/db/:db/:doc_id">>, ?H(doc)},
        {<<"DELETE">>, <<"/db/:db/:doc_id">>, ?H(doc)}
    ]).
