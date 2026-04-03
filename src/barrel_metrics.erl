%%%-------------------------------------------------------------------
%%% @doc Prometheus metrics for barrel_docdb
%%%
%%% Provides comprehensive metrics for monitoring:
%%% - Document operations (put, get, delete)
%%% - Query performance
%%% - Replication status and throughput
%%% - Storage utilization
%%% - HTTP request latencies
%%% - Peer connectivity
%%%
%%% Metrics are exposed via the '/metrics' HTTP endpoint in Prometheus
%%% text format for scraping.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_metrics).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([setup/0]).

%% Metric recording functions
-export([
    %% Document operations
    inc_doc_ops/2,
    inc_doc_ops/3,
    observe_doc_latency/3,

    %% Query operations
    inc_query_ops/1,
    observe_query_latency/2,
    observe_query_results/2,

    %% Replication
    inc_rep_docs/2,
    inc_rep_errors/1,
    set_rep_lag/2,
    set_rep_active/2,

    %% Storage
    set_db_docs/2,
    set_db_size/2,
    set_db_attachments/2,

    %% HTTP
    inc_http_requests/3,
    observe_http_latency/3,

    %% Peers
    set_peers_total/1,
    set_peers_active/1,

    %% Federation
    inc_federation_queries/1,
    observe_federation_latency/2
]).

%% Export function
-export([export/0, export_text/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

%%====================================================================
%% Metric definitions
%%====================================================================

-define(METRICS, [
    %% Document operation counters
    {counter, barrel_doc_operations_total,
     "Total number of document operations",
     [db, operation]},

    %% Document operation latency histogram
    {histogram, barrel_doc_operation_duration_seconds,
     "Document operation duration in seconds",
     [db, operation],
     [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]},

    %% Query counters
    {counter, barrel_query_operations_total,
     "Total number of query operations",
     [db]},

    %% Query latency histogram
    {histogram, barrel_query_duration_seconds,
     "Query duration in seconds",
     [db],
     [0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]},

    %% Query result count histogram
    {histogram, barrel_query_results_count,
     "Number of results returned per query",
     [db],
     [1, 10, 50, 100, 500, 1000, 5000]},

    %% Replication document counter
    {counter, barrel_replication_docs_total,
     "Total documents replicated",
     [direction]},

    %% Replication error counter
    {counter, barrel_replication_errors_total,
     "Total replication errors",
     [task_id]},

    %% Replication lag gauge
    {gauge, barrel_replication_lag_seconds,
     "Replication lag in seconds",
     [task_id]},

    %% Active replications gauge
    {gauge, barrel_replication_active,
     "Whether replication is active (1) or not (0)",
     [task_id]},

    %% Database document count gauge
    {gauge, barrel_db_documents_total,
     "Total number of documents in database",
     [db]},

    %% Database size gauge
    {gauge, barrel_db_size_bytes,
     "Database size in bytes",
     [db]},

    %% Database attachment count gauge
    {gauge, barrel_db_attachments_total,
     "Total number of attachments in database",
     [db]},

    %% HTTP request counter
    {counter, barrel_http_requests_total,
     "Total HTTP requests",
     [method, path, status]},

    %% HTTP latency histogram
    {histogram, barrel_http_request_duration_seconds,
     "HTTP request duration in seconds",
     [method, path],
     [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]},

    %% Peer count gauges
    {gauge, barrel_peers_total,
     "Total number of known peers",
     []},

    {gauge, barrel_peers_active,
     "Number of active/reachable peers",
     []},

    %% Federation query counter
    {counter, barrel_federation_queries_total,
     "Total federation queries",
     [federation]},

    %% Federation latency histogram
    {histogram, barrel_federation_query_duration_seconds,
     "Federation query duration in seconds",
     [federation],
     [0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]}
]).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Setup all metrics - call this during application startup
setup() ->
    lists:foreach(fun declare_metric/1, ?METRICS).

%%====================================================================
%% Document Operations
%%====================================================================

%% @doc Increment document operation counter
-spec inc_doc_ops(binary(), atom()) -> ok.
inc_doc_ops(Db, Op) ->
    inc_doc_ops(Db, Op, 1).

-spec inc_doc_ops(binary(), atom(), pos_integer()) -> ok.
inc_doc_ops(Db, Op, Count) ->
    prometheus_counter:inc(barrel_doc_operations_total, [Db, Op], Count).

%% @doc Record document operation latency
-spec observe_doc_latency(binary(), atom(), number()) -> ok.
observe_doc_latency(Db, Op, DurationMs) ->
    prometheus_histogram:observe(barrel_doc_operation_duration_seconds,
                                  [Db, Op], DurationMs / 1000).

%%====================================================================
%% Query Operations
%%====================================================================

%% @doc Increment query operation counter
-spec inc_query_ops(binary()) -> ok.
inc_query_ops(Db) ->
    prometheus_counter:inc(barrel_query_operations_total, [Db]).

%% @doc Record query latency
-spec observe_query_latency(binary(), number()) -> ok.
observe_query_latency(Db, DurationMs) ->
    prometheus_histogram:observe(barrel_query_duration_seconds,
                                  [Db], DurationMs / 1000).

%% @doc Record query result count
-spec observe_query_results(binary(), non_neg_integer()) -> ok.
observe_query_results(Db, Count) ->
    prometheus_histogram:observe(barrel_query_results_count, [Db], Count).

%%====================================================================
%% Replication
%%====================================================================

%% @doc Increment replicated document counter
-spec inc_rep_docs(push | pull, pos_integer()) -> ok.
inc_rep_docs(Direction, Count) ->
    prometheus_counter:inc(barrel_replication_docs_total, [Direction], Count).

%% @doc Increment replication error counter
-spec inc_rep_errors(binary()) -> ok.
inc_rep_errors(TaskId) ->
    prometheus_counter:inc(barrel_replication_errors_total, [TaskId]).

%% @doc Set replication lag
-spec set_rep_lag(binary(), number()) -> ok.
set_rep_lag(TaskId, LagSeconds) ->
    prometheus_gauge:set(barrel_replication_lag_seconds, [TaskId], LagSeconds).

%% @doc Set replication active status
-spec set_rep_active(binary(), boolean()) -> ok.
set_rep_active(TaskId, Active) ->
    Value = case Active of true -> 1; false -> 0 end,
    prometheus_gauge:set(barrel_replication_active, [TaskId], Value).

%%====================================================================
%% Storage
%%====================================================================

%% @doc Set database document count
-spec set_db_docs(binary(), non_neg_integer()) -> ok.
set_db_docs(Db, Count) ->
    prometheus_gauge:set(barrel_db_documents_total, [Db], Count).

%% @doc Set database size in bytes
-spec set_db_size(binary(), non_neg_integer()) -> ok.
set_db_size(Db, SizeBytes) ->
    prometheus_gauge:set(barrel_db_size_bytes, [Db], SizeBytes).

%% @doc Set database attachment count
-spec set_db_attachments(binary(), non_neg_integer()) -> ok.
set_db_attachments(Db, Count) ->
    prometheus_gauge:set(barrel_db_attachments_total, [Db], Count).

%%====================================================================
%% HTTP
%%====================================================================

%% @doc Increment HTTP request counter
-spec inc_http_requests(binary(), binary(), integer()) -> ok.
inc_http_requests(Method, Path, Status) ->
    prometheus_counter:inc(barrel_http_requests_total, [Method, Path, Status]).

%% @doc Record HTTP request latency
-spec observe_http_latency(binary(), binary(), number()) -> ok.
observe_http_latency(Method, Path, DurationMs) ->
    prometheus_histogram:observe(barrel_http_request_duration_seconds,
                                  [Method, Path], DurationMs / 1000).

%%====================================================================
%% Peers
%%====================================================================

%% @doc Set total peer count
-spec set_peers_total(non_neg_integer()) -> ok.
set_peers_total(Count) ->
    prometheus_gauge:set(barrel_peers_total, [], Count).

%% @doc Set active peer count
-spec set_peers_active(non_neg_integer()) -> ok.
set_peers_active(Count) ->
    prometheus_gauge:set(barrel_peers_active, [], Count).

%%====================================================================
%% Federation
%%====================================================================

%% @doc Increment federation query counter
-spec inc_federation_queries(binary()) -> ok.
inc_federation_queries(Federation) ->
    prometheus_counter:inc(barrel_federation_queries_total, [Federation]).

%% @doc Record federation query latency
-spec observe_federation_latency(binary(), number()) -> ok.
observe_federation_latency(Federation, DurationMs) ->
    prometheus_histogram:observe(barrel_federation_query_duration_seconds,
                                  [Federation], DurationMs / 1000).

%%====================================================================
%% Export
%%====================================================================

%% @doc Export all metrics in Prometheus text format
-spec export() -> binary().
export() ->
    prometheus_text_format:format().

%% @doc Export all metrics as a binary string
-spec export_text() -> binary().
export_text() ->
    iolist_to_binary(export()).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Ensure prometheus application is started (creates ETS tables)
    _ = application:ensure_all_started(prometheus),
    setup(),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

declare_metric({counter, Name, Help, Labels}) ->
    prometheus_counter:declare([
        {name, Name},
        {help, Help},
        {labels, Labels}
    ]);
declare_metric({gauge, Name, Help, Labels}) ->
    prometheus_gauge:declare([
        {name, Name},
        {help, Help},
        {labels, Labels}
    ]);
declare_metric({histogram, Name, Help, Labels, Buckets}) ->
    prometheus_histogram:declare([
        {name, Name},
        {help, Help},
        {labels, Labels},
        {buckets, Buckets}
    ]).
