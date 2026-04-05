%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_metrics module
%%%
%%% Tests OpenTelemetry metrics functionality using instrument_test
%%% for validation of counters, gauges, histograms, and Prometheus export.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_metrics_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases - setup
-export([
    setup_test/1,
    all_metrics_registered/1
]).

%% Test cases - counters
-export([
    counter_doc_ops/1,
    counter_doc_ops_increment/1,
    counter_query_ops/1,
    counter_http_requests/1,
    counter_replication/1,
    counter_federation/1
]).

%% Test cases - gauges
-export([
    gauge_db_stats/1,
    gauge_replication/1,
    gauge_peers/1
]).

%% Test cases - histograms
-export([
    histogram_doc_latency/1,
    histogram_query_latency/1,
    histogram_http_latency/1,
    histogram_federation_latency/1
]).

%% Test cases - export
-export([
    export_format/1,
    export_values/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, setup}, {group, counters}, {group, gauges},
     {group, histograms}, {group, export}].

groups() ->
    [
        {setup, [sequence], [
            setup_test,
            all_metrics_registered
        ]},
        {counters, [sequence], [
            counter_doc_ops,
            counter_doc_ops_increment,
            counter_query_ops,
            counter_http_requests,
            counter_replication,
            counter_federation
        ]},
        {gauges, [sequence], [
            gauge_db_stats,
            gauge_replication,
            gauge_peers
        ]},
        {histograms, [sequence], [
            histogram_doc_latency,
            histogram_query_latency,
            histogram_http_latency,
            histogram_federation_latency
        ]},
        {export, [sequence], [
            export_format,
            export_values
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    %% Use instrument_test for proper setup
    instrument_test:setup(),
    %% Setup barrel metrics
    barrel_metrics:setup(),
    Config.

end_per_group(_Group, _Config) ->
    instrument_test:cleanup(),
    ok.

init_per_testcase(TestCase, Config) ->
    %% Full cleanup and setup to reset NIF values between tests
    %% (counters/gauges/histograms accumulate in NIFs)
    case TestCase of
        setup_test ->
            %% First test, already setup in init_per_group
            ok;
        all_metrics_registered ->
            %% Just reset collectors
            instrument_test:reset();
        _ ->
            %% Full reset for metric value tests
            instrument_test:cleanup(),
            instrument_test:setup(),
            barrel_metrics:setup()
    end,
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Setup
%%====================================================================

setup_test(_Config) ->
    %% Setup should complete without errors
    ?assertEqual(ok, barrel_metrics:setup()),
    ok.

all_metrics_registered(_Config) ->
    %% Verify all expected instruments are registered
    ExpectedMetrics = [
        barrel_doc_operations,
        barrel_doc_operation_duration_seconds,
        barrel_query_operations,
        barrel_query_duration_seconds,
        barrel_query_results_count,
        barrel_replication_docs,
        barrel_replication_errors,
        barrel_replication_lag_seconds,
        barrel_replication_active,
        barrel_db_documents_total,
        barrel_db_size_bytes,
        barrel_db_attachments_total,
        barrel_http_requests,
        barrel_http_request_duration_seconds,
        barrel_peers_total,
        barrel_peers_active,
        barrel_federation_queries,
        barrel_federation_query_duration_seconds
    ],

    lists:foreach(fun(Name) ->
        Instrument = instrument_meter:get_instrument(Name),
        ?assertNotEqual(undefined, Instrument,
            io_lib:format("Instrument ~p should be registered", [Name]))
    end, ExpectedMetrics),
    ok.

%%====================================================================
%% Test Cases - Counters
%%====================================================================

counter_doc_ops(_Config) ->
    %% Test document operation counter
    ?assertEqual(ok, barrel_metrics:inc_doc_ops(<<"testdb">>, put)),
    ?assertEqual(ok, barrel_metrics:inc_doc_ops(<<"testdb">>, get)),
    ?assertEqual(ok, barrel_metrics:inc_doc_ops(<<"testdb">>, delete)),

    %% Verify with instrument_test
    instrument_test:assert_counter(barrel_doc_operations, 3.0),
    ok.

counter_doc_ops_increment(_Config) ->
    %% Test counter with explicit count
    barrel_metrics:inc_doc_ops(<<"db1">>, put, 5),
    barrel_metrics:inc_doc_ops(<<"db1">>, put, 3),

    %% Total should be 8
    instrument_test:assert_counter(barrel_doc_operations, 8.0),
    ok.

counter_query_ops(_Config) ->
    %% Test query operation counter
    barrel_metrics:inc_query_ops(<<"testdb">>),
    barrel_metrics:inc_query_ops(<<"testdb">>),

    instrument_test:assert_counter(barrel_query_operations, 2.0),
    ok.

counter_http_requests(_Config) ->
    %% Test HTTP request counter
    barrel_metrics:inc_http_requests(<<"GET">>, <<"/db">>, 200),
    barrel_metrics:inc_http_requests(<<"POST">>, <<"/db/doc">>, 201),
    barrel_metrics:inc_http_requests(<<"GET">>, <<"/db/doc">>, 404),

    instrument_test:assert_counter(barrel_http_requests, 3.0),
    ok.

counter_replication(_Config) ->
    %% Test replication counters
    barrel_metrics:inc_rep_docs(push, 10),
    barrel_metrics:inc_rep_docs(pull, 5),

    instrument_test:assert_counter(barrel_replication_docs, 15.0),

    barrel_metrics:inc_rep_errors(<<"task-1">>),
    instrument_test:assert_counter(barrel_replication_errors, 1.0),
    ok.

counter_federation(_Config) ->
    %% Test federation counter
    barrel_metrics:inc_federation_queries(<<"fed-1">>),
    barrel_metrics:inc_federation_queries(<<"fed-1">>),
    barrel_metrics:inc_federation_queries(<<"fed-2">>),

    instrument_test:assert_counter(barrel_federation_queries, 3.0),
    ok.

%%====================================================================
%% Test Cases - Gauges
%%====================================================================

gauge_db_stats(_Config) ->
    %% Test database stat gauges
    barrel_metrics:set_db_docs(<<"testdb">>, 1000),
    barrel_metrics:set_db_size(<<"testdb">>, 1048576),
    barrel_metrics:set_db_attachments(<<"testdb">>, 50),

    instrument_test:assert_gauge(barrel_db_documents_total, 1000.0),
    instrument_test:assert_gauge(barrel_db_size_bytes, 1048576.0),
    instrument_test:assert_gauge(barrel_db_attachments_total, 50.0),
    ok.

gauge_replication(_Config) ->
    %% Test replication gauges
    barrel_metrics:set_rep_lag(<<"task-1">>, 2.5),
    barrel_metrics:set_rep_active(<<"task-1">>, true),

    instrument_test:assert_gauge(barrel_replication_lag_seconds, 2.5),
    instrument_test:assert_gauge(barrel_replication_active, 1.0),

    %% Test setting to false
    barrel_metrics:set_rep_active(<<"task-1">>, false),
    instrument_test:assert_gauge(barrel_replication_active, 0.0),
    ok.

gauge_peers(_Config) ->
    %% Test peer gauges
    barrel_metrics:set_peers_total(10),
    barrel_metrics:set_peers_active(8),

    instrument_test:assert_gauge(barrel_peers_total, 10.0),
    instrument_test:assert_gauge(barrel_peers_active, 8.0),

    %% Test updating values
    barrel_metrics:set_peers_total(12),
    barrel_metrics:set_peers_active(10),

    instrument_test:assert_gauge(barrel_peers_total, 12.0),
    instrument_test:assert_gauge(barrel_peers_active, 10.0),
    ok.

%%====================================================================
%% Test Cases - Histograms
%%====================================================================

histogram_doc_latency(_Config) ->
    %% Test document operation latency histogram (values in ms)
    %% 5ms = 0.005s, 10ms = 0.01s, 25ms = 0.025s
    barrel_metrics:observe_doc_latency(<<"testdb">>, put, 5.0),
    barrel_metrics:observe_doc_latency(<<"testdb">>, get, 10.0),
    barrel_metrics:observe_doc_latency(<<"testdb">>, delete, 25.0),

    %% Verify count and sum
    instrument_test:assert_histogram_count(barrel_doc_operation_duration_seconds, 3),
    %% Sum is (5+10+25)/1000 = 0.040 seconds
    instrument_test:assert_histogram_sum(barrel_doc_operation_duration_seconds, 0.040),
    ok.

histogram_query_latency(_Config) ->
    %% Test query latency histogram
    barrel_metrics:observe_query_latency(<<"testdb">>, 50.0),
    barrel_metrics:observe_query_latency(<<"testdb">>, 100.0),

    instrument_test:assert_histogram_count(barrel_query_duration_seconds, 2),
    %% Sum is (50+100)/1000 = 0.150 seconds
    instrument_test:assert_histogram_sum(barrel_query_duration_seconds, 0.150),

    %% Test query results histogram
    barrel_metrics:observe_query_results(<<"testdb">>, 100),
    barrel_metrics:observe_query_results(<<"testdb">>, 50),

    instrument_test:assert_histogram_count(barrel_query_results_count, 2),
    instrument_test:assert_histogram_sum(barrel_query_results_count, 150.0),
    ok.

histogram_http_latency(_Config) ->
    %% Test HTTP latency histogram
    barrel_metrics:observe_http_latency(<<"GET">>, <<"/db">>, 25.0),
    barrel_metrics:observe_http_latency(<<"POST">>, <<"/db/doc">>, 100.0),
    barrel_metrics:observe_http_latency(<<"GET">>, <<"/db/doc">>, 50.0),

    instrument_test:assert_histogram_count(barrel_http_request_duration_seconds, 3),
    %% Sum is (25+100+50)/1000 = 0.175 seconds
    instrument_test:assert_histogram_sum(barrel_http_request_duration_seconds, 0.175),
    ok.

histogram_federation_latency(_Config) ->
    %% Test federation latency histogram
    barrel_metrics:observe_federation_latency(<<"fed-1">>, 200.0),
    barrel_metrics:observe_federation_latency(<<"fed-1">>, 300.0),

    instrument_test:assert_histogram_count(barrel_federation_query_duration_seconds, 2),
    %% Sum is (200+300)/1000 = 0.500 seconds
    instrument_test:assert_histogram_sum(barrel_federation_query_duration_seconds, 0.500),
    ok.

%%====================================================================
%% Test Cases - Export
%%====================================================================

export_format(_Config) ->
    %% Record some metrics
    barrel_metrics:inc_doc_ops(<<"exportdb">>, put),
    barrel_metrics:set_db_docs(<<"exportdb">>, 500),
    barrel_metrics:observe_doc_latency(<<"exportdb">>, put, 5.0),

    %% Export as binary
    Output = barrel_metrics:export_text(),
    ?assert(is_binary(Output)),

    %% Check for Prometheus format markers
    ?assertNotEqual(nomatch, binary:match(Output, <<"# HELP">>)),
    ?assertNotEqual(nomatch, binary:match(Output, <<"# TYPE">>)),

    %% Check metric types
    ?assertNotEqual(nomatch, binary:match(Output, <<"counter">>)),
    ?assertNotEqual(nomatch, binary:match(Output, <<"gauge">>)),
    ?assertNotEqual(nomatch, binary:match(Output, <<"histogram">>)),

    %% Check histogram has buckets
    ?assertNotEqual(nomatch, binary:match(Output, <<"_bucket">>)),
    ?assertNotEqual(nomatch, binary:match(Output, <<"_sum">>)),
    ?assertNotEqual(nomatch, binary:match(Output, <<"_count">>)),
    ?assertNotEqual(nomatch, binary:match(Output, <<"{le=">>)),

    ok.

export_values(_Config) ->
    %% Record metrics with known values
    barrel_metrics:inc_doc_ops(<<"db">>, put, 5),
    barrel_metrics:set_peers_total(42),

    %% Export
    Output = barrel_metrics:export_text(),

    %% Verify counter name and value in export
    ?assertNotEqual(nomatch, binary:match(Output, <<"barrel_doc_operations_total">>)),
    ?assertNotEqual(nomatch, binary:match(Output, <<"5">>)),

    %% Verify gauge name and value in export
    ?assertNotEqual(nomatch, binary:match(Output, <<"barrel_peers_total">>)),
    ?assertNotEqual(nomatch, binary:match(Output, <<"42">>)),

    ok.
