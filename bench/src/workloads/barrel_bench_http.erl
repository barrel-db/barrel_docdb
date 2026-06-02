%%%-------------------------------------------------------------------
%%% @doc HTTP API Benchmark workload
%%%
%%% Measures HTTP API overhead compared to direct Erlang API for
%%% document CRUD operations.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench_http).

-export([run/3]).
-export([run_comparison/1]).

-define(HTTP_PORT, 19080).
-define(BASE_URL, "http://localhost:" ++ integer_to_list(?HTTP_PORT)).
-define(BENCH_DB, <<"http_bench_db">>).
-define(POOL_NAME, barrel_bench_pool).
-define(POOL_SIZE, 100).

%% @doc Run HTTP benchmarks (called from barrel_bench)
-spec run(pid(), non_neg_integer(), non_neg_integer()) -> map().
run(_Db, NumDocs, Iterations) ->
    io:format("Running HTTP API benchmarks...~n"),
    run_comparison(#{num_docs => NumDocs, iterations => Iterations}).

%% @doc Compare HTTP API vs Direct API performance
-spec run_comparison(map()) -> map().
run_comparison(Config) ->
    NumDocs = maps:get(num_docs, Config, 1000),
    Iterations = maps:get(iterations, Config, 1000),

    io:format("~n=== HTTP API vs Direct API Benchmark ===~n"),
    io:format("Documents: ~p, Iterations: ~p~n~n", [NumDocs, Iterations]),

    %% Ensure applications are started
    application:ensure_all_started(barrel_docdb),
    application:ensure_all_started(hackney),

    %% Start HTTP server on dedicated bench port
    {ok, HttpPid} = barrel_http_server:start_link(#{port => ?HTTP_PORT}),
    unlink(HttpPid),

    %% Start HTTP connection pool and warm it up
    ok = start_pool(),

    %% Create API key for benchmarks
    {ok, ApiKey, _} = barrel_http_api_keys:create_key(#{
        name => <<"bench-http-key">>,
        permissions => [<<"read">>, <<"write">>, <<"admin">>],
        is_admin => true
    }),

    %% Cleanup any existing test databases
    cleanup(),

    try
        io:format("=== Database CRUD ===~n~n"),

        io:format("--- Direct Erlang API ---~n"),
        {ok, _} = barrel_docdb:create_db(?BENCH_DB),
        DirectDbResults = run_db_crud(direct, ?BENCH_DB, ApiKey, NumDocs, Iterations),
        _ = barrel_docdb:delete_db(?BENCH_DB),

        io:format("~n--- HTTP API ---~n"),
        {ok, _} = barrel_docdb:create_db(?BENCH_DB),
        HttpDbResults = run_db_crud(http, ?BENCH_DB, ApiKey, NumDocs, Iterations),
        _ = barrel_docdb:delete_db(?BENCH_DB),

        print_comparison(DirectDbResults, HttpDbResults, "Database CRUD"),

        #{
            db => #{
                direct => DirectDbResults,
                http => HttpDbResults
            },
            config => #{
                num_docs => NumDocs,
                iterations => Iterations
            }
        }
    after
        cleanup(),
        barrel_http_server:stop(),
        stop_pool()
    end.

%%====================================================================
%% Internal - Database CRUD
%%====================================================================

run_db_crud(Mode, DbName, ApiKey, NumDocs, Iterations) ->
    io:format("  Inserting ~p documents...~n", [NumDocs]),
    InsertMetrics = bench_db_inserts(Mode, DbName, ApiKey, NumDocs),

    io:format("  Reading ~p documents...~n", [Iterations]),
    ReadMetrics = bench_db_reads(Mode, DbName, ApiKey, NumDocs, Iterations),

    io:format("  Updating ~p documents...~n", [Iterations]),
    UpdateMetrics = bench_db_updates(Mode, DbName, ApiKey, NumDocs, Iterations),

    #{
        insert => barrel_bench_metrics:summarize(InsertMetrics),
        read => barrel_bench_metrics:summarize(ReadMetrics),
        update => barrel_bench_metrics:summarize(UpdateMetrics)
    }.

bench_db_inserts(Mode, DbName, ApiKey, NumDocs) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(I, Acc) ->
        Doc = generate_doc(I),
        {Time, _} = timer:tc(fun() ->
            case Mode of
                direct ->
                    barrel_docdb:put_doc(DbName, Doc);
                http ->
                    http_put_doc(DbName, ApiKey, Doc)
            end
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(0, NumDocs - 1)).

bench_db_reads(Mode, DbName, ApiKey, NumDocs, Iterations) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(I, Acc) ->
        DocId = doc_id(I rem NumDocs),
        {Time, _} = timer:tc(fun() ->
            case Mode of
                direct ->
                    barrel_docdb:get_doc(DbName, DocId);
                http ->
                    http_get_doc(DbName, ApiKey, DocId)
            end
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(0, Iterations - 1)).

bench_db_updates(Mode, DbName, ApiKey, NumDocs, Iterations) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(I, Acc) ->
        DocId = doc_id(I rem NumDocs),
        {Time, _} = timer:tc(fun() ->
            case Mode of
                direct ->
                    case barrel_docdb:get_doc(DbName, DocId) of
                        {ok, Doc, _} ->
                            Updated = Doc#{<<"updated_at">> => I},
                            barrel_docdb:put_doc(DbName, Updated);
                        Error -> Error
                    end;
                http ->
                    case http_get_doc(DbName, ApiKey, DocId) of
                        {ok, Doc} ->
                            Updated = Doc#{<<"updated_at">> => I},
                            http_put_doc(DbName, ApiKey, Updated);
                        Error -> Error
                    end
            end
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(0, Iterations - 1)).

%%====================================================================
%% HTTP Client Functions
%%====================================================================

http_put_doc(DbName, ApiKey, Doc) ->
    DocId = maps:get(<<"id">>, Doc, maps:get(<<"_id">>, Doc, undefined)),
    Url = ?BASE_URL ++ "/db/" ++ binary_to_list(DbName) ++ "/" ++ binary_to_list(DocId),
    Body = json:encode(Doc),
    case hackney:put(Url, http_headers(ApiKey), Body, [{pool, false}]) of
        {ok, Status, _, RespBody} when Status >= 200, Status < 300 ->
            {ok, json:decode(RespBody)};
        {ok, Status, _, _RespBody} ->
            {error, {http_status, Status}};
        {error, Reason} ->
            {error, Reason}
    end.

http_get_doc(DbName, ApiKey, DocId) ->
    Url = ?BASE_URL ++ "/db/" ++ binary_to_list(DbName) ++ "/" ++ binary_to_list(DocId),
    case hackney:get(Url, http_headers(ApiKey), <<>>, [{pool, false}]) of
        {ok, 200, _, RespBody} ->
            {ok, json:decode(RespBody)};
        {ok, Status, _, _RespBody} ->
            {error, {http_status, Status}};
        {error, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% Helpers
%%====================================================================

start_pool() ->
    application:set_env(hackney, max_connections, ?POOL_SIZE),
    ok.

stop_pool() ->
    ok.

http_headers(ApiKey) ->
    [{<<"Content-Type">>, <<"application/json">>},
     {<<"Accept">>, <<"application/json">>},
     {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}].

generate_doc(I) ->
    #{
        <<"id">> => doc_id(I),
        <<"type">> => <<"bench_doc">>,
        <<"index">> => I,
        <<"status">> => case I rem 2 of 0 -> <<"active">>; 1 -> <<"inactive">> end,
        <<"value">> => I * 10,
        <<"data">> => #{
            <<"name">> => <<"Item ", (integer_to_binary(I))/binary>>
        }
    }.

doc_id(I) ->
    <<"bench_doc_", (integer_to_binary(I))/binary>>.

cleanup() ->
    _ = barrel_docdb:delete_db(?BENCH_DB),
    ok.

print_comparison(Direct, Http, Label) ->
    io:format("~n=== ~s: Direct API vs HTTP API ===~n~n", [Label]),
    io:format("Operation | Direct (ops/s) | HTTP (ops/s) | Overhead~n"),
    io:format("----------|----------------|--------------|----------~n"),

    print_row("Insert", Direct, Http, insert),
    print_row("Read", Direct, Http, read),
    print_row("Update", Direct, Http, update).

print_row(Label, Direct, Http, Key) ->
    DirectTp = get_throughput(Direct, Key),
    HttpTp = get_throughput(Http, Key),
    Overhead = case DirectTp of
        N when N == 0.0 -> 0.0;
        _ -> ((DirectTp - HttpTp) / DirectTp) * 100
    end,
    io:format("~-9s | ~14.1f | ~12.1f | ~6.1f%~n",
              [Label, DirectTp, HttpTp, Overhead]).

get_throughput(Results, Key) ->
    case maps:get(Key, Results, undefined) of
        undefined -> 0.0;
        Summary -> maps:get(throughput, Summary, 0.0)
    end.
