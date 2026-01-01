%%%-------------------------------------------------------------------
%%% @doc Barrel DocDB Performance Benchmark
%%%
%%% Simple benchmarking tool to measure barrel_docdb performance.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench).

-export([run/0, run/1]).
-export([run_crud/0, run_crud/1]).
-export([run_query/0, run_query/1]).
-export([run_changes/0, run_changes/1]).

-define(DEFAULT_NUM_DOCS, 10000).
-define(DEFAULT_ITERATIONS, 10000).
-define(BENCH_DB, <<"barrel_bench_db">>).

%% @doc Run all benchmarks with default configuration
-spec run() -> map().
run() ->
    run(#{}).

%% @doc Run all benchmarks with custom configuration
%%
%% Options:
%% - `num_docs': Number of documents to load (default: 10000)
%% - `iterations': Operations per test (default: 10000)
-spec run(map()) -> map().
run(Config) ->
    NumDocs = maps:get(num_docs, Config, ?DEFAULT_NUM_DOCS),
    Iterations = maps:get(iterations, Config, ?DEFAULT_ITERATIONS),

    io:format("~n=== barrel_docdb Benchmark ===~n"),
    io:format("Documents: ~p, Iterations: ~p~n~n", [NumDocs, Iterations]),

    {ok, Db} = setup_db(),

    Results = #{
        crud => run_workload(barrel_bench_crud, Db, NumDocs, Iterations),
        query => run_workload(barrel_bench_query, Db, NumDocs, Iterations),
        changes => run_workload(barrel_bench_changes, Db, NumDocs, Iterations)
    },

    cleanup_db(),

    print_results(Results),
    Results.

%% @doc Run only CRUD benchmarks
-spec run_crud() -> map().
run_crud() ->
    run_crud(#{}).

-spec run_crud(map()) -> map().
run_crud(Config) ->
    NumDocs = maps:get(num_docs, Config, ?DEFAULT_NUM_DOCS),
    Iterations = maps:get(iterations, Config, ?DEFAULT_ITERATIONS),

    io:format("~n=== CRUD Benchmark ===~n"),
    io:format("Documents: ~p, Iterations: ~p~n~n", [NumDocs, Iterations]),

    {ok, Db} = setup_db(),
    Result = run_workload(barrel_bench_crud, Db, NumDocs, Iterations),
    cleanup_db(),

    print_workload_result(crud, Result),
    Result.

%% @doc Run only query benchmarks
-spec run_query() -> map().
run_query() ->
    run_query(#{}).

-spec run_query(map()) -> map().
run_query(Config) ->
    NumDocs = maps:get(num_docs, Config, ?DEFAULT_NUM_DOCS),
    Iterations = maps:get(iterations, Config, ?DEFAULT_ITERATIONS),

    io:format("~n=== Query Benchmark ===~n"),
    io:format("Documents: ~p, Iterations: ~p~n~n", [NumDocs, Iterations]),

    {ok, Db} = setup_db(),
    Result = run_workload(barrel_bench_query, Db, NumDocs, Iterations),
    cleanup_db(),

    print_workload_result(query, Result),
    Result.

%% @doc Run only changes benchmarks
-spec run_changes() -> map().
run_changes() ->
    run_changes(#{}).

-spec run_changes(map()) -> map().
run_changes(Config) ->
    NumDocs = maps:get(num_docs, Config, ?DEFAULT_NUM_DOCS),
    Iterations = maps:get(iterations, Config, ?DEFAULT_ITERATIONS),

    io:format("~n=== Changes Benchmark ===~n"),
    io:format("Documents: ~p, Iterations: ~p~n~n", [NumDocs, Iterations]),

    {ok, Db} = setup_db(),
    Result = run_workload(barrel_bench_changes, Db, NumDocs, Iterations),
    cleanup_db(),

    print_workload_result(changes, Result),
    Result.

%%====================================================================
%% Internal functions
%%====================================================================

setup_db() ->
    %% Ensure application is started
    application:ensure_all_started(barrel_docdb),

    %% Clean up any existing bench db
    _ = barrel_docdb:delete_db(?BENCH_DB),

    %% Create fresh database
    barrel_docdb:create_db(?BENCH_DB).

cleanup_db() ->
    _ = barrel_docdb:delete_db(?BENCH_DB),
    ok.

run_workload(Module, Db, NumDocs, Iterations) ->
    case code:ensure_loaded(Module) of
        {module, Module} ->
            Module:run(Db, NumDocs, Iterations);
        {error, _} ->
            %% Workload not yet implemented
            #{}
    end.

print_results(Results) ->
    io:format("~n=== Results ===~n~n"),
    maps:foreach(fun(Name, Result) ->
        print_workload_result(Name, Result)
    end, Results).

print_workload_result(_Name, Result) when map_size(Result) =:= 0 ->
    ok;
print_workload_result(Name, Result) ->
    io:format("~s:~n", [string:uppercase(atom_to_list(Name))]),
    maps:foreach(fun(Op, Summary) ->
        print_summary(Op, Summary)
    end, Result),
    io:format("~n").

print_summary(Op, Summary) ->
    Count = maps:get(count, Summary, 0),
    Throughput = maps:get(throughput, Summary, 0.0),
    P50 = maps:get(latency_p50, Summary, 0),
    P99 = maps:get(latency_p99, Summary, 0),
    io:format("  ~-15s ~8.1f ops/sec, p50: ~6bus, p99: ~6bus (~p ops)~n",
              [atom_to_list(Op) ++ ":", Throughput, P50, P99, Count]).
