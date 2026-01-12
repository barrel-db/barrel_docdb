%%%-------------------------------------------------------------------
%%% @doc VDB (Virtual Database / Sharded) workload for barrel_bench
%%%
%%% Benchmarks VDB operations vs non-sharded baseline:
%%% - CRUD operations on sharded vs non-sharded databases
%%% - Cross-shard query latency (scatter-gather overhead)
%%% - Impact of different shard counts
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench_vdb).

-export([run/3]).
-export([run_comparison/1]).
-export([run_shard_scaling/1]).

-define(BENCH_DB, <<"bench_db">>).
-define(BENCH_VDB, <<"bench_vdb">>).

%% @doc Run VDB benchmarks (called from barrel_bench)
-spec run(pid(), non_neg_integer(), non_neg_integer()) -> map().
run(_Db, NumDocs, Iterations) ->
    %% We ignore the Db parameter since we create our own VDB
    io:format("Running VDB benchmarks...~n"),

    %% Setup
    cleanup(),

    %% Run comparison benchmark
    Results = run_comparison(#{num_docs => NumDocs, iterations => Iterations}),

    %% Cleanup
    cleanup(),

    Results.

%% @doc Compare VDB (sharded) vs regular DB performance
-spec run_comparison(map()) -> map().
run_comparison(Config) ->
    NumDocs = maps:get(num_docs, Config, 5000),
    Iterations = maps:get(iterations, Config, 5000),
    ShardCount = maps:get(shard_count, Config, 4),

    io:format("~n=== VDB vs Non-Sharded Comparison ===~n"),
    io:format("Documents: ~p, Iterations: ~p, Shards: ~p~n~n",
              [NumDocs, Iterations, ShardCount]),

    %% Ensure clean state
    cleanup(),

    %% Benchmark non-sharded database
    io:format("--- Non-Sharded Database ---~n"),
    {ok, _} = barrel_docdb:create_db(?BENCH_DB),
    NonShardedResults = run_crud_benchmark(?BENCH_DB, db, NumDocs, Iterations),
    _ = barrel_docdb:delete_db(?BENCH_DB),

    %% Benchmark sharded VDB
    io:format("~n--- Sharded VDB (~p shards) ---~n", [ShardCount]),
    ok = barrel_vdb:create(?BENCH_VDB, #{shard_count => ShardCount}),
    VdbResults = run_crud_benchmark(?BENCH_VDB, vdb, NumDocs, Iterations),

    %% Cross-shard query benchmark
    io:format("~n--- Cross-Shard Query ---~n"),
    QueryResults = run_query_benchmark(?BENCH_VDB, Iterations),

    _ = barrel_vdb:delete(?BENCH_VDB),

    %% Print comparison
    print_comparison(NonShardedResults, VdbResults, QueryResults),

    #{
        non_sharded => NonShardedResults,
        vdb => VdbResults,
        vdb_query => QueryResults,
        config => #{
            num_docs => NumDocs,
            iterations => Iterations,
            shard_count => ShardCount
        }
    }.

%% @doc Benchmark different shard counts to measure scaling overhead
-spec run_shard_scaling(map()) -> map().
run_shard_scaling(Config) ->
    NumDocs = maps:get(num_docs, Config, 5000),
    Iterations = maps:get(iterations, Config, 5000),
    ShardCounts = maps:get(shard_counts, Config, [1, 2, 4, 8]),

    io:format("~n=== Shard Scaling Benchmark ===~n"),
    io:format("Documents: ~p, Iterations: ~p~n", [NumDocs, Iterations]),
    io:format("Testing shard counts: ~p~n~n", [ShardCounts]),

    Results = lists:foldl(fun(ShardCount, Acc) ->
        io:format("~n--- ~p Shards ---~n", [ShardCount]),

        cleanup(),

        VdbName = list_to_binary(io_lib:format("bench_vdb_~p", [ShardCount])),
        ok = barrel_vdb:create(VdbName, #{shard_count => ShardCount}),

        CrudResults = run_crud_benchmark(VdbName, vdb, NumDocs, Iterations),
        QueryResults = run_query_benchmark(VdbName, Iterations),

        _ = barrel_vdb:delete(VdbName),

        Acc#{ShardCount => #{crud => CrudResults, query => QueryResults}}
    end, #{}, ShardCounts),

    %% Print scaling comparison
    print_scaling_comparison(Results, ShardCounts),

    Results.

%%====================================================================
%% Internal functions
%%====================================================================

cleanup() ->
    %% Ensure application is started
    _ = application:ensure_all_started(barrel_docdb),
    %% Clean up any existing databases
    _ = barrel_docdb:delete_db(?BENCH_DB),
    _ = barrel_vdb:delete(?BENCH_VDB),
    %% Also clean up shard scaling DBs
    lists:foreach(fun(N) ->
        VdbName = list_to_binary(io_lib:format("bench_vdb_~p", [N])),
        _ = barrel_vdb:delete(VdbName)
    end, [1, 2, 4, 8, 16]),
    ok.

%% Run CRUD benchmark for either DB or VDB
run_crud_benchmark(Name, Type, NumDocs, Iterations) ->
    %% Insert
    io:format("  Inserting ~p documents...~n", [NumDocs]),
    InsertMetrics = bench_inserts(Name, Type, NumDocs),

    %% Read
    io:format("  Reading ~p documents...~n", [Iterations]),
    ReadMetrics = bench_reads(Name, Type, NumDocs, Iterations),

    %% Update
    io:format("  Updating ~p documents...~n", [Iterations]),
    UpdateMetrics = bench_updates(Name, Type, NumDocs, Iterations),

    #{
        insert => barrel_bench_metrics:summarize(InsertMetrics),
        read => barrel_bench_metrics:summarize(ReadMetrics),
        update => barrel_bench_metrics:summarize(UpdateMetrics)
    }.

bench_inserts(Name, Type, NumDocs) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(I, Acc) ->
        Doc = generate_doc(I),
        {Time, _} = timer:tc(fun() ->
            case Type of
                db -> barrel_docdb:put_doc(Name, Doc);
                vdb -> barrel_vdb:put_doc(Name, Doc)
            end
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(0, NumDocs - 1)).

bench_reads(Name, Type, NumDocs, Iterations) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(I, Acc) ->
        DocId = doc_id(I rem NumDocs),
        {Time, _} = timer:tc(fun() ->
            case Type of
                db -> barrel_docdb:get_doc(Name, DocId);
                vdb -> barrel_vdb:get_doc(Name, DocId)
            end
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(0, Iterations - 1)).

bench_updates(Name, Type, NumDocs, Iterations) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(I, Acc) ->
        DocId = doc_id(I rem NumDocs),
        {Time, _} = timer:tc(fun() ->
            GetResult = case Type of
                db -> barrel_docdb:get_doc(Name, DocId);
                vdb -> barrel_vdb:get_doc(Name, DocId)
            end,
            case GetResult of
                {ok, Doc} ->
                    Updated = Doc#{<<"updated_at">> => I},
                    case Type of
                        db -> barrel_docdb:put_doc(Name, Updated);
                        vdb -> barrel_vdb:put_doc(Name, Updated)
                    end;
                Error ->
                    Error
            end
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(0, Iterations - 1)).

%% Run query benchmark for VDB (scatter-gather)
run_query_benchmark(VdbName, Iterations) ->
    %% Simple equality query
    io:format("  Running simple_eq query (~p iterations)...~n", [Iterations]),
    SimpleEqMetrics = bench_query(VdbName,
        #{where => [{path, [<<"type">>], <<"bench_doc">>}], limit => 100},
        Iterations),

    %% Multi-condition query
    io:format("  Running multi_cond query (~p iterations)...~n", [Iterations]),
    MultiCondMetrics = bench_query(VdbName,
        #{where => [
            {path, [<<"type">>], <<"bench_doc">>},
            {path, [<<"status">>], <<"active">>}
        ], limit => 100},
        Iterations),

    %% Full scan (no limit, measure scatter-gather overhead)
    io:format("  Running full_scan query (~p iterations)...~n", [min(100, Iterations)]),
    FullScanMetrics = bench_query(VdbName,
        #{where => [{path, [<<"type">>], <<"bench_doc">>}]},
        min(100, Iterations)),

    #{
        simple_eq => barrel_bench_metrics:summarize(SimpleEqMetrics),
        multi_cond => barrel_bench_metrics:summarize(MultiCondMetrics),
        full_scan => barrel_bench_metrics:summarize(FullScanMetrics)
    }.

bench_query(VdbName, Query, Iterations) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(_, Acc) ->
        {Time, _} = timer:tc(fun() ->
            barrel_vdb:find(VdbName, Query)
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(1, Iterations)).

%% Generate a benchmark document
generate_doc(I) ->
    #{
        <<"id">> => doc_id(I),
        <<"type">> => <<"bench_doc">>,
        <<"index">> => I,
        <<"status">> => case I rem 2 of 0 -> <<"active">>; 1 -> <<"inactive">> end,
        <<"category">> => case I rem 5 of
            0 -> <<"electronics">>;
            1 -> <<"clothing">>;
            2 -> <<"food">>;
            3 -> <<"books">>;
            4 -> <<"other">>
        end,
        <<"value">> => I * 10,
        <<"data">> => #{
            <<"name">> => <<"Item ", (integer_to_binary(I))/binary>>,
            <<"description">> => <<"Benchmark document number ", (integer_to_binary(I))/binary>>
        }
    }.

doc_id(I) ->
    <<"bench_doc_", (integer_to_binary(I))/binary>>.

%% Print comparison results
print_comparison(NonSharded, Vdb, VdbQuery) ->
    io:format("~n=== Comparison Results ===~n~n"),

    io:format("Operation          | Non-Sharded | VDB (Sharded) | Overhead~n"),
    io:format("-------------------|-------------|---------------|----------~n"),

    print_comparison_row("Insert", NonSharded, Vdb, insert),
    print_comparison_row("Read", NonSharded, Vdb, read),
    print_comparison_row("Update", NonSharded, Vdb, update),

    io:format("~n--- VDB Query Performance ---~n"),
    io:format("Query Type    | ops/sec  | p50 (us) | p99 (us)~n"),
    io:format("--------------|----------|----------|----------~n"),

    print_query_row("simple_eq", VdbQuery),
    print_query_row("multi_cond", VdbQuery),
    print_query_row("full_scan", VdbQuery).

print_comparison_row(Label, NonSharded, Vdb, Key) ->
    NsThroughput = get_throughput(NonSharded, Key),
    VdbThroughput = get_throughput(Vdb, Key),
    Overhead = case NsThroughput of
        N when N == 0.0 -> 0.0;
        _ -> ((NsThroughput - VdbThroughput) / NsThroughput) * 100
    end,
    io:format("~-18s | ~10.1f  | ~13.1f | ~6.1f%~n",
              [Label, NsThroughput, VdbThroughput, Overhead]).

print_query_row(Key, QueryResults) ->
    case maps:get(Key, QueryResults, undefined) of
        undefined -> ok;
        Summary ->
            Throughput = maps:get(throughput, Summary, 0.0),
            P50 = maps:get(latency_p50, Summary, 0),
            P99 = maps:get(latency_p99, Summary, 0),
            io:format("~-13s | ~8.1f | ~8w | ~8w~n",
                      [atom_to_list(Key), Throughput, P50, P99])
    end.

%% Print scaling comparison
print_scaling_comparison(Results, ShardCounts) ->
    io:format("~n=== Shard Scaling Results ===~n~n"),

    io:format("Shards | Insert ops/s | Read ops/s | Query ops/s | Query p99~n"),
    io:format("-------|--------------|------------|-------------|----------~n"),

    lists:foreach(fun(ShardCount) ->
        case maps:get(ShardCount, Results, undefined) of
            undefined -> ok;
            #{crud := Crud, query := Query} ->
                InsertTp = get_throughput(Crud, insert),
                ReadTp = get_throughput(Crud, read),
                QueryTp = get_throughput(Query, simple_eq),
                QueryP99 = case maps:get(simple_eq, Query, undefined) of
                    undefined -> 0;
                    S -> maps:get(latency_p99, S, 0)
                end,
                io:format("~6w | ~12.1f | ~10.1f | ~11.1f | ~8w us~n",
                          [ShardCount, InsertTp, ReadTp, QueryTp, QueryP99])
        end
    end, ShardCounts).

get_throughput(Results, Key) ->
    case maps:get(Key, Results, undefined) of
        undefined -> 0.0;
        Summary -> maps:get(throughput, Summary, 0.0)
    end.
