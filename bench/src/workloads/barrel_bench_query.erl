%%%-------------------------------------------------------------------
%%% @doc Query workload for barrel_bench
%%%
%%% Benchmarks different query patterns using barrel_docdb:find/2.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench_query).

-export([run/3]).

%% @doc Run query benchmarks
-spec run(pid(), non_neg_integer(), non_neg_integer()) -> map().
run(Db, NumDocs, Iterations) ->
    io:format("Running query benchmarks...~n"),

    %% First load documents if not already loaded
    case barrel_docdb:get_doc(Db, <<"user_0">>) of
        {error, not_found} ->
            io:format("  Loading ~p documents...~n", [NumDocs]),
            load_docs(Db, NumDocs);
        {ok, _} ->
            ok
    end,

    %% Benchmark different query types
    io:format("  Running simple equality queries...~n"),
    SimpleEq = bench_query(Db, simple_eq_query(), Iterations),

    io:format("  Running range queries...~n"),
    Range = bench_query(Db, range_query(), Iterations),

    io:format("  Running multi-condition queries...~n"),
    MultiCond = bench_query(Db, multi_condition_query(), Iterations),

    io:format("  Running nested path queries...~n"),
    NestedPath = bench_query(Db, nested_path_query(), Iterations),

    #{
        simple_eq => barrel_bench_metrics:summarize(SimpleEq),
        range => barrel_bench_metrics:summarize(Range),
        multi_condition => barrel_bench_metrics:summarize(MultiCond),
        nested_path => barrel_bench_metrics:summarize(NestedPath)
    }.

%%====================================================================
%% Query definitions
%%====================================================================

simple_eq_query() ->
    #{where => [{path, [<<"type">>], <<"user">>}]}.

range_query() ->
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"age">>], '>', 50}
    ]}.

multi_condition_query() ->
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>}
    ]}.

nested_path_query() ->
    #{where => [{path, [<<"profile">>, <<"city">>], <<"Paris">>}]}.

%%====================================================================
%% Internal functions
%%====================================================================

load_docs(Db, NumDocs) ->
    lists:foreach(fun(I) ->
        Doc = barrel_bench_generator:user_doc(I),
        barrel_docdb:put_doc(Db, Doc)
    end, lists:seq(0, NumDocs - 1)).

bench_query(Db, Query, Iterations) ->
    Metrics = barrel_bench_metrics:new(),
    lists:foldl(fun(_I, Acc) ->
        {Time, _Result} = timer:tc(fun() ->
            barrel_docdb:find(Db, Query)
        end),
        barrel_bench_metrics:record(Acc, Time)
    end, Metrics, lists:seq(1, Iterations)).
