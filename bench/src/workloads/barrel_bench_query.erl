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

    %% Benchmark different query types - summarize immediately after each to get correct elapsed time
    io:format("  Running simple equality queries...~n"),
    SimpleEq = barrel_bench_metrics:summarize(bench_query(Db, simple_eq_query(), Iterations)),

    io:format("  Running simple equality with LIMIT 10...~n"),
    SimpleEqLimit = barrel_bench_metrics:summarize(bench_query(Db, simple_eq_limit_query(), Iterations)),

    io:format("  Running selective equality (status=active, ~~1/3 docs)...~n"),
    SelectiveEq = barrel_bench_metrics:summarize(bench_query(Db, selective_eq_query(), Iterations)),

    io:format("  Running very selective equality (city=Paris, ~~1/8 docs)...~n"),
    VerySelectiveEq = barrel_bench_metrics:summarize(bench_query(Db, very_selective_eq_query(), Iterations)),

    io:format("  Running range queries...~n"),
    Range = barrel_bench_metrics:summarize(bench_query(Db, range_query(), Iterations)),

    io:format("  Running pure compare queries (age > 50)...~n"),
    PureCompare = barrel_bench_metrics:summarize(bench_query(Db, pure_compare_query(), Iterations)),

    io:format("  Running pure compare with LIMIT 10...~n"),
    PureCompareLimit = barrel_bench_metrics:summarize(bench_query(Db, pure_compare_limit_query(), Iterations)),

    io:format("  Running multi-condition queries...~n"),
    MultiCond = barrel_bench_metrics:summarize(bench_query(Db, multi_condition_query(), Iterations)),

    io:format("  Running multi-index intersection (type=user AND status=active)...~n"),
    MultiIndex = barrel_bench_metrics:summarize(bench_query(Db, multi_index_query(), Iterations)),

    io:format("  Running multi-index range (type=user AND age>50)...~n"),
    MultiIndexRange = barrel_bench_metrics:summarize(bench_query(Db, multi_index_range_query(), Iterations)),

    io:format("  Running nested path queries...~n"),
    NestedPath = barrel_bench_metrics:summarize(bench_query(Db, nested_path_query(), Iterations)),

    io:format("  Running ORDER BY + LIMIT queries (Top-K)...~n"),
    TopK = barrel_bench_metrics:summarize(bench_query(Db, order_by_limit_query(), Iterations)),

    io:format("  Running pure ORDER BY + LIMIT (no filter)...~n"),
    PureTopK = barrel_bench_metrics:summarize(bench_query(Db, pure_order_limit_query(), Iterations)),

    io:format("  Running prefix queries...~n"),
    PrefixQ = barrel_bench_metrics:summarize(bench_query(Db, prefix_query(), Iterations)),

    io:format("  Running prefix with LIMIT 10...~n"),
    PrefixLimit = barrel_bench_metrics:summarize(bench_query(Db, prefix_limit_query(), Iterations)),

    io:format("  Running exists queries...~n"),
    ExistsQ = barrel_bench_metrics:summarize(bench_query(Db, exists_query(), Iterations)),

    io:format("  Running exists with LIMIT 10...~n"),
    ExistsLimit = barrel_bench_metrics:summarize(bench_query(Db, exists_limit_query(), Iterations)),

    #{
        simple_eq => SimpleEq,
        simple_eq_limit => SimpleEqLimit,
        selective_eq => SelectiveEq,
        very_selective_eq => VerySelectiveEq,
        range => Range,
        pure_compare => PureCompare,
        pure_compare_limit => PureCompareLimit,
        multi_condition => MultiCond,
        multi_index => MultiIndex,
        multi_index_range => MultiIndexRange,
        nested_path => NestedPath,
        order_by_limit => TopK,
        pure_topk => PureTopK,
        prefix => PrefixQ,
        prefix_limit => PrefixLimit,
        exists => ExistsQ,
        exists_limit => ExistsLimit
    }.

%%====================================================================
%% Query definitions
%%====================================================================

simple_eq_query() ->
    %% Simple equality: find all users with type=user
    %% include_docs => false enables pure index path (no doc body fetch)
    #{where => [{path, [<<"type">>], <<"user">>}], include_docs => false}.

simple_eq_limit_query() ->
    %% Simple equality with LIMIT - tests early termination
    #{where => [{path, [<<"type">>], <<"user">>}], limit => 10, include_docs => false}.

selective_eq_query() ->
    %% Selective equality: find docs with status=active (~1/3 of docs)
    %% Shows that we only scan matching keys, not all docs
    #{where => [{path, [<<"status">>], <<"active">>}], include_docs => false}.

very_selective_eq_query() ->
    %% Very selective: find docs with profile.city=Paris (~1/8 of docs)
    #{where => [{path, [<<"profile">>, <<"city">>], <<"Paris">>}], include_docs => false}.

range_query() ->
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"age">>], '>', 50}
    ]}.

pure_compare_query() ->
    %% Pure compare query: find all docs where age > 50
    %% Uses optimized range scan instead of full scan + filter
    %% include_docs => false enables pure index path (no doc body fetch)
    #{where => [{compare, [<<"age">>], '>', 50}], include_docs => false}.

pure_compare_limit_query() ->
    %% Pure compare with LIMIT - tests early termination on range scans
    #{where => [{compare, [<<"age">>], '>', 50}], limit => 10, include_docs => false}.

multi_condition_query() ->
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>}
    ]}.

multi_index_query() ->
    %% Multi-condition query using posting list intersection
    %% Uses optimized index intersection instead of full scan + filter
    %% include_docs => false enables pure index path (no doc body fetch)
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>}
    ], include_docs => false}.

multi_index_range_query() ->
    %% Multi-condition with equality + compare using index intersection
    %% Tests intersection of equality posting list with range scan
    #{where => [
        {path, [<<"type">>], <<"user">>},
        {compare, [<<"age">>], '>', 50}
    ], include_docs => false}.

nested_path_query() ->
    #{where => [{path, [<<"profile">>, <<"city">>], <<"Paris">>}]}.

order_by_limit_query() ->
    %% Get the 10 most recently created users (ORDER BY created_at DESC LIMIT 10)
    #{where => [{path, [<<"type">>], <<"user">>}],
      order_by => {[<<"created_at">>], desc},
      limit => 10}.

pure_order_limit_query() ->
    %% Pure ORDER BY + LIMIT with no filter conditions
    %% This is where Top-K optimization provides the most benefit
    #{where => [],
      order_by => {[<<"created_at">>], desc},
      limit => 10}.

prefix_query() ->
    %% Prefix query: find all users whose name starts with "User 1"
    %% Uses optimized interval scan instead of full scan + regex
    %% include_docs => false enables pure index path (no doc body fetch)
    #{where => [{prefix, [<<"name">>], <<"User 1">>}], include_docs => false}.

prefix_limit_query() ->
    %% Prefix query with LIMIT - tests early termination
    %% Should be very fast regardless of how many docs match
    #{where => [{prefix, [<<"name">>], <<"User 1">>}], limit => 10, include_docs => false}.

exists_query() ->
    %% Exists query: find all docs that have a "profile" field
    %% Uses path index scan without fetching full documents
    %% include_docs => false enables pure index path (no doc body fetch)
    #{where => [{exists, [<<"profile">>]}], include_docs => false}.

exists_limit_query() ->
    %% Exists query with LIMIT - tests early termination
    %% Should be very fast regardless of how many docs have the field
    #{where => [{exists, [<<"profile">>]}], limit => 10, include_docs => false}.

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
