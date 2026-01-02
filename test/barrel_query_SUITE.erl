%%%-------------------------------------------------------------------
%%% @doc Common Test suite for barrel_query
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_query_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases - compilation
-export([
    compile_simple_equality/1,
    compile_multiple_conditions/1,
    compile_with_variables/1,
    compile_comparison/1,
    compile_and_or/1,
    compile_in_operator/1,
    compile_exists_missing/1,
    compile_regex_prefix/1,
    compile_invalid_spec/1,
    compile_invalid_condition/1,
    compile_invalid_operator/1
]).

%% Test cases - validation
-export([
    validate_valid_spec/1,
    validate_missing_where/1,
    validate_invalid_path/1,
    validate_invalid_regex/1
]).

%% Test cases - strategy
-export([
    strategy_index_seek/1,
    strategy_index_scan/1,
    strategy_multi_index/1,
    strategy_full_scan/1
]).

%% Test cases - execution
-export([
    execute_simple_equality/1,
    execute_multiple_conditions/1,
    execute_comparison_gt/1,
    execute_comparison_lt/1,
    execute_in_operator/1,
    execute_or_condition/1,
    execute_not_condition/1,
    execute_exists/1,
    execute_missing/1,
    execute_prefix/1,
    execute_regex/1,
    execute_nested_path/1,
    execute_with_limit/1,
    execute_with_offset/1,
    execute_with_order/1,
    execute_include_docs/1,
    execute_variable_binding/1,
    execute_multi_index_intersection/1,
    execute_multi_index_zero_cardinality/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, compilation},
        {group, validation},
        {group, strategy},
        {group, execution}
    ].

groups() ->
    [
        {compilation, [sequence], [
            compile_simple_equality,
            compile_multiple_conditions,
            compile_with_variables,
            compile_comparison,
            compile_and_or,
            compile_in_operator,
            compile_exists_missing,
            compile_regex_prefix,
            compile_invalid_spec,
            compile_invalid_condition,
            compile_invalid_operator
        ]},
        {validation, [sequence], [
            validate_valid_spec,
            validate_missing_where,
            validate_invalid_path,
            validate_invalid_regex
        ]},
        {strategy, [sequence], [
            strategy_index_seek,
            strategy_index_scan,
            strategy_multi_index,
            strategy_full_scan
        ]},
        {execution, [sequence], [
            execute_simple_equality,
            execute_multiple_conditions,
            execute_comparison_gt,
            execute_comparison_lt,
            execute_in_operator,
            execute_or_condition,
            execute_not_condition,
            execute_exists,
            execute_missing,
            execute_prefix,
            execute_regex,
            execute_nested_path,
            execute_with_limit,
            execute_with_offset,
            execute_with_order,
            execute_include_docs,
            execute_variable_binding,
            execute_multi_index_intersection,
            execute_multi_index_zero_cardinality
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(execution, Config) ->
    %% Set up database with test documents for execution tests
    {ok, _} = application:ensure_all_started(barrel_docdb),
    DbName = <<"query_test_db">>,
    {ok, Pid} = barrel_docdb:create_db(DbName, #{}),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Pid),

    %% Insert test documents
    Docs = [
        #{<<"id">> => <<"user1">>, <<"type">> => <<"user">>, <<"name">> => <<"Alice">>,
          <<"age">> => 30, <<"status">> => <<"active">>, <<"org">> => <<"org1">>},
        #{<<"id">> => <<"user2">>, <<"type">> => <<"user">>, <<"name">> => <<"Bob">>,
          <<"age">> => 25, <<"status">> => <<"active">>, <<"org">> => <<"org1">>},
        #{<<"id">> => <<"user3">>, <<"type">> => <<"user">>, <<"name">> => <<"Charlie">>,
          <<"age">> => 35, <<"status">> => <<"inactive">>, <<"org">> => <<"org2">>},
        #{<<"id">> => <<"post1">>, <<"type">> => <<"post">>, <<"title">> => <<"Hello World">>,
          <<"author">> => <<"user1">>, <<"tags">> => [<<"intro">>, <<"welcome">>]},
        #{<<"id">> => <<"post2">>, <<"type">> => <<"post">>, <<"title">> => <<"Goodbye">>,
          <<"author">> => <<"user2">>},
        #{<<"id">> => <<"nested1">>, <<"type">> => <<"nested">>,
          <<"profile">> => #{<<"name">> => <<"Deep">>, <<"address">> => #{<<"city">> => <<"Paris">>}}}
    ],

    lists:foreach(
        fun(Doc) ->
            {ok, _} = barrel_docdb:put_doc(DbName, Doc)
        end,
        Docs
    ),

    [{db_name, DbName}, {store_ref, StoreRef} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(execution, Config) ->
    DbName = proplists:get_value(db_name, Config),
    ok = barrel_docdb:delete_db(DbName),
    ok = application:stop(barrel_docdb),
    Config;
end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Compilation
%%====================================================================

compile_simple_equality(_Config) ->
    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual([{path, [<<"type">>], <<"user">>}], maps:get(conditions, Explained)),
    ok.

compile_multiple_conditions(_Config) ->
    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"status">>], <<"active">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(2, length(maps:get(conditions, Explained))),
    ok.

compile_with_variables(_Config) ->
    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"org">>], '?Org'}
        ],
        select => ['?Org']
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    Bindings = maps:get(bindings, Explained),
    ?assertEqual([<<"org">>], maps:get('?Org', Bindings)),
    ok.

compile_comparison(_Config) ->
    Spec = #{
        where => [
            {compare, [<<"age">>], '>', 18}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    [{compare, [<<"age">>], '>', 18}] = maps:get(conditions, Explained),
    ok.

compile_and_or(_Config) ->
    Spec = #{
        where => [
            {'and', [
                {path, [<<"type">>], <<"user">>},
                {'or', [
                    {path, [<<"status">>], <<"active">>},
                    {path, [<<"status">>], <<"pending">>}
                ]}
            ]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    [{'and', _}] = maps:get(conditions, Explained),
    ok.

compile_in_operator(_Config) ->
    Spec = #{
        where => [
            {in, [<<"status">>], [<<"active">>, <<"pending">>]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    [{in, [<<"status">>], [<<"active">>, <<"pending">>]}] = maps:get(conditions, Explained),
    ok.

compile_exists_missing(_Config) ->
    Spec = #{
        where => [
            {exists, [<<"email">>]},
            {missing, [<<"deleted">>]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(2, length(maps:get(conditions, Explained))),
    ok.

compile_regex_prefix(_Config) ->
    Spec = #{
        where => [
            {regex, [<<"name">>], <<"^A.*">>},
            {prefix, [<<"email">>], <<"admin@">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(2, length(maps:get(conditions, Explained))),
    ok.

compile_invalid_spec(_Config) ->
    ?assertEqual({error, {invalid_spec, not_a_map}}, barrel_query:compile(not_a_map)),
    ok.

compile_invalid_condition(_Config) ->
    Spec = #{
        where => [{invalid_op, [<<"field">>], <<"value">>}]
    },
    {error, {invalid_condition, _}} = barrel_query:compile(Spec),
    ok.

compile_invalid_operator(_Config) ->
    Spec = #{
        where => [{compare, [<<"age">>], 'invalid_op', 18}]
    },
    {error, {invalid_operator, invalid_op}} = barrel_query:compile(Spec),
    ok.

%%====================================================================
%% Test Cases - Validation
%%====================================================================

validate_valid_spec(_Config) ->
    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}]
    },
    ?assertEqual(ok, barrel_query:validate_spec(Spec)),
    ok.

validate_missing_where(_Config) ->
    Spec = #{
        select => ['*']
    },
    ?assertEqual({error, {missing_clause, where}}, barrel_query:validate_spec(Spec)),
    ok.

validate_invalid_path(_Config) ->
    Spec = #{
        where => [{path, not_a_list, <<"value">>}]
    },
    {error, {invalid_path, _, _}} = barrel_query:validate_spec(Spec),
    ok.

validate_invalid_regex(_Config) ->
    Spec = #{
        where => [{regex, [<<"field">>], <<"[invalid">>}]
    },
    {error, {invalid_regex, _}} = barrel_query:validate_spec(Spec),
    ok.

%%====================================================================
%% Test Cases - Strategy
%%====================================================================

strategy_index_seek(_Config) ->
    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(index_seek, maps:get(strategy, Explained)),
    ok.

strategy_index_scan(_Config) ->
    Spec = #{
        where => [{compare, [<<"age">>], '>', 18}]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(index_scan, maps:get(strategy, Explained)),
    ok.

strategy_multi_index(_Config) ->
    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"status">>], <<"active">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(multi_index, maps:get(strategy, Explained)),
    ok.

strategy_full_scan(_Config) ->
    Spec = #{
        where => [
            {path, [<<"name">>], '?Name'}  % Only variable binding, no concrete value
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    Explained = barrel_query:explain(Plan),
    ?assertEqual(full_scan, maps:get(strategy, Explained)),
    ok.

%%====================================================================
%% Test Cases - Execution
%%====================================================================

execute_simple_equality(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{where => [{path, [<<"type">>], <<"user">>}]},
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _LastSeq} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(3, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),
    ?assert(lists:member(<<"user3">>, DocIds)),
    ok.

execute_multiple_conditions(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"status">>], <<"active">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),
    ok.

execute_comparison_gt(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {compare, [<<"age">>], '>', 28}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),  % age 30
    ?assert(lists:member(<<"user3">>, DocIds)),  % age 35
    ok.

execute_comparison_lt(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {compare, [<<"age">>], '<', 30}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assertEqual(<<"user2">>, maps:get(<<"id">>, Result)),  % age 25
    ok.

execute_in_operator(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {in, [<<"org">>], [<<"org1">>]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),
    ok.

execute_or_condition(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {'or', [
                {path, [<<"type">>], <<"user">>},
                {path, [<<"type">>], <<"post">>}
            ]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(5, length(Results)),  % 3 users + 2 posts
    ok.

execute_not_condition(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {'not', {path, [<<"status">>], <<"inactive">>}}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),  % Only active users
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assertNot(lists:member(<<"user3">>, DocIds)),  % user3 is inactive
    ok.

execute_exists(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"post">>},
            {exists, [<<"tags">>]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assertEqual(<<"post1">>, maps:get(<<"id">>, Result)),
    ok.

execute_missing(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"post">>},
            {missing, [<<"tags">>]}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assertEqual(<<"post2">>, maps:get(<<"id">>, Result)),
    ok.

execute_prefix(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {prefix, [<<"name">>], <<"A">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assertEqual(<<"user1">>, maps:get(<<"id">>, Result)),  % Alice
    ok.

execute_regex(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {regex, [<<"name">>], <<"^[AB].*">>}  % Names starting with A or B
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),  % Alice
    ?assert(lists:member(<<"user2">>, DocIds)),  % Bob
    ok.

execute_nested_path(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"profile">>, <<"address">>, <<"city">>], <<"Paris">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assertEqual(<<"nested1">>, maps:get(<<"id">>, Result)),
    ok.

execute_with_limit(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}],
        limit => 2
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    ok.

execute_with_offset(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}],
        offset => 1,
        limit => 2
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    ok.

execute_with_order(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}],
        select => ['?Name'],
        order_by => {'?Name', asc}
    },
    %% First add variable binding for name
    SpecWithBinding = Spec#{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"name">>], '?Name'}
        ]
    },
    {ok, Plan} = barrel_query:compile(SpecWithBinding),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(3, length(Results)),
    Names = [maps:get(<<"?Name">>, R) || R <- Results],
    ?assertEqual([<<"Alice">>, <<"Bob">>, <<"Charlie">>], Names),
    ok.

execute_include_docs(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [{path, [<<"type">>], <<"user">>}],
        include_docs => true,
        limit => 1
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(1, length(Results)),
    [Result] = Results,
    ?assert(maps:is_key(<<"doc">>, Result)),
    Doc = maps:get(<<"doc">>, Result),
    ?assert(maps:is_key(<<"type">>, Doc)),
    ok.

execute_variable_binding(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"org">>], '?Org'},
            {path, [<<"name">>], '?Name'}
        ],
        select => ['?Org', '?Name']
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(3, length(Results)),
    lists:foreach(
        fun(R) ->
            ?assert(maps:is_key(<<"?Org">>, R)),
            ?assert(maps:is_key(<<"?Name">>, R))
        end,
        Results
    ),
    ok.

execute_multi_index_intersection(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"org">>], <<"org1">>},
            {path, [<<"status">>], <<"active">>}
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(2, length(Results)),
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),
    ok.

%% Test that 3+ condition queries with a zero-cardinality condition
%% short-circuit and return empty results immediately
execute_multi_index_zero_cardinality(Config) ->
    DbName = proplists:get_value(db_name, Config),
    StoreRef = proplists:get_value(store_ref, Config),

    %% Query with nonexistent value - should short-circuit and return empty
    Spec = #{
        where => [
            {path, [<<"type">>], <<"user">>},
            {path, [<<"org">>], <<"org1">>},
            {path, [<<"status">>], <<"nonexistent">>}  %% No docs have this status
        ]
    },
    {ok, Plan} = barrel_query:compile(Spec),
    {ok, Results, _} = barrel_query:execute(StoreRef, DbName, Plan),

    ?assertEqual(0, length(Results)),
    ok.
