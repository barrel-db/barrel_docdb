%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb views
%%%
%%% Tests view registration, indexing, querying, and reduce.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_view_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    %% View registration
    register_view/1,
    unregister_view/1,
    list_views/1,

    %% View indexing
    index_on_register/1,
    index_on_doc_write/1,
    index_on_doc_delete/1,

    %% View query
    query_all/1,
    query_range/1,
    query_limit_skip/1,

    %% Reduce
    reduce_count/1,
    reduce_sum/1,
    reduce_group/1,
    reduce_group_level/1,

    %% Query-based views
    query_view_register/1,
    query_view_index/1,
    query_view_query/1,
    query_view_with_reduce/1,
    query_view_compound_key/1,
    query_view_manual_refresh/1
]).

%% Test view module (for testing)
-export([version/0, map/1, reduce/3]).

%%====================================================================
%% View behaviour callbacks (test view)
%%====================================================================

version() -> 1.

map(#{<<"type">> := Type, <<"value">> := Value}) ->
    [{Type, Value}];
map(#{<<"type">> := Type}) ->
    [{Type, 1}];
map(_) ->
    [].

reduce(_Keys, Values, _Rereduce) ->
    lists:sum(Values).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, registration}, {group, indexing}, {group, query}, {group, reduce}, {group, query_views}].

groups() ->
    [
        {registration, [sequence], [
            register_view,
            unregister_view,
            list_views
        ]},
        {indexing, [sequence], [
            index_on_register,
            index_on_doc_write,
            index_on_doc_delete
        ]},
        {query, [sequence], [
            query_all,
            query_range,
            query_limit_skip
        ]},
        {reduce, [sequence], [
            reduce_count,
            reduce_sum,
            reduce_group,
            reduce_group_level
        ]},
        {query_views, [sequence], [
            query_view_register,
            query_view_index,
            query_view_query,
            query_view_with_reduce,
            query_view_compound_key,
            query_view_manual_refresh
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Create a fresh database for each test
    DbName = <<"view_test_", (integer_to_binary(erlang:system_time(millisecond)))/binary>>,
    TestDir = "/tmp/barrel_view_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    DbConfig = #{data_dir => TestDir},
    {ok, DbPid} = barrel_db_sup:start_db(DbName, DbConfig),
    {ok, StoreRef} = barrel_db_server:get_store_ref(DbPid),
    [{db_name, DbName}, {db_pid, DbPid}, {store_ref, StoreRef}, {test_dir, TestDir} | Config].

end_per_testcase(_TestCase, Config) ->
    DbPid = ?config(db_pid, Config),
    TestDir = ?config(test_dir, Config),
    ok = barrel_db_server:stop(DbPid),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%%====================================================================
%% Test Cases - Registration
%%====================================================================

register_view(Config) ->
    DbPid = ?config(db_pid, Config),

    %% Register a view
    ViewConfig = #{module => ?MODULE},
    ok = barrel_db_server:register_view(DbPid, <<"by_type">>, ViewConfig),

    %% Verify view is registered
    {ok, ViewPid} = barrel_db_server:get_view_pid(DbPid, <<"by_type">>),
    ?assert(is_pid(ViewPid)),
    ?assert(is_process_alive(ViewPid)),

    %% Cannot register twice
    {error, already_registered} = barrel_db_server:register_view(DbPid, <<"by_type">>, ViewConfig),

    ok.

unregister_view(Config) ->
    DbPid = ?config(db_pid, Config),

    %% Register a view
    ViewConfig = #{module => ?MODULE},
    ok = barrel_db_server:register_view(DbPid, <<"by_type">>, ViewConfig),

    %% Get pid before unregister
    {ok, ViewPid} = barrel_db_server:get_view_pid(DbPid, <<"by_type">>),

    %% Unregister
    ok = barrel_db_server:unregister_view(DbPid, <<"by_type">>),

    %% View process should be stopped
    timer:sleep(100),
    ?assertNot(is_process_alive(ViewPid)),

    %% Cannot get view pid anymore
    {error, not_found} = barrel_db_server:get_view_pid(DbPid, <<"by_type">>),

    ok.

list_views(Config) ->
    DbPid = ?config(db_pid, Config),

    %% Initially no views
    {ok, []} = barrel_db_server:list_views(DbPid),

    %% Register views
    ok = barrel_db_server:register_view(DbPid, <<"view1">>, #{module => ?MODULE}),
    ok = barrel_db_server:register_view(DbPid, <<"view2">>, #{module => ?MODULE}),

    %% List views
    {ok, Views} = barrel_db_server:list_views(DbPid),
    ?assertEqual(2, length(Views)),

    %% Check view IDs are present
    ViewIds = [maps:get(id, V) || V <- Views],
    ?assert(lists:member(<<"view1">>, ViewIds)),
    ?assert(lists:member(<<"view2">>, ViewIds)),

    ok.

%%====================================================================
%% Test Cases - Indexing
%%====================================================================

index_on_register(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    %% Write some documents first
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc1">>, <<"type">> => <<"user">>, <<"value">> => 10},
        #{<<"_id">> => <<"doc2">>, <<"type">> => <<"user">>, <<"value">> => 20},
        #{<<"_id">> => <<"doc3">>, <<"type">> => <<"post">>, <<"value">> => 5}
    ]),

    %% Register view - should index existing docs
    ok = barrel_db_server:register_view(DbPid, <<"by_type">>, #{module => ?MODULE}),

    %% Wait for indexing
    {ok, _Seq} = barrel_view:refresh(DbPid, <<"by_type">>),

    %% Query should return indexed entries
    {ok, Results} = barrel_view:query(DbPid, <<"by_type">>, #{reduce => false}),
    ?assertEqual(3, length(Results)),

    ok.

index_on_doc_write(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    %% Register view first
    ok = barrel_db_server:register_view(DbPid, <<"by_type">>, #{module => ?MODULE}),
    {ok, _} = barrel_view:refresh(DbPid, <<"by_type">>),

    %% Initially empty
    {ok, []} = barrel_view:query(DbPid, <<"by_type">>, #{reduce => false}),

    %% Write a document
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc1">>, <<"type">> => <<"user">>, <<"value">> => 10}
    ]),

    %% Refresh and query
    {ok, _} = barrel_view:refresh(DbPid, <<"by_type">>),
    {ok, Results} = barrel_view:query(DbPid, <<"by_type">>, #{reduce => false}),
    ?assertEqual(1, length(Results)),

    %% Write more documents
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc2">>, <<"type">> => <<"user">>, <<"value">> => 20}
    ]),

    {ok, _} = barrel_view:refresh(DbPid, <<"by_type">>),
    {ok, Results2} = barrel_view:query(DbPid, <<"by_type">>, #{reduce => false}),
    ?assertEqual(2, length(Results2)),

    ok.

index_on_doc_delete(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    %% Write documents
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc1">>, <<"type">> => <<"user">>, <<"value">> => 10},
        #{<<"_id">> => <<"doc2">>, <<"type">> => <<"user">>, <<"value">> => 20}
    ]),

    %% Register and refresh
    ok = barrel_db_server:register_view(DbPid, <<"by_type">>, #{module => ?MODULE}),
    {ok, _} = barrel_view:refresh(DbPid, <<"by_type">>),

    {ok, Results1} = barrel_view:query(DbPid, <<"by_type">>, #{reduce => false}),
    ?assertEqual(2, length(Results1)),

    %% Delete a document (by writing a change with deleted=true)
    delete_test_doc(StoreRef, DbName, <<"doc1">>),

    %% Refresh and query
    {ok, _} = barrel_view:refresh(DbPid, <<"by_type">>),
    {ok, Results2} = barrel_view:query(DbPid, <<"by_type">>, #{reduce => false}),
    ?assertEqual(1, length(Results2)),

    ok.

%%====================================================================
%% Test Cases - Query
%%====================================================================

query_all(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    %% Write documents
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc1">>, <<"type">> => <<"a">>, <<"value">> => 1},
        #{<<"_id">> => <<"doc2">>, <<"type">> => <<"b">>, <<"value">> => 2},
        #{<<"_id">> => <<"doc3">>, <<"type">> => <<"c">>, <<"value">> => 3}
    ]),

    ok = barrel_db_server:register_view(DbPid, <<"by_type">>, #{module => ?MODULE}),
    {ok, _} = barrel_view:refresh(DbPid, <<"by_type">>),

    %% Query all
    {ok, Results} = barrel_view:query(DbPid, <<"by_type">>, #{reduce => false}),
    ?assertEqual(3, length(Results)),

    %% Verify entries have key, value, id
    [First | _] = Results,
    ?assert(maps:is_key(key, First)),
    ?assert(maps:is_key(value, First)),
    ?assert(maps:is_key(id, First)),

    ok.

query_range(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc1">>, <<"type">> => <<"a">>, <<"value">> => 1},
        #{<<"_id">> => <<"doc2">>, <<"type">> => <<"b">>, <<"value">> => 2},
        #{<<"_id">> => <<"doc3">>, <<"type">> => <<"b">>, <<"value">> => 3},
        #{<<"_id">> => <<"doc4">>, <<"type">> => <<"c">>, <<"value">> => 4}
    ]),

    ok = barrel_db_server:register_view(DbPid, <<"by_type">>, #{module => ?MODULE}),
    {ok, _} = barrel_view:refresh(DbPid, <<"by_type">>),

    %% Query range for type "b"
    {ok, Results} = barrel_view:query(DbPid, <<"by_type">>, #{
        start_key => <<"b">>,
        end_key => <<"b">>,
        reduce => false
    }),
    ?assertEqual(2, length(Results)),

    %% All results should have key "b"
    lists:foreach(fun(R) -> ?assertEqual(<<"b">>, maps:get(key, R)) end, Results),

    ok.

query_limit_skip(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc1">>, <<"type">> => <<"a">>, <<"value">> => 1},
        #{<<"_id">> => <<"doc2">>, <<"type">> => <<"b">>, <<"value">> => 2},
        #{<<"_id">> => <<"doc3">>, <<"type">> => <<"c">>, <<"value">> => 3},
        #{<<"_id">> => <<"doc4">>, <<"type">> => <<"d">>, <<"value">> => 4},
        #{<<"_id">> => <<"doc5">>, <<"type">> => <<"e">>, <<"value">> => 5}
    ]),

    ok = barrel_db_server:register_view(DbPid, <<"by_type">>, #{module => ?MODULE}),
    {ok, _} = barrel_view:refresh(DbPid, <<"by_type">>),

    %% Query with limit
    {ok, Results1} = barrel_view:query(DbPid, <<"by_type">>, #{limit => 2, reduce => false}),
    ?assertEqual(2, length(Results1)),

    %% Query with skip
    {ok, Results2} = barrel_view:query(DbPid, <<"by_type">>, #{skip => 3, reduce => false}),
    ?assertEqual(2, length(Results2)),

    %% Query with limit and skip
    {ok, Results3} = barrel_view:query(DbPid, <<"by_type">>, #{limit => 2, skip => 1, reduce => false}),
    ?assertEqual(2, length(Results3)),

    ok.

%%====================================================================
%% Test Cases - Reduce
%%====================================================================

reduce_count(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc1">>, <<"type">> => <<"user">>},
        #{<<"_id">> => <<"doc2">>, <<"type">> => <<"user">>},
        #{<<"_id">> => <<"doc3">>, <<"type">> => <<"post">>}
    ]),

    %% Register with built-in _count reduce
    ok = barrel_db_server:register_view(DbPid, <<"count_view">>, #{
        module => ?MODULE,
        reduce => '_count'
    }),
    {ok, _} = barrel_view:refresh(DbPid, <<"count_view">>),

    %% Query with reduce (no grouping)
    {ok, Results} = barrel_view:query(DbPid, <<"count_view">>, #{reduce => true}),
    ?assertEqual(1, length(Results)),
    [#{value := Count}] = Results,
    ?assertEqual(3, Count),

    ok.

reduce_sum(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc1">>, <<"type">> => <<"a">>, <<"value">> => 10},
        #{<<"_id">> => <<"doc2">>, <<"type">> => <<"a">>, <<"value">> => 20},
        #{<<"_id">> => <<"doc3">>, <<"type">> => <<"b">>, <<"value">> => 30}
    ]),

    %% Register with module reduce (sums values)
    ok = barrel_db_server:register_view(DbPid, <<"sum_view">>, #{module => ?MODULE}),
    {ok, _} = barrel_view:refresh(DbPid, <<"sum_view">>),

    %% Query with reduce
    {ok, Results} = barrel_view:query(DbPid, <<"sum_view">>, #{reduce => true}),
    ?assertEqual(1, length(Results)),
    [#{value := Sum}] = Results,
    ?assertEqual(60, Sum),

    ok.

reduce_group(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc1">>, <<"type">> => <<"a">>, <<"value">> => 10},
        #{<<"_id">> => <<"doc2">>, <<"type">> => <<"a">>, <<"value">> => 20},
        #{<<"_id">> => <<"doc3">>, <<"type">> => <<"b">>, <<"value">> => 30}
    ]),

    ok = barrel_db_server:register_view(DbPid, <<"group_view">>, #{module => ?MODULE}),
    {ok, _} = barrel_view:refresh(DbPid, <<"group_view">>),

    %% Query with reduce and group
    {ok, Results} = barrel_view:query(DbPid, <<"group_view">>, #{reduce => true, group => true}),
    ?assertEqual(2, length(Results)),

    %% Find sum for each group
    ResultMap = maps:from_list([{maps:get(key, R), maps:get(value, R)} || R <- Results]),
    ?assertEqual(30, maps:get(<<"a">>, ResultMap)),
    ?assertEqual(30, maps:get(<<"b">>, ResultMap)),

    ok.

reduce_group_level(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    %% Write docs with compound keys in view
    %% For this test, we'll use type as the key (since our simple view doesn't support compound keys)
    %% A real test would need a view that emits compound keys like [type, year]
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"doc1">>, <<"type">> => <<"user">>, <<"value">> => 1},
        #{<<"_id">> => <<"doc2">>, <<"type">> => <<"user">>, <<"value">> => 2},
        #{<<"_id">> => <<"doc3">>, <<"type">> => <<"post">>, <<"value">> => 3}
    ]),

    ok = barrel_db_server:register_view(DbPid, <<"level_view">>, #{module => ?MODULE}),
    {ok, _} = barrel_view:refresh(DbPid, <<"level_view">>),

    %% Query with group_level (for simple keys, group_level=1 is same as group=true)
    {ok, Results} = barrel_view:query(DbPid, <<"level_view">>, #{
        reduce => true,
        group => true,
        group_level => 1
    }),
    ?assertEqual(2, length(Results)),

    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

write_test_docs(StoreRef, DbName, Docs) ->
    lists:foreach(
        fun(Doc) ->
            DocId = maps:get(<<"_id">>, Doc),
            Rev = <<"1-abc123">>,
            DocInfo = #{
                id => DocId,
                rev => Rev,
                deleted => false,
                revtree => #{Rev => #{id => Rev, parent => undefined, deleted => false}}
            },
            %% Write doc_info
            DocInfoKey = barrel_store_keys:doc_info(DbName, DocId),
            barrel_store_rocksdb:put(StoreRef, DocInfoKey, term_to_binary(DocInfo)),

            %% Write doc body
            DocRevKey = barrel_store_keys:doc_rev(DbName, DocId, Rev),
            barrel_store_rocksdb:put(StoreRef, DocRevKey, term_to_binary(Doc)),

            %% Generate new HLC and write change
            NextHlc = barrel_hlc:new_hlc(),
            %% Write change with doc included
            ChangeInfo = DocInfo#{doc => Doc, hlc => NextHlc},
            barrel_changes:write_change(StoreRef, DbName, NextHlc, ChangeInfo)
        end,
        Docs
    ).

delete_test_doc(StoreRef, DbName, DocId) ->
    %% Get current doc info
    DocInfoKey = barrel_store_keys:doc_info(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, DocInfoKey) of
        {ok, Bin} ->
            DocInfo = binary_to_term(Bin),
            %% Mark as deleted
            NextHlc = barrel_hlc:new_hlc(),
            DeletedInfo = DocInfo#{deleted => true, hlc => NextHlc},
            barrel_store_rocksdb:put(StoreRef, DocInfoKey, term_to_binary(DeletedInfo)),

            %% Write change
            barrel_changes:write_change(StoreRef, DbName, NextHlc, DeletedInfo#{deleted => true});
        not_found ->
            ok
    end.

%%====================================================================
%% Test Cases - Query-based Views
%%====================================================================

query_view_register(Config) ->
    DbPid = ?config(db_pid, Config),

    %% Register a query-based view
    ViewConfig = #{
        query => #{
            where => [
                {path, [<<"type">>], <<"user">>},
                {path, [<<"org_id">>], '?Org'}
            ],
            key => '?Org'
        }
    },
    ok = barrel_db_server:register_view(DbPid, <<"users_by_org">>, ViewConfig),

    %% Verify view is registered
    {ok, ViewPid} = barrel_db_server:get_view_pid(DbPid, <<"users_by_org">>),
    ?assert(is_pid(ViewPid)),
    ?assert(is_process_alive(ViewPid)),

    %% Cannot register twice
    {error, already_registered} = barrel_db_server:register_view(DbPid, <<"users_by_org">>, ViewConfig),

    ok.

query_view_index(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    %% Write some documents
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"user1">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org1">>, <<"name">> => <<"Alice">>},
        #{<<"_id">> => <<"user2">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org1">>, <<"name">> => <<"Bob">>},
        #{<<"_id">> => <<"user3">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org2">>, <<"name">> => <<"Charlie">>},
        #{<<"_id">> => <<"post1">>, <<"type">> => <<"post">>, <<"title">> => <<"Hello">>}  % Won't match
    ]),

    %% Register query-based view
    ViewConfig = #{
        query => #{
            where => [
                {path, [<<"type">>], <<"user">>},
                {path, [<<"org_id">>], '?Org'}
            ],
            key => '?Org'
        }
    },
    ok = barrel_db_server:register_view(DbPid, <<"users_by_org">>, ViewConfig),
    {ok, _} = barrel_view:refresh(DbPid, <<"users_by_org">>),

    %% Query - should only index users, not posts
    {ok, Results} = barrel_view:query(DbPid, <<"users_by_org">>, #{reduce => false}),
    ?assertEqual(3, length(Results)),

    %% Verify all indexed documents are users
    DocIds = [maps:get(id, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),
    ?assert(lists:member(<<"user3">>, DocIds)),
    ?assertNot(lists:member(<<"post1">>, DocIds)),

    %% Verify keys are org_ids
    Keys = [maps:get(key, R) || R <- Results],
    ?assert(lists:member(<<"org1">>, Keys)),
    ?assert(lists:member(<<"org2">>, Keys)),

    ok.

query_view_query(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    %% Write documents
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"user1">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org1">>},
        #{<<"_id">> => <<"user2">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org1">>},
        #{<<"_id">> => <<"user3">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org2">>}
    ]),

    %% Register view
    ViewConfig = #{
        query => #{
            where => [
                {path, [<<"type">>], <<"user">>},
                {path, [<<"org_id">>], '?Org'}
            ],
            key => '?Org'
        }
    },
    ok = barrel_db_server:register_view(DbPid, <<"users_by_org">>, ViewConfig),
    {ok, _} = barrel_view:refresh(DbPid, <<"users_by_org">>),

    %% Query by key range - should return only org1 users
    {ok, Results1} = barrel_view:query(DbPid, <<"users_by_org">>, #{
        start_key => <<"org1">>,
        end_key => <<"org1">>,
        reduce => false
    }),
    ?assertEqual(2, length(Results1)),

    %% Verify results are org1 users only
    Org1DocIds = [maps:get(id, R) || R <- Results1],
    ?assert(lists:member(<<"user1">>, Org1DocIds)),
    ?assert(lists:member(<<"user2">>, Org1DocIds)),
    ?assertNot(lists:member(<<"user3">>, Org1DocIds)),

    %% All results should have key "org1"
    Org1Keys = [maps:get(key, R) || R <- Results1],
    ?assert(lists:all(fun(K) -> K =:= <<"org1">> end, Org1Keys)),

    %% Query all
    {ok, Results2} = barrel_view:query(DbPid, <<"users_by_org">>, #{reduce => false}),
    ?assertEqual(3, length(Results2)),

    %% Verify all users are present
    AllDocIds = [maps:get(id, R) || R <- Results2],
    ?assert(lists:member(<<"user1">>, AllDocIds)),
    ?assert(lists:member(<<"user2">>, AllDocIds)),
    ?assert(lists:member(<<"user3">>, AllDocIds)),

    ok.

query_view_with_reduce(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    %% Write documents
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"user1">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org1">>},
        #{<<"_id">> => <<"user2">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org1">>},
        #{<<"_id">> => <<"user3">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org2">>}
    ]),

    %% Register view with _count reduce
    ViewConfig = #{
        query => #{
            where => [
                {path, [<<"type">>], <<"user">>},
                {path, [<<"org_id">>], '?Org'}
            ],
            key => '?Org'
        },
        reduce => '_count'
    },
    ok = barrel_db_server:register_view(DbPid, <<"users_count_by_org">>, ViewConfig),
    {ok, _} = barrel_view:refresh(DbPid, <<"users_count_by_org">>),

    %% Query with reduce and group
    {ok, Results} = barrel_view:query(DbPid, <<"users_count_by_org">>, #{
        reduce => true,
        group => true
    }),
    ?assertEqual(2, length(Results)),

    %% Check counts per org
    ResultMap = maps:from_list([{maps:get(key, R), maps:get(value, R)} || R <- Results]),
    ?assertEqual(2, maps:get(<<"org1">>, ResultMap)),
    ?assertEqual(1, maps:get(<<"org2">>, ResultMap)),

    ok.

query_view_compound_key(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    %% Write documents
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"user1">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org1">>, <<"status">> => <<"active">>},
        #{<<"_id">> => <<"user2">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org1">>, <<"status">> => <<"inactive">>},
        #{<<"_id">> => <<"user3">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org2">>, <<"status">> => <<"active">>}
    ]),

    %% Register view with compound key [org_id, status]
    ViewConfig = #{
        query => #{
            where => [
                {path, [<<"type">>], <<"user">>},
                {path, [<<"org_id">>], '?Org'},
                {path, [<<"status">>], '?Status'}
            ],
            key => ['?Org', '?Status']
        }
    },
    ok = barrel_db_server:register_view(DbPid, <<"users_by_org_status">>, ViewConfig),
    {ok, _} = barrel_view:refresh(DbPid, <<"users_by_org_status">>),

    %% Query all
    {ok, Results} = barrel_view:query(DbPid, <<"users_by_org_status">>, #{reduce => false}),
    ?assertEqual(3, length(Results)),

    %% Check that all keys are compound (lists of 2 elements)
    lists:foreach(fun(R) ->
        Key = maps:get(key, R),
        ?assert(is_list(Key)),
        ?assertEqual(2, length(Key)),
        [Org, Status] = Key,
        ?assert(is_binary(Org)),
        ?assert(is_binary(Status))
    end, Results),

    %% Verify specific compound keys exist
    Keys = [maps:get(key, R) || R <- Results],
    ?assert(lists:member([<<"org1">>, <<"active">>], Keys)),
    ?assert(lists:member([<<"org1">>, <<"inactive">>], Keys)),
    ?assert(lists:member([<<"org2">>, <<"active">>], Keys)),

    %% Verify doc ids match expected keys
    KeyDocPairs = [{maps:get(key, R), maps:get(id, R)} || R <- Results],
    ?assert(lists:member({[<<"org1">>, <<"active">>], <<"user1">>}, KeyDocPairs)),
    ?assert(lists:member({[<<"org1">>, <<"inactive">>], <<"user2">>}, KeyDocPairs)),
    ?assert(lists:member({[<<"org2">>, <<"active">>], <<"user3">>}, KeyDocPairs)),

    ok.

query_view_manual_refresh(Config) ->
    DbPid = ?config(db_pid, Config),
    DbName = ?config(db_name, Config),
    StoreRef = ?config(store_ref, Config),

    %% Register view with manual refresh mode
    ViewConfig = #{
        query => #{
            where => [
                {path, [<<"type">>], <<"user">>},
                {path, [<<"org_id">>], '?Org'}
            ],
            key => '?Org'
        },
        refresh => manual
    },
    ok = barrel_db_server:register_view(DbPid, <<"manual_view">>, ViewConfig),

    %% Write documents after registration (should not auto-index)
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"user1">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org1">>},
        #{<<"_id">> => <<"user2">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org2">>}
    ]),

    %% Give time for auto-update to NOT happen
    timer:sleep(100),

    %% Query without refresh - should be empty
    {ok, Results1} = barrel_view:query(DbPid, <<"manual_view">>, #{reduce => false}),
    ?assertEqual(0, length(Results1)),

    %% Manually refresh
    {ok, _} = barrel_view:refresh(DbPid, <<"manual_view">>),

    %% Now should have results
    {ok, Results2} = barrel_view:query(DbPid, <<"manual_view">>, #{reduce => false}),
    ?assertEqual(2, length(Results2)),

    %% Verify content
    DocIds = [maps:get(id, R) || R <- Results2],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),

    Keys = [maps:get(key, R) || R <- Results2],
    ?assert(lists:member(<<"org1">>, Keys)),
    ?assert(lists:member(<<"org2">>, Keys)),

    %% Write more documents - should not appear until refresh
    write_test_docs(StoreRef, DbName, [
        #{<<"_id">> => <<"user3">>, <<"type">> => <<"user">>, <<"org_id">> => <<"org3">>}
    ]),

    %% Query again - still only 2 results
    {ok, Results3} = barrel_view:query(DbPid, <<"manual_view">>, #{reduce => false}),
    ?assertEqual(2, length(Results3)),

    %% Refresh again
    {ok, _} = barrel_view:refresh(DbPid, <<"manual_view">>),

    %% Now should have 3 results
    {ok, Results4} = barrel_view:query(DbPid, <<"manual_view">>, #{reduce => false}),
    ?assertEqual(3, length(Results4)),

    DocIds4 = [maps:get(id, R) || R <- Results4],
    ?assert(lists:member(<<"user3">>, DocIds4)),

    ok.
