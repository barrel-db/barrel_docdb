%%%-------------------------------------------------------------------
%%% @doc Tests for barrel_vdb module
%%%
%%% Tests virtual database operations including routing and sharding.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Common Test callbacks
%%====================================================================

all() ->
    [
        {group, lifecycle},
        {group, document_ops},
        {group, bulk_ops},
        {group, query_ops}
    ].

groups() ->
    [
        {lifecycle, [], [
            create_vdb,
            create_vdb_already_exists,
            delete_vdb,
            delete_vdb_not_found,
            exists_vdb,
            list_vdbs,
            vdb_info
        ]},
        {document_ops, [], [
            put_doc,
            put_doc_generated_id,
            get_doc,
            get_doc_not_found,
            delete_doc,
            doc_routing_consistency
        ]},
        {bulk_ops, [], [
            bulk_docs_insert,
            bulk_docs_mixed
        ]},
        {query_ops, [], [
            find_all,
            find_with_selector,
            find_with_limit,
            find_with_offset,
            find_with_sort,
            get_changes_basic,
            fold_docs
        ]}
    ].

init_per_suite(Config) ->
    application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    application:stop(barrel_docdb),
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Clean up any test VDBs
    cleanup_test_vdbs(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    cleanup_test_vdbs(),
    ok.

cleanup_test_vdbs() ->
    {ok, VDBs} = barrel_vdb:list(),
    lists:foreach(fun(Name) ->
        case binary:match(Name, <<"test_">>) of
            {0, _} -> barrel_vdb:delete(Name);
            _ -> ok
        end
    end, VDBs).

%%====================================================================
%% Test Cases - Lifecycle
%%====================================================================

create_vdb(_Config) ->
    VdbName = <<"test_users">>,
    Opts = #{shard_count => 4},
    ?assertEqual(ok, barrel_vdb:create(VdbName, Opts)),
    ?assert(barrel_vdb:exists(VdbName)),
    %% Verify shards were created
    {ok, Info} = barrel_vdb:info(VdbName),
    ?assertEqual(4, maps:get(shard_count, Info)),
    ?assertEqual(4, length(maps:get(shards, Info))).

create_vdb_already_exists(_Config) ->
    VdbName = <<"test_dup">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{})),
    ?assertEqual({error, already_exists}, barrel_vdb:create(VdbName, #{})).

delete_vdb(_Config) ->
    VdbName = <<"test_delete">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    ?assert(barrel_vdb:exists(VdbName)),
    ?assertEqual(ok, barrel_vdb:delete(VdbName)),
    ?assertNot(barrel_vdb:exists(VdbName)),
    %% Verify shard DBs were deleted
    ?assertEqual({error, not_found}, barrel_docdb:db_info(<<"test_delete_s0">>)),
    ?assertEqual({error, not_found}, barrel_docdb:db_info(<<"test_delete_s1">>)).

delete_vdb_not_found(_Config) ->
    ?assertEqual({error, not_found}, barrel_vdb:delete(<<"nonexistent">>)).

exists_vdb(_Config) ->
    VdbName = <<"test_exists">>,
    ?assertNot(barrel_vdb:exists(VdbName)),
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{})),
    ?assert(barrel_vdb:exists(VdbName)).

list_vdbs(_Config) ->
    ?assertEqual(ok, barrel_vdb:create(<<"test_list_a">>, #{})),
    ?assertEqual(ok, barrel_vdb:create(<<"test_list_b">>, #{})),
    {ok, VDBs} = barrel_vdb:list(),
    ?assert(lists:member(<<"test_list_a">>, VDBs)),
    ?assert(lists:member(<<"test_list_b">>, VDBs)).

vdb_info(_Config) ->
    VdbName = <<"test_info">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    %% Add some documents
    {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => <<"doc1">>, <<"x">> => 1}),
    {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => <<"doc2">>, <<"x">> => 2}),
    %% Get info
    {ok, Info} = barrel_vdb:info(VdbName),
    ?assertEqual(VdbName, maps:get(name, Info)),
    ?assertEqual(2, maps:get(shard_count, Info)),
    ?assertEqual(2, maps:get(total_docs, Info)),
    ?assert(maps:get(total_disk_size, Info) >= 0).

%%====================================================================
%% Test Cases - Document Operations
%%====================================================================

put_doc(_Config) ->
    VdbName = <<"test_put">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 4})),
    Doc = #{<<"id">> => <<"mydoc">>, <<"name">> => <<"Test">>},
    {ok, Result} = barrel_vdb:put_doc(VdbName, Doc),
    ?assertEqual(<<"mydoc">>, maps:get(<<"id">>, Result)),
    ?assert(is_binary(maps:get(<<"rev">>, Result))).

put_doc_generated_id(_Config) ->
    VdbName = <<"test_put_gen">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    Doc = #{<<"name">> => <<"NoId">>},
    {ok, Result} = barrel_vdb:put_doc(VdbName, Doc),
    ?assert(is_binary(maps:get(<<"id">>, Result))),
    ?assert(byte_size(maps:get(<<"id">>, Result)) > 0),
    ?assert(is_binary(maps:get(<<"rev">>, Result))).

get_doc(_Config) ->
    VdbName = <<"test_get">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 4})),
    OrigDoc = #{<<"id">> => <<"getme">>, <<"value">> => 42},
    {ok, _} = barrel_vdb:put_doc(VdbName, OrigDoc),
    {ok, Doc} = barrel_vdb:get_doc(VdbName, <<"getme">>),
    ?assertEqual(<<"getme">>, maps:get(<<"id">>, Doc)),
    ?assertEqual(42, maps:get(<<"value">>, Doc)).

get_doc_not_found(_Config) ->
    VdbName = <<"test_get_404">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{})),
    ?assertEqual({error, not_found}, barrel_vdb:get_doc(VdbName, <<"missing">>)).

delete_doc(_Config) ->
    VdbName = <<"test_del">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => <<"todelete">>}),
    {ok, _} = barrel_vdb:get_doc(VdbName, <<"todelete">>),
    {ok, _} = barrel_vdb:delete_doc(VdbName, <<"todelete">>),
    ?assertEqual({error, not_found}, barrel_vdb:get_doc(VdbName, <<"todelete">>)).

doc_routing_consistency(_Config) ->
    VdbName = <<"test_routing">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 8})),
    %% Create multiple docs and verify they can be retrieved
    DocIds = [list_to_binary("doc" ++ integer_to_list(I)) || I <- lists:seq(1, 100)],
    lists:foreach(fun(DocId) ->
        {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => DocId, <<"data">> => DocId})
    end, DocIds),
    %% Verify all docs can be retrieved
    lists:foreach(fun(DocId) ->
        {ok, Doc} = barrel_vdb:get_doc(VdbName, DocId),
        ?assertEqual(DocId, maps:get(<<"id">>, Doc))
    end, DocIds).

%%====================================================================
%% Test Cases - Bulk Operations
%%====================================================================

bulk_docs_insert(_Config) ->
    VdbName = <<"test_bulk">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 4})),
    Docs = [
        #{<<"id">> => <<"bulk1">>, <<"n">> => 1},
        #{<<"id">> => <<"bulk2">>, <<"n">> => 2},
        #{<<"id">> => <<"bulk3">>, <<"n">> => 3},
        #{<<"id">> => <<"bulk4">>, <<"n">> => 4}
    ],
    {ok, Results} = barrel_vdb:bulk_docs(VdbName, Docs),
    ?assertEqual(4, length(Results)),
    %% Verify all docs exist
    lists:foreach(fun(#{<<"id">> := Id}) ->
        {ok, _} = barrel_vdb:get_doc(VdbName, Id)
    end, Docs).

bulk_docs_mixed(_Config) ->
    VdbName = <<"test_bulk_mix">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    %% Insert initial doc
    {ok, #{<<"rev">> := Rev1}} = barrel_vdb:put_doc(VdbName, #{<<"id">> => <<"existing">>, <<"v">> => 1}),
    %% Bulk with mix of new and update
    Docs = [
        #{<<"id">> => <<"new1">>},
        #{<<"id">> => <<"existing">>, <<"_rev">> => Rev1, <<"v">> => 2}
    ],
    {ok, Results} = barrel_vdb:bulk_docs(VdbName, Docs),
    ?assertEqual(2, length(Results)),
    %% Verify update
    {ok, Updated} = barrel_vdb:get_doc(VdbName, <<"existing">>),
    ?assertEqual(2, maps:get(<<"v">>, Updated)).

%%====================================================================
%% Test Cases - Query Operations
%%====================================================================

find_all(_Config) ->
    VdbName = <<"test_find_all">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 4})),
    %% Insert docs across shards
    lists:foreach(fun(I) ->
        DocId = list_to_binary("find" ++ integer_to_list(I)),
        {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => DocId, <<"i">> => I})
    end, lists:seq(1, 20)),
    %% Find all
    {ok, Results} = barrel_vdb:find(VdbName, #{}),
    ?assertEqual(20, length(Results)).

find_with_selector(_Config) ->
    VdbName = <<"test_find_sel">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    %% Insert docs with different types
    {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => <<"a">>, <<"type">> => <<"x">>}),
    {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => <<"b">>, <<"type">> => <<"y">>}),
    {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => <<"c">>, <<"type">> => <<"x">>}),
    %% Find by selector
    {ok, Results} = barrel_vdb:find(VdbName, #{<<"selector">> => #{<<"type">> => <<"x">>}}),
    ?assertEqual(2, length(Results)),
    Types = [maps:get(<<"type">>, R) || R <- Results],
    ?assert(lists:all(fun(T) -> T =:= <<"x">> end, Types)).

find_with_limit(_Config) ->
    VdbName = <<"test_find_lim">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 4})),
    %% Insert many docs
    lists:foreach(fun(I) ->
        DocId = list_to_binary("lim" ++ integer_to_list(I)),
        {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => DocId})
    end, lists:seq(1, 50)),
    %% Find with limit
    {ok, Results} = barrel_vdb:find(VdbName, #{}, #{limit => 10}),
    ?assertEqual(10, length(Results)).

find_with_offset(_Config) ->
    VdbName = <<"test_find_off">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    %% Insert docs with sequential IDs
    lists:foreach(fun(I) ->
        DocId = list_to_binary(io_lib:format("off~3..0B", [I])),
        {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => DocId, <<"n">> => I})
    end, lists:seq(1, 30)),
    %% Find with offset and limit (pagination)
    {ok, Page1} = barrel_vdb:find(VdbName, #{}, #{limit => 10, offset => 0}),
    ?assertEqual(10, length(Page1)),
    {ok, Page2} = barrel_vdb:find(VdbName, #{}, #{limit => 10, offset => 10}),
    ?assertEqual(10, length(Page2)),
    {ok, Page3} = barrel_vdb:find(VdbName, #{}, #{limit => 10, offset => 20}),
    ?assertEqual(10, length(Page3)),
    %% Pages should not overlap (different doc IDs)
    Page1Ids = [maps:get(<<"id">>, D) || D <- Page1],
    Page2Ids = [maps:get(<<"id">>, D) || D <- Page2],
    ?assertEqual([], Page1Ids -- (Page1Ids -- Page2Ids)).

find_with_sort(_Config) ->
    VdbName = <<"test_find_sort">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 4})),
    %% Insert docs with different values
    lists:foreach(fun(I) ->
        DocId = list_to_binary("sort" ++ integer_to_list(I)),
        {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => DocId, <<"val">> => I})
    end, lists:seq(1, 20)),
    %% Find with ascending sort
    {ok, AscResults} = barrel_vdb:find(VdbName, #{}, #{sort => {<<"val">>, asc}}),
    AscVals = [maps:get(<<"val">>, D) || D <- AscResults],
    ?assertEqual(lists:sort(AscVals), AscVals),
    %% Find with descending sort
    {ok, DescResults} = barrel_vdb:find(VdbName, #{}, #{sort => {<<"val">>, desc}}),
    DescVals = [maps:get(<<"val">>, D) || D <- DescResults],
    ?assertEqual(lists:reverse(lists:sort(DescVals)), DescVals).

get_changes_basic(_Config) ->
    VdbName = <<"test_changes">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    %% Insert some docs
    lists:foreach(fun(I) ->
        DocId = list_to_binary("chg" ++ integer_to_list(I)),
        {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => DocId})
    end, lists:seq(1, 5)),
    %% Get changes
    {ok, Changes} = barrel_vdb:get_changes(VdbName, #{}),
    ?assert(is_map(Changes)),
    ?assert(maps:is_key(changes, Changes)),
    ?assert(maps:is_key(last_seq, Changes)),
    ChangesList = maps:get(changes, Changes),
    ?assertEqual(5, length(ChangesList)).

fold_docs(_Config) ->
    VdbName = <<"test_fold">>,
    ?assertEqual(ok, barrel_vdb:create(VdbName, #{shard_count => 2})),
    %% Insert docs
    lists:foreach(fun(I) ->
        DocId = list_to_binary("fold" ++ integer_to_list(I)),
        {ok, _} = barrel_vdb:put_doc(VdbName, #{<<"id">> => DocId, <<"n">> => I})
    end, lists:seq(1, 10)),
    %% Fold to count docs - callback must return {ok, Acc}
    {ok, Count} = barrel_vdb:fold_docs(VdbName, fun(_, Acc) -> {ok, Acc + 1} end, 0, #{}),
    ?assertEqual(10, Count).
