%%%-------------------------------------------------------------------
%%% @doc Common Test suite for barrel_ars_index
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ars_index_SUITE).

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

%% Test cases
-export([
    index_single_doc/1,
    index_nested_doc/1,
    index_doc_with_arrays/1,
    update_doc_add_fields/1,
    update_doc_remove_fields/1,
    update_doc_modify_fields/1,
    remove_doc/1,
    remove_nonexistent_doc/1,
    fold_exact_path/1,
    fold_path_prefix/1,
    fold_empty_result/1,
    multiple_docs_same_path/1,
    cardinality_basic/1,
    cardinality_update/1,
    cardinality_remove/1,
    bitmap_basic/1,
    bitmap_size_by_depth/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, index}, {group, update}, {group, remove}, {group, fold}, {group, cardinality}, {group, bitmap}].

groups() ->
    [
        {index, [sequence], [
            index_single_doc,
            index_nested_doc,
            index_doc_with_arrays
        ]},
        {update, [sequence], [
            update_doc_add_fields,
            update_doc_remove_fields,
            update_doc_modify_fields
        ]},
        {remove, [sequence], [
            remove_doc,
            remove_nonexistent_doc
        ]},
        {fold, [sequence], [
            fold_exact_path,
            fold_path_prefix,
            fold_empty_result,
            multiple_docs_same_path
        ]},
        {cardinality, [sequence], [
            cardinality_basic,
            cardinality_update,
            cardinality_remove
        ]},
        {bitmap, [sequence], [
            bitmap_basic,
            bitmap_size_by_depth
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    %% Start application for barrel_path_dict and other dependencies
    {ok, Apps} = application:ensure_all_started(barrel_docdb),
    %% Reset path dict for clean state
    barrel_path_dict:reset(),
    %% Create a temporary directory for RocksDB
    TestDir = "/tmp/barrel_ars_index_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    DbPath = TestDir ++ "/db",
    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    [{started_apps, Apps}, {test_dir, TestDir}, {store_ref, StoreRef}, {db_name, <<"testdb">>} | Config].

end_per_group(_Group, Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    ok = barrel_store_rocksdb:close(StoreRef),
    TestDir = proplists:get_value(test_dir, Config),
    os:cmd("rm -rf " ++ TestDir),
    application:stop(barrel_docdb),
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Indexing
%%====================================================================

index_single_doc(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    Doc = #{
        <<"type">> => <<"user">>,
        <<"name">> => <<"Alice">>
    },
    DocId = <<"doc1">>,

    ok = barrel_ars_index:index_doc(StoreRef, DbName, DocId, Doc),

    %% Verify paths are indexed
    Results = fold_all_paths(StoreRef, DbName, [<<"type">>]),
    ?assertEqual(1, length(Results)),
    [{Path, RetDocId}] = Results,
    ?assertEqual([<<"type">>, <<"user">>], Path),
    ?assertEqual(DocId, RetDocId),

    ok.

index_nested_doc(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    Doc = #{
        <<"profile">> => #{
            <<"name">> => <<"Bob">>,
            <<"address">> => #{
                <<"city">> => <<"Paris">>
            }
        }
    },
    DocId = <<"doc_nested">>,

    ok = barrel_ars_index:index_doc(StoreRef, DbName, DocId, Doc),

    %% Check nested path
    Results = fold_all_paths(StoreRef, DbName, [<<"profile">>, <<"address">>]),
    ?assertEqual(1, length(Results)),
    [{Path, _}] = Results,
    ?assertEqual([<<"profile">>, <<"address">>, <<"city">>, <<"Paris">>], Path),

    ok.

index_doc_with_arrays(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    Doc = #{
        <<"tags">> => [<<"a">>, <<"b">>, <<"c">>]
    },
    DocId = <<"doc_arrays">>,

    ok = barrel_ars_index:index_doc(StoreRef, DbName, DocId, Doc),

    %% Check array paths
    Results = fold_all_paths(StoreRef, DbName, [<<"tags">>]),
    ?assertEqual(3, length(Results)),

    %% Verify paths include array indices
    Paths = [P || {P, _} <- Results],
    ?assert(lists:member([<<"tags">>, 0, <<"a">>], Paths)),
    ?assert(lists:member([<<"tags">>, 1, <<"b">>], Paths)),
    ?assert(lists:member([<<"tags">>, 2, <<"c">>], Paths)),

    ok.

%%====================================================================
%% Test Cases - Update
%%====================================================================

update_doc_add_fields(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    OldDoc = #{<<"name">> => <<"Alice">>},
    NewDoc = #{<<"name">> => <<"Alice">>, <<"email">> => <<"alice@example.com">>},
    DocId = <<"doc_update_add">>,

    %% Index original doc
    ok = barrel_ars_index:index_doc(StoreRef, DbName, DocId, OldDoc),

    %% Update with new field
    ok = barrel_ars_index:update_doc(StoreRef, DbName, DocId, OldDoc, NewDoc),

    %% Verify new field is indexed
    Results = fold_all_paths(StoreRef, DbName, [<<"email">>]),
    ?assertEqual(1, length(Results)),

    %% Verify old field still exists
    NameResults = fold_all_paths(StoreRef, DbName, [<<"name">>]),
    ?assertEqual(1, length(NameResults)),

    ok.

update_doc_remove_fields(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    OldDoc = #{<<"name">> => <<"Bob">>, <<"age">> => 30},
    NewDoc = #{<<"name">> => <<"Bob">>},
    DocId = <<"doc_update_remove">>,

    %% Index original doc
    ok = barrel_ars_index:index_doc(StoreRef, DbName, DocId, OldDoc),

    %% Verify age is indexed
    AgeResults1 = fold_all_paths(StoreRef, DbName, [<<"age">>]),
    AgePaths1 = [D || {_, D} <- AgeResults1, D =:= DocId],
    ?assertEqual(1, length(AgePaths1)),

    %% Update with removed field
    ok = barrel_ars_index:update_doc(StoreRef, DbName, DocId, OldDoc, NewDoc),

    %% Verify age is no longer indexed for this doc
    AgeResults2 = fold_all_paths(StoreRef, DbName, [<<"age">>]),
    AgePaths2 = [D || {_, D} <- AgeResults2, D =:= DocId],
    ?assertEqual(0, length(AgePaths2)),

    ok.

update_doc_modify_fields(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    OldDoc = #{<<"status">> => <<"active">>},
    NewDoc = #{<<"status">> => <<"inactive">>},
    DocId = <<"doc_update_modify">>,

    %% Index original doc
    ok = barrel_ars_index:index_doc(StoreRef, DbName, DocId, OldDoc),

    %% Update with modified value
    ok = barrel_ars_index:update_doc(StoreRef, DbName, DocId, OldDoc, NewDoc),

    %% Verify old value is gone
    ActiveResults = fold_all_paths(StoreRef, DbName, [<<"status">>, <<"active">>]),
    ActiveDocs = [D || {_, D} <- ActiveResults, D =:= DocId],
    ?assertEqual(0, length(ActiveDocs)),

    %% Verify new value is indexed
    InactiveResults = fold_all_paths(StoreRef, DbName, [<<"status">>, <<"inactive">>]),
    InactiveDocs = [D || {_, D} <- InactiveResults, D =:= DocId],
    ?assertEqual(1, length(InactiveDocs)),

    ok.

%%====================================================================
%% Test Cases - Remove
%%====================================================================

remove_doc(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    Doc = #{
        <<"type">> => <<"temp">>,
        <<"data">> => <<"value">>
    },
    DocId = <<"doc_to_remove">>,

    %% Index doc
    ok = barrel_ars_index:index_doc(StoreRef, DbName, DocId, Doc),

    %% Verify indexed
    Results1 = fold_all_paths(StoreRef, DbName, [<<"type">>, <<"temp">>]),
    Docs1 = [D || {_, D} <- Results1, D =:= DocId],
    ?assertEqual(1, length(Docs1)),

    %% Remove doc
    ok = barrel_ars_index:remove_doc(StoreRef, DbName, DocId),

    %% Verify removed
    Results2 = fold_all_paths(StoreRef, DbName, [<<"type">>, <<"temp">>]),
    Docs2 = [D || {_, D} <- Results2, D =:= DocId],
    ?assertEqual(0, length(Docs2)),

    ok.

remove_nonexistent_doc(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    %% Should not error on nonexistent doc
    ok = barrel_ars_index:remove_doc(StoreRef, DbName, <<"nonexistent_doc">>),

    ok.

%%====================================================================
%% Test Cases - Fold/Query
%%====================================================================

fold_exact_path(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    Doc = #{<<"exact_field">> => <<"exact_value">>},
    DocId = <<"doc_exact">>,

    ok = barrel_ars_index:index_doc(StoreRef, DbName, DocId, Doc),

    %% Query exact path
    Results = fold_all_paths(StoreRef, DbName, [<<"exact_field">>, <<"exact_value">>]),
    ?assertEqual(1, length(Results)),
    [{Path, RetDocId}] = Results,
    ?assertEqual([<<"exact_field">>, <<"exact_value">>], Path),
    ?assertEqual(DocId, RetDocId),

    ok.

fold_path_prefix(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    Doc = #{
        <<"prefix_test">> => #{
            <<"a">> => 1,
            <<"b">> => 2,
            <<"c">> => 3
        }
    },
    DocId = <<"doc_prefix">>,

    ok = barrel_ars_index:index_doc(StoreRef, DbName, DocId, Doc),

    %% Query by prefix should get all nested paths
    Results = fold_all_paths(StoreRef, DbName, [<<"prefix_test">>]),
    ?assertEqual(3, length(Results)),

    ok.

fold_empty_result(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    %% Query non-existent path
    Results = fold_all_paths(StoreRef, DbName, [<<"nonexistent_path">>]),
    ?assertEqual(0, length(Results)),

    ok.

multiple_docs_same_path(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    %% Index multiple docs with same path value
    Doc1 = #{<<"category">> => <<"electronics">>},
    Doc2 = #{<<"category">> => <<"electronics">>},
    Doc3 = #{<<"category">> => <<"clothing">>},

    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"prod1">>, Doc1),
    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"prod2">>, Doc2),
    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"prod3">>, Doc3),

    %% Query for electronics
    ElecResults = fold_all_paths(StoreRef, DbName, [<<"category">>, <<"electronics">>]),
    ElecDocs = [D || {_, D} <- ElecResults],
    ?assertEqual(2, length(ElecDocs)),
    ?assert(lists:member(<<"prod1">>, ElecDocs)),
    ?assert(lists:member(<<"prod2">>, ElecDocs)),

    %% Query for clothing
    ClothResults = fold_all_paths(StoreRef, DbName, [<<"category">>, <<"clothing">>]),
    ClothDocs = [D || {_, D} <- ClothResults],
    ?assertEqual(1, length(ClothDocs)),
    ?assert(lists:member(<<"prod3">>, ClothDocs)),

    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

fold_all_paths(StoreRef, DbName, PathPrefix) ->
    barrel_ars_index:fold_path(
        StoreRef, DbName, PathPrefix,
        fun({Path, DocId}, Acc) -> {ok, [{Path, DocId} | Acc]} end,
        []
    ).

%%====================================================================
%% Test Cases - Cardinality
%%====================================================================

cardinality_basic(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    %% Initially no cardinality
    {ok, 0} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"type">>, <<"card_test">>]),

    %% Index first doc
    Doc1 = #{<<"type">> => <<"card_test">>},
    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"card1">>, Doc1),
    {ok, 1} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"type">>, <<"card_test">>]),

    %% Index second doc with same path
    Doc2 = #{<<"type">> => <<"card_test">>},
    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"card2">>, Doc2),
    {ok, 2} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"type">>, <<"card_test">>]),

    %% Index doc with different value
    Doc3 = #{<<"type">> => <<"other">>},
    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"card3">>, Doc3),
    {ok, 2} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"type">>, <<"card_test">>]),
    {ok, 1} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"type">>, <<"other">>]),

    ok.

cardinality_update(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    %% Index doc
    OldDoc = #{<<"status">> => <<"pending">>},
    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"upd1">>, OldDoc),
    {ok, 1} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"status">>, <<"pending">>]),

    %% Update doc to new status
    NewDoc = #{<<"status">> => <<"complete">>},
    ok = barrel_ars_index:update_doc(StoreRef, DbName, <<"upd1">>, OldDoc, NewDoc),

    %% Old path decremented, new path incremented
    {ok, 0} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"status">>, <<"pending">>]),
    {ok, 1} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"status">>, <<"complete">>]),

    ok.

cardinality_remove(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    %% Index docs
    Doc1 = #{<<"cat">> => <<"rm_test">>},
    Doc2 = #{<<"cat">> => <<"rm_test">>},
    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"rm1">>, Doc1),
    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"rm2">>, Doc2),
    {ok, 2} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"cat">>, <<"rm_test">>]),

    %% Remove first doc
    ok = barrel_ars_index:remove_doc(StoreRef, DbName, <<"rm1">>),
    {ok, 1} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"cat">>, <<"rm_test">>]),

    %% Remove second doc
    ok = barrel_ars_index:remove_doc(StoreRef, DbName, <<"rm2">>),
    {ok, 0} = barrel_ars_index:get_path_cardinality(StoreRef, DbName, [<<"cat">>, <<"rm_test">>]),

    ok.

%%====================================================================
%% Test Cases - Bitmap
%%====================================================================

bitmap_basic(Config) ->
    StoreRef = proplists:get_value(store_ref, Config),
    DbName = proplists:get_value(db_name, Config),

    %% Initially no bitmap
    not_found = barrel_ars_index:get_path_bitmap(StoreRef, DbName, [<<"type">>, <<"bitmap_test">>]),

    %% Index a doc - bitmap should be created
    Doc1 = #{<<"type">> => <<"bitmap_test">>},
    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"bm1">>, Doc1),

    %% Bitmap should now exist
    {ok, Bitmap1} = barrel_ars_index:get_path_bitmap(StoreRef, DbName, [<<"type">>, <<"bitmap_test">>]),
    ?assert(is_binary(Bitmap1)),

    %% Index another doc with same path
    Doc2 = #{<<"type">> => <<"bitmap_test">>},
    ok = barrel_ars_index:index_doc(StoreRef, DbName, <<"bm2">>, Doc2),

    %% Bitmap should still exist (and be updated)
    {ok, Bitmap2} = barrel_ars_index:get_path_bitmap(StoreRef, DbName, [<<"type">>, <<"bitmap_test">>]),
    ?assert(is_binary(Bitmap2)),

    ok.

bitmap_size_by_depth(_Config) ->
    %% Test that bitmap size is global (same for all paths)
    %% This enables cross-path bitmap intersection
    Size = barrel_ars_index:bitmap_size(),
    ?assertEqual(1048576, Size),  %% 1M bits global

    ok.
