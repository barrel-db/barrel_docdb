%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_posting module
%%%
%%% Tests posting list storage including encoding/decoding,
%%% add/remove operations, and set operations.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_posting_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([init_per_group/2, end_per_group/2]).
-export([init_per_testcase/2, end_per_testcase/2]).

%% Test cases - encoding
-export([
    encode_empty/1,
    encode_single/1,
    encode_multiple/1,
    encode_decode_roundtrip/1
]).

%% Test cases - key generation
-export([
    posting_key_format/1,
    posting_key_different_values/1,
    posting_key_different_paths/1
]).

%% Test cases - add/remove operations
-export([
    add_single_doc/1,
    add_multiple_docs/1,
    add_duplicate_doc/1,
    remove_doc/1,
    remove_nonexistent_doc/1,
    add_remove_sequence/1
]).

%% Test cases - get operations
-export([
    get_empty/1,
    get_existing/1,
    get_multi/1
]).

%% Test cases - set operations
-export([
    intersect_empty/1,
    intersect_disjoint/1,
    intersect_overlap/1,
    union_empty/1,
    union_disjoint/1,
    union_overlap/1,
    member_true/1,
    member_false/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, encoding}, {group, keys}, {group, add_remove},
     {group, get_ops}, {group, set_ops}].

groups() ->
    [
        {encoding, [sequence], [
            encode_empty,
            encode_single,
            encode_multiple,
            encode_decode_roundtrip
        ]},
        {keys, [sequence], [
            posting_key_format,
            posting_key_different_values,
            posting_key_different_paths
        ]},
        {add_remove, [sequence], [
            add_single_doc,
            add_multiple_docs,
            add_duplicate_doc,
            remove_doc,
            remove_nonexistent_doc,
            add_remove_sequence
        ]},
        {get_ops, [sequence], [
            get_empty,
            get_existing,
            get_multi
        ]},
        {set_ops, [sequence], [
            intersect_empty,
            intersect_disjoint,
            intersect_overlap,
            union_empty,
            union_disjoint,
            union_overlap,
            member_true,
            member_false
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(set_ops, Config) ->
    %% Set operations don't need database
    Config;
init_per_group(encoding, Config) ->
    %% Encoding tests don't need database
    Config;
init_per_group(keys, Config) ->
    %% Key tests don't need database
    Config;
init_per_group(_Group, Config) ->
    %% Start full application
    application:stop(barrel_docdb),
    timer:sleep(50),
    {ok, Apps} = application:ensure_all_started(barrel_docdb),
    %% Create a test database
    DbName = <<"posting_test_db">>,
    barrel_docdb:delete_db(DbName),
    {ok, Db} = barrel_docdb:create_db(DbName, #{}),
    {ok, StoreRef} = barrel_db_server:get_store_ref(Db),
    [{started_apps, Apps}, {db_name, DbName}, {db, Db}, {store_ref, StoreRef} | Config].

end_per_group(set_ops, _Config) ->
    ok;
end_per_group(encoding, _Config) ->
    ok;
end_per_group(keys, _Config) ->
    ok;
end_per_group(_Group, Config) ->
    %% Cleanup test database
    DbName = ?config(db_name, Config),
    barrel_docdb:delete_db(DbName),
    application:stop(barrel_docdb),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Encoding
%%====================================================================

encode_empty(_Config) ->
    %% Empty list should encode to empty binary
    ?assertEqual(<<>>, barrel_posting:encode_posting([])),
    ?assertEqual([], barrel_posting:decode_posting(<<>>)),
    ok.

encode_single(_Config) ->
    %% Single DocId
    DocId = <<"doc1">>,
    Encoded = barrel_posting:encode_posting([DocId]),
    Decoded = barrel_posting:decode_posting(Encoded),
    ?assertEqual([DocId], Decoded),
    ok.

encode_multiple(_Config) ->
    %% Multiple DocIds should be sorted
    DocIds = [<<"doc3">>, <<"doc1">>, <<"doc2">>],
    Encoded = barrel_posting:encode_posting(DocIds),
    Decoded = barrel_posting:decode_posting(Encoded),
    %% Result should be sorted
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>], Decoded),
    ok.

encode_decode_roundtrip(_Config) ->
    %% Various DocId patterns
    TestCases = [
        [],
        [<<"a">>],
        [<<"doc1">>, <<"doc2">>, <<"doc3">>],
        [<<"user:123">>, <<"user:456">>, <<"user:789">>],
        [<<"very_long_document_id_that_exceeds_normal_length">>],
        %% Already sorted
        [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>]
    ],
    lists:foreach(fun(DocIds) ->
        Sorted = lists:usort(DocIds),
        Encoded = barrel_posting:encode_posting(DocIds),
        Decoded = barrel_posting:decode_posting(Encoded),
        ?assertEqual(Sorted, Decoded)
    end, TestCases),
    ok.

%%====================================================================
%% Test Cases - Key Generation
%%====================================================================

posting_key_format(_Config) ->
    DbName = <<"testdb">>,
    PathId = 123,
    Value = <<"user">>,
    Key = barrel_posting:posting_key(DbName, PathId, Value),
    %% Key should be a binary
    ?assert(is_binary(Key)),
    %% Key should start with posting prefix (0x13)
    ?assertEqual(16#13, binary:first(Key)),
    ok.

posting_key_different_values(_Config) ->
    DbName = <<"testdb">>,
    PathId = 123,
    Key1 = barrel_posting:posting_key(DbName, PathId, <<"user">>),
    Key2 = barrel_posting:posting_key(DbName, PathId, <<"admin">>),
    %% Different values should produce different keys
    ?assertNotEqual(Key1, Key2),
    ok.

posting_key_different_paths(_Config) ->
    DbName = <<"testdb">>,
    Value = <<"active">>,
    Key1 = barrel_posting:posting_key(DbName, 1, Value),
    Key2 = barrel_posting:posting_key(DbName, 2, Value),
    %% Different path IDs should produce different keys
    ?assertNotEqual(Key1, Key2),
    ok.

%%====================================================================
%% Test Cases - Add/Remove Operations
%%====================================================================

add_single_doc(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),
    PathId = 1,
    Value = <<"test_value">>,
    DocId = <<"add_single_doc1">>,

    %% Add a single document
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, DocId),

    %% Verify it's in the posting list
    Posting = barrel_posting:get(StoreRef, DbName, PathId, Value),
    ?assertEqual([DocId], Posting),
    ok.

add_multiple_docs(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),
    PathId = 2,
    Value = <<"multi_value">>,

    %% Add multiple documents
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"doc_a">>),
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"doc_c">>),
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"doc_b">>),

    %% Verify all are in the posting list (sorted)
    Posting = barrel_posting:get(StoreRef, DbName, PathId, Value),
    ?assertEqual([<<"doc_a">>, <<"doc_b">>, <<"doc_c">>], Posting),
    ok.

add_duplicate_doc(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),
    PathId = 3,
    Value = <<"dup_value">>,
    DocId = <<"dup_doc">>,

    %% Add same document twice
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, DocId),
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, DocId),

    %% Should only appear once
    Posting = barrel_posting:get(StoreRef, DbName, PathId, Value),
    ?assertEqual([DocId], Posting),
    ok.

remove_doc(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),
    PathId = 4,
    Value = <<"remove_value">>,

    %% Add documents
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"doc1">>),
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"doc2">>),
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"doc3">>),

    %% Remove middle document
    ok = barrel_posting:remove(StoreRef, DbName, PathId, Value, <<"doc2">>),

    %% Verify removal
    Posting = barrel_posting:get(StoreRef, DbName, PathId, Value),
    ?assertEqual([<<"doc1">>, <<"doc3">>], Posting),
    ok.

remove_nonexistent_doc(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),
    PathId = 5,
    Value = <<"remove_nonexist">>,

    %% Add a document
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"exists">>),

    %% Remove nonexistent document (should not error)
    ok = barrel_posting:remove(StoreRef, DbName, PathId, Value, <<"not_exists">>),

    %% Original document should still be there
    Posting = barrel_posting:get(StoreRef, DbName, PathId, Value),
    ?assertEqual([<<"exists">>], Posting),
    ok.

add_remove_sequence(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),
    PathId = 6,
    Value = <<"sequence_value">>,

    %% Complex sequence of add/remove
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"a">>),
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"b">>),
    ok = barrel_posting:remove(StoreRef, DbName, PathId, Value, <<"a">>),
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"c">>),
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"a">>),
    ok = barrel_posting:remove(StoreRef, DbName, PathId, Value, <<"b">>),

    %% Final state should be [a, c]
    Posting = barrel_posting:get(StoreRef, DbName, PathId, Value),
    ?assertEqual([<<"a">>, <<"c">>], Posting),
    ok.

%%====================================================================
%% Test Cases - Get Operations
%%====================================================================

get_empty(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Get nonexistent posting list should return empty
    Posting = barrel_posting:get(StoreRef, DbName, 999, <<"nonexistent">>),
    ?assertEqual([], Posting),
    ok.

get_existing(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),
    PathId = 10,
    Value = <<"get_exist">>,

    %% Add some documents
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"x">>),
    ok = barrel_posting:add(StoreRef, DbName, PathId, Value, <<"y">>),

    %% Get should return them
    Posting = barrel_posting:get(StoreRef, DbName, PathId, Value),
    ?assertEqual([<<"x">>, <<"y">>], Posting),
    ok.

get_multi(Config) ->
    StoreRef = ?config(store_ref, Config),
    DbName = ?config(db_name, Config),

    %% Create several posting lists
    ok = barrel_posting:add(StoreRef, DbName, 20, <<"val1">>, <<"d1">>),
    ok = barrel_posting:add(StoreRef, DbName, 21, <<"val2">>, <<"d2">>),
    ok = barrel_posting:add(StoreRef, DbName, 21, <<"val2">>, <<"d3">>),

    %% Get multiple posting lists at once
    Results = barrel_posting:get_multi(StoreRef, DbName, [
        {20, <<"val1">>},
        {21, <<"val2">>},
        {22, <<"nonexistent">>}
    ]),

    ?assertEqual([
        [<<"d1">>],
        [<<"d2">>, <<"d3">>],
        []
    ], Results),
    ok.

%%====================================================================
%% Test Cases - Set Operations
%%====================================================================

intersect_empty(_Config) ->
    ?assertEqual([], barrel_posting:intersect([], [])),
    ?assertEqual([], barrel_posting:intersect([<<"a">>], [])),
    ?assertEqual([], barrel_posting:intersect([], [<<"a">>])),
    ok.

intersect_disjoint(_Config) ->
    L1 = [<<"a">>, <<"b">>, <<"c">>],
    L2 = [<<"x">>, <<"y">>, <<"z">>],
    ?assertEqual([], barrel_posting:intersect(L1, L2)),
    ok.

intersect_overlap(_Config) ->
    L1 = [<<"a">>, <<"b">>, <<"c">>, <<"d">>],
    L2 = [<<"b">>, <<"d">>, <<"e">>, <<"f">>],
    ?assertEqual([<<"b">>, <<"d">>], barrel_posting:intersect(L1, L2)),
    ok.

union_empty(_Config) ->
    ?assertEqual([], barrel_posting:union([], [])),
    ?assertEqual([<<"a">>], barrel_posting:union([<<"a">>], [])),
    ?assertEqual([<<"a">>], barrel_posting:union([], [<<"a">>])),
    ok.

union_disjoint(_Config) ->
    L1 = [<<"a">>, <<"c">>],
    L2 = [<<"b">>, <<"d">>],
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>, <<"d">>], barrel_posting:union(L1, L2)),
    ok.

union_overlap(_Config) ->
    L1 = [<<"a">>, <<"b">>, <<"c">>],
    L2 = [<<"b">>, <<"c">>, <<"d">>],
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>, <<"d">>], barrel_posting:union(L1, L2)),
    ok.

member_true(_Config) ->
    List = [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>],
    ?assert(barrel_posting:member(<<"a">>, List)),
    ?assert(barrel_posting:member(<<"c">>, List)),
    ?assert(barrel_posting:member(<<"e">>, List)),
    ok.

member_false(_Config) ->
    List = [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>],
    ?assertNot(barrel_posting:member(<<"x">>, List)),
    ?assertNot(barrel_posting:member(<<"z">>, List)),
    ?assertNot(barrel_posting:member(<<"aa">>, List)),
    ok.
