%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb changes feed
%%%
%%% Tests HLC-based changes API and streaming.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_changes_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("hlc/include/hlc.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases - barrel_sequence (legacy, kept for internal use)
-export([
    seq_new/1,
    seq_inc/1,
    seq_compare/1,
    seq_encode_decode/1,
    seq_to_from_string/1
]).

%% Test cases - barrel_changes
-export([
    changes_write_read/1,
    changes_fold/1,
    changes_get_list/1,
    changes_limit/1,
    changes_doc_ids_filter/1,
    changes_path_filter/1,
    changes_path_filter_wildcard/1,
    changes_path_and_docid_filter/1,
    changes_query_filter/1,
    changes_query_filter_compare/1,
    changes_query_and_path_filter/1,
    changes_style/1,
    changes_last_seq/1,
    changes_count_since/1,
    changes_has_changes_since/1
]).

%% Test cases - barrel_changes_stream
-export([
    stream_iterate_mode/1,
    stream_push_mode/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, sequence}, {group, changes}, {group, stream}].

groups() ->
    [
        {sequence, [sequence], [
            seq_new,
            seq_inc,
            seq_compare,
            seq_encode_decode,
            seq_to_from_string
        ]},
        {changes, [sequence], [
            changes_write_read,
            changes_fold,
            changes_get_list,
            changes_limit,
            changes_doc_ids_filter,
            changes_path_filter,
            changes_path_filter_wildcard,
            changes_path_and_docid_filter,
            changes_query_filter,
            changes_query_filter_compare,
            changes_query_and_path_filter,
            changes_style,
            changes_last_seq,
            changes_count_since,
            changes_has_changes_since
        ]},
        {stream, [sequence], [
            stream_iterate_mode,
            stream_push_mode
        ]}
    ].

init_per_suite(Config) ->
    %% Start the HLC clock for tests
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    TestDir = "/tmp/barrel_changes_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    [{test_dir, TestDir} | Config].

end_per_group(_Group, Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    os:cmd("rm -rf " ++ TestDir),
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - barrel_sequence (legacy internal module)
%%====================================================================

seq_new(_Config) ->
    ?assertEqual({0, 0}, barrel_sequence:new()),
    ?assertEqual({5, 0}, barrel_sequence:new(5)),
    ok.

seq_inc(_Config) ->
    Seq0 = barrel_sequence:new(),
    Seq1 = barrel_sequence:inc(Seq0),
    Seq2 = barrel_sequence:inc(Seq1),

    ?assertEqual({0, 0}, Seq0),
    ?assertEqual({0, 1}, Seq1),
    ?assertEqual({0, 2}, Seq2),
    ok.

seq_compare(_Config) ->
    %% Same sequence
    ?assertEqual(0, barrel_sequence:compare({0, 0}, {0, 0})),
    ?assertEqual(0, barrel_sequence:compare({1, 5}, {1, 5})),

    %% Different epochs
    ?assertEqual(-1, barrel_sequence:compare({0, 100}, {1, 0})),
    ?assertEqual(1, barrel_sequence:compare({2, 0}, {1, 100})),

    %% Same epoch, different counters
    ?assertEqual(-1, barrel_sequence:compare({1, 5}, {1, 10})),
    ?assertEqual(1, barrel_sequence:compare({1, 10}, {1, 5})),
    ok.

seq_encode_decode(_Config) ->
    Seqs = [{0, 0}, {0, 1}, {1, 0}, {1, 100}, {16#FFFFFFFF, 16#FFFFFFFF}],
    lists:foreach(
        fun(Seq) ->
            Encoded = barrel_sequence:encode(Seq),
            ?assert(is_binary(Encoded)),
            ?assertEqual(8, byte_size(Encoded)),
            Decoded = barrel_sequence:decode(Encoded),
            ?assertEqual(Seq, Decoded)
        end,
        Seqs
    ),
    ok.

seq_to_from_string(_Config) ->
    Seqs = [{0, 0}, {0, 1}, {1, 0}, {1, 100}, {999, 12345}],
    lists:foreach(
        fun(Seq) ->
            Str = barrel_sequence:to_string(Seq),
            ?assert(is_binary(Str)),
            Parsed = barrel_sequence:from_string(Str),
            ?assertEqual(Seq, Parsed)
        end,
        Seqs
    ),

    %% Test invalid strings
    ?assertEqual({error, invalid_sequence}, barrel_sequence:from_string(<<"invalid">>)),
    ?assertEqual({error, invalid_sequence}, barrel_sequence:from_string(<<"not-a-number">>)),
    ok.

%%====================================================================
%% Test Cases - barrel_changes (HLC-based)
%%====================================================================

%% Helper to create HLC timestamps for testing
make_test_hlc(WallTime, Logical) ->
    #timestamp{wall_time = WallTime, logical = Logical}.

changes_write_read(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_write_read",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write a change with HLC
    Hlc = make_test_hlc(1000, 1),
    DocInfo = #{
        id => <<"doc1">>,
        rev => <<"1-abc">>,
        deleted => false
    },
    ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo),

    %% Read it back via fold
    {ok, Changes, LastHlc} = barrel_changes:fold_changes(
        StoreRef, DbName, first,
        fun(Change, Acc) -> {ok, [Change | Acc]} end,
        []
    ),

    ?assertEqual(1, length(Changes)),
    [Change] = Changes,
    ?assertEqual(<<"doc1">>, maps:get(id, Change)),
    ?assertEqual(Hlc, maps:get(hlc, Change)),
    ?assertEqual(Hlc, LastHlc),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_fold(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_fold",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write multiple changes with HLC
    Changes = [
        {<<"doc1">>, make_test_hlc(1000, 1)},
        {<<"doc2">>, make_test_hlc(1000, 2)},
        {<<"doc3">>, make_test_hlc(1000, 3)}
    ],
    lists:foreach(
        fun({DocId, Hlc}) ->
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        Changes
    ),

    %% Fold from beginning
    {ok, AllChanges, _} = barrel_changes:fold_changes(
        StoreRef, DbName, first,
        fun(Change, Acc) -> {ok, [maps:get(id, Change) | Acc]} end,
        []
    ),
    ?assertEqual([<<"doc3">>, <<"doc2">>, <<"doc1">>], AllChanges),

    %% Fold from specific HLC (exclusive)
    {ok, PartialChanges, _} = barrel_changes:fold_changes(
        StoreRef, DbName, make_test_hlc(1000, 1),
        fun(Change, Acc) -> {ok, [maps:get(id, Change) | Acc]} end,
        []
    ),
    ?assertEqual([<<"doc3">>, <<"doc2">>], PartialChanges),

    %% Fold with stop
    {ok, StoppedChanges, _} = barrel_changes:fold_changes(
        StoreRef, DbName, first,
        fun(Change, Acc) ->
            case maps:get(id, Change) of
                <<"doc2">> -> {stop, [maps:get(id, Change) | Acc]};
                _ -> {ok, [maps:get(id, Change) | Acc]}
            end
        end,
        []
    ),
    ?assertEqual([<<"doc2">>, <<"doc1">>], StoppedChanges),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_get_list(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_get_list",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with HLC
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 5)
    ),

    %% Get all changes
    {ok, Changes, LastHlc} = barrel_changes:get_changes(StoreRef, DbName, first, #{}),
    ?assertEqual(5, length(Changes)),
    ?assertEqual(make_test_hlc(1000, 5), LastHlc),

    %% Verify order (ascending by default)
    Ids = [maps:get(id, C) || C <- Changes],
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>, <<"doc4">>, <<"doc5">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_limit(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_limit",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write 10 changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 10)
    ),

    %% Get with limit
    {ok, Changes, LastHlc} = barrel_changes:get_changes(StoreRef, DbName, first, #{limit => 3}),
    ?assertEqual(3, length(Changes)),
    ?assertEqual(make_test_hlc(1000, 3), LastHlc),

    Ids = [maps:get(id, C) || C <- Changes],
    ?assertEqual([<<"doc1">>, <<"doc2">>, <<"doc3">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_doc_ids_filter(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_doc_ids",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 5)
    ),

    %% Filter to specific doc_ids
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{doc_ids => [<<"doc2">>, <<"doc4">>]}
    ),

    ?assertEqual(2, length(Changes)),
    Ids = [maps:get(id, C) || C <- Changes],
    ?assertEqual([<<"doc2">>, <<"doc4">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_path_filter(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_path_filter",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with documents of different types
    DocInfos = [
        {<<"user1">>, 1, #{<<"type">> => <<"user">>, <<"name">> => <<"Alice">>}},
        {<<"order1">>, 2, #{<<"type">> => <<"order">>, <<"total">> => 100}},
        {<<"user2">>, 3, #{<<"type">> => <<"user">>, <<"name">> => <<"Bob">>}},
        {<<"product1">>, 4, #{<<"type">> => <<"product">>, <<"price">> => 50}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Filter to documents with type=user path
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{paths => [<<"type/#">>]}
    ),

    %% All documents have a type field, so all should match type/#
    ?assertEqual(4, length(Changes)),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_path_filter_wildcard(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_path_wildcard",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with nested document structures
    DocInfos = [
        {<<"u1">>, 1, #{<<"users">> => #{<<"123">> => #{<<"name">> => <<"Alice">>}}}},
        {<<"u2">>, 2, #{<<"users">> => #{<<"456">> => #{<<"name">> => <<"Bob">>}}}},
        {<<"o1">>, 3, #{<<"orders">> => #{<<"789">> => #{<<"total">> => 100}}}},
        {<<"p1">>, 4, #{<<"products">> => #{<<"abc">> => #{<<"price">> => 50}}}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Filter to documents with users path (using # wildcard)
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{paths => [<<"users/#">>]}
    ),

    %% Should match u1 and u2
    ?assertEqual(2, length(Changes)),
    Ids = lists:sort([maps:get(id, C) || C <- Changes]),
    ?assertEqual([<<"u1">>, <<"u2">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_path_and_docid_filter(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_path_docid",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes
    DocInfos = [
        {<<"user1">>, 1, #{<<"type">> => <<"user">>}},
        {<<"user2">>, 2, #{<<"type">> => <<"user">>}},
        {<<"order1">>, 3, #{<<"type">> => <<"order">>}},
        {<<"order2">>, 4, #{<<"type">> => <<"order">>}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Filter to user1 with type path - both filters must match (AND)
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{doc_ids => [<<"user1">>, <<"order1">>], paths => [<<"type/#">>]}
    ),

    %% Both filters: doc_ids AND paths must match
    ?assertEqual(2, length(Changes)),
    Ids = [maps:get(id, C) || C <- Changes],
    ?assertEqual([<<"user1">>, <<"order1">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_query_filter(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_query_filter",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with documents of different types
    DocInfos = [
        {<<"user1">>, 1, #{<<"type">> => <<"user">>, <<"name">> => <<"Alice">>}},
        {<<"order1">>, 2, #{<<"type">> => <<"order">>, <<"total">> => 100}},
        {<<"user2">>, 3, #{<<"type">> => <<"user">>, <<"name">> => <<"Bob">>}},
        {<<"product1">>, 4, #{<<"type">> => <<"product">>, <<"price">> => 50}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Query for type=user documents
    Query = #{where => [{path, [<<"type">>], <<"user">>}]},
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{query => Query}
    ),

    %% Should match user1 and user2
    ?assertEqual(2, length(Changes)),
    Ids = lists:sort([maps:get(id, C) || C <- Changes]),
    ?assertEqual([<<"user1">>, <<"user2">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_query_filter_compare(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_query_compare",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with numeric values
    DocInfos = [
        {<<"order1">>, 1, #{<<"type">> => <<"order">>, <<"total">> => 50}},
        {<<"order2">>, 2, #{<<"type">> => <<"order">>, <<"total">> => 100}},
        {<<"order3">>, 3, #{<<"type">> => <<"order">>, <<"total">> => 150}},
        {<<"order4">>, 4, #{<<"type">> => <<"order">>, <<"total">> => 200}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Query for orders with total > 100
    Query = #{where => [{compare, [<<"total">>], '>', 100}]},
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{query => Query}
    ),

    %% Should match order3 and order4
    ?assertEqual(2, length(Changes)),
    Ids = lists:sort([maps:get(id, C) || C <- Changes]),
    ?assertEqual([<<"order3">>, <<"order4">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_query_and_path_filter(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_query_path",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes with documents of different types
    DocInfos = [
        {<<"user1">>, 1, #{<<"type">> => <<"user">>, <<"status">> => <<"active">>}},
        {<<"user2">>, 2, #{<<"type">> => <<"user">>, <<"status">> => <<"inactive">>}},
        {<<"order1">>, 3, #{<<"type">> => <<"order">>, <<"status">> => <<"active">>}},
        {<<"order2">>, 4, #{<<"type">> => <<"order">>, <<"status">> => <<"pending">>}}
    ],
    lists:foreach(
        fun({DocId, N, Doc}) ->
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false, doc => Doc},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        DocInfos
    ),

    %% Query for active users - both path and query must match (AND logic)
    Query = #{where => [{path, [<<"status">>], <<"active">>}]},
    {ok, Changes, _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{
            paths => [<<"type/#">>],
            query => Query
        }
    ),

    %% All have type path, but only user1 and order1 have status=active
    ?assertEqual(2, length(Changes)),
    Ids = lists:sort([maps:get(id, C) || C <- Changes]),
    ?assertEqual([<<"order1">>, <<"user1">>], Ids),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_style(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_style",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write a change with multiple revisions (simulating conflicts)
    Hlc = make_test_hlc(1000, 1),
    DocInfo = #{
        id => <<"doc1">>,
        rev => <<"2-winner">>,
        deleted => false,
        revtree => #{} %% Empty revtree means no conflicts in current impl
    },
    ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo),

    %% With main_only style
    {ok, [Change], _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{style => main_only}
    ),
    ?assertEqual(1, length(maps:get(changes, Change))),

    %% With all_docs style (default)
    {ok, [Change2], _} = barrel_changes:get_changes(
        StoreRef, DbName, first,
        #{style => all_docs}
    ),
    ?assert(length(maps:get(changes, Change2)) >= 1),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_last_seq(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_last_seq",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Empty database - last_seq returns encoded min HLC
    LastSeq1 = barrel_changes:get_last_seq(StoreRef, DbName),
    ?assert(is_binary(LastSeq1)),
    ?assertEqual(12, byte_size(LastSeq1)),  %% HLC encoded is 12 bytes

    %% Decode it to verify it's min HLC
    LastHlc1 = barrel_changes:get_last_hlc(StoreRef, DbName),
    ?assertEqual(barrel_hlc:min(), LastHlc1),

    %% After adding changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 5)
    ),

    LastSeq2 = barrel_changes:get_last_seq(StoreRef, DbName),
    ?assert(is_binary(LastSeq2)),

    LastHlc2 = barrel_changes:get_last_hlc(StoreRef, DbName),
    ?assertEqual(make_test_hlc(1000, 5), LastHlc2),

    %% Verify encoding roundtrip
    ?assertEqual(LastSeq2, barrel_hlc:encode(LastHlc2)),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_count_since(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_count",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 10)
    ),

    %% Count from beginning (min HLC)
    ?assertEqual(10, barrel_changes:count_changes_since(StoreRef, DbName, barrel_hlc:min())),

    %% Count from middle
    ?assertEqual(5, barrel_changes:count_changes_since(StoreRef, DbName, make_test_hlc(1000, 5))),

    %% Count from end
    ?assertEqual(0, barrel_changes:count_changes_since(StoreRef, DbName, make_test_hlc(1000, 10))),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_has_changes_since(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_has_since",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Before any writes, has_changes_since should return true (safe fallback)
    ?assertEqual(true, barrel_changes:has_changes_since(StoreRef, DbName, barrel_hlc:min())),

    %% Write changes with HLC - this also updates the bucket hints
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(2000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 5)
    ),

    %% After writes, has_changes_since with old HLC should return true
    %% (bucket hint shows max_hlc > since)
    OldHlc = make_test_hlc(1000, 1),  %% Before all changes
    ?assertEqual(true, barrel_changes:has_changes_since(StoreRef, DbName, OldHlc)),

    %% has_changes_since with min HLC should return true
    ?assertEqual(true, barrel_changes:has_changes_since(StoreRef, DbName, barrel_hlc:min())),

    %% has_changes_since with the last written HLC should return true
    %% (safe fallback when no changes after)
    LastHlc = make_test_hlc(2000, 5),
    ?assertEqual(true, barrel_changes:has_changes_since(StoreRef, DbName, LastHlc)),

    %% Verify the bucket was written by checking the key directly
    NowSecs = erlang:system_time(second),
    BucketTs = NowSecs div 60,  %% 60 second granularity
    BucketKey = barrel_store_keys:change_bucket(DbName, BucketTs),
    {ok, BucketValue} = barrel_store_rocksdb:get(StoreRef, BucketKey),

    %% Bucket format: <<MinHlc:12/binary, MaxHlc:12/binary, Count:32>>
    ?assertEqual(28, byte_size(BucketValue)),
    <<_MinBin:12/binary, MaxBin:12/binary, Count:32>> = BucketValue,
    ?assertEqual(5, Count),  %% 5 changes written

    %% Verify max HLC in bucket matches last written change
    MaxHlc = barrel_hlc:decode(MaxBin),
    ?assertEqual(make_test_hlc(2000, 5), MaxHlc),

    barrel_store_rocksdb:close(StoreRef),
    ok.

%%====================================================================
%% Test Cases - barrel_changes_stream
%%====================================================================

stream_iterate_mode(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/stream_iterate",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write some changes with HLC
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 3)
    ),

    %% Start iterate stream
    {ok, Stream} = barrel_changes_stream:start_link(
        StoreRef, DbName,
        #{mode => iterate, since => first}
    ),

    %% Read changes one by one
    {ok, C1} = barrel_changes_stream:next(Stream),
    ?assertEqual(<<"doc1">>, maps:get(id, C1)),

    {ok, C2} = barrel_changes_stream:next(Stream),
    ?assertEqual(<<"doc2">>, maps:get(id, C2)),

    {ok, C3} = barrel_changes_stream:next(Stream),
    ?assertEqual(<<"doc3">>, maps:get(id, C3)),

    %% No more changes
    done = barrel_changes_stream:next(Stream),

    barrel_store_rocksdb:close(StoreRef),
    ok.

stream_push_mode(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/stream_push",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write some changes with HLC
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Hlc = make_test_hlc(1000, N),
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Hlc, DocInfo)
        end,
        lists:seq(1, 3)
    ),

    %% Start push stream
    {ok, Stream} = barrel_changes_stream:start_link(
        StoreRef, DbName,
        #{mode => push, since => first, owner => self(), batch_size => 10}
    ),

    %% Wait for changes to be pushed
    {ReqId, Changes} = barrel_changes_stream:await(Stream, 1000),
    ?assert(is_reference(ReqId)),
    ?assertEqual(3, length(Changes)),

    %% Acknowledge
    ok = barrel_changes_stream:ack(Stream, ReqId),

    %% Stop
    ok = barrel_changes_stream:stop(Stream),

    barrel_store_rocksdb:close(StoreRef),
    ok.
