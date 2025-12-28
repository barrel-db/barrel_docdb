%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb changes feed
%%%
%%% Tests sequence handling, changes API, and streaming.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_changes_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases - barrel_sequence
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
    changes_style/1,
    changes_last_seq/1,
    changes_count_since/1
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
            changes_style,
            changes_last_seq,
            changes_count_since
        ]},
        {stream, [sequence], [
            stream_iterate_mode,
            stream_push_mode
        ]}
    ].

init_per_suite(Config) ->
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
%% Test Cases - barrel_sequence
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
%% Test Cases - barrel_changes
%%====================================================================

changes_write_read(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_write_read",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write a change
    Seq = {0, 1},
    DocInfo = #{
        id => <<"doc1">>,
        rev => <<"1-abc">>,
        deleted => false
    },
    ok = barrel_changes:write_change(StoreRef, DbName, Seq, DocInfo),

    %% Read it back via fold
    {ok, Changes, LastSeq} = barrel_changes:fold_changes(
        StoreRef, DbName, first,
        fun(Change, Acc) -> {ok, [Change | Acc]} end,
        []
    ),

    ?assertEqual(1, length(Changes)),
    [Change] = Changes,
    ?assertEqual(<<"doc1">>, maps:get(id, Change)),
    ?assertEqual(Seq, maps:get(seq, Change)),
    ?assertEqual(Seq, LastSeq),

    barrel_store_rocksdb:close(StoreRef),
    ok.

changes_fold(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_fold",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write multiple changes
    Changes = [
        {<<"doc1">>, {0, 1}},
        {<<"doc2">>, {0, 2}},
        {<<"doc3">>, {0, 3}}
    ],
    lists:foreach(
        fun({DocId, Seq}) ->
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Seq, DocInfo)
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

    %% Fold from specific sequence
    {ok, PartialChanges, _} = barrel_changes:fold_changes(
        StoreRef, DbName, {0, 1},
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

    %% Write changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Seq = {0, N},
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Seq, DocInfo)
        end,
        lists:seq(1, 5)
    ),

    %% Get all changes
    {ok, Changes, LastSeq} = barrel_changes:get_changes(StoreRef, DbName, first, #{}),
    ?assertEqual(5, length(Changes)),
    ?assertEqual({0, 5}, LastSeq),

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
            Seq = {0, N},
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Seq, DocInfo)
        end,
        lists:seq(1, 10)
    ),

    %% Get with limit
    {ok, Changes, LastSeq} = barrel_changes:get_changes(StoreRef, DbName, first, #{limit => 3}),
    ?assertEqual(3, length(Changes)),
    ?assertEqual({0, 3}, LastSeq),

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
            Seq = {0, N},
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Seq, DocInfo)
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

changes_style(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/changes_style",

    {ok, StoreRef} = barrel_store_rocksdb:open(DbPath, #{}),
    DbName = <<"testdb">>,

    %% Write a change with multiple revisions (simulating conflicts)
    Seq = {0, 1},
    DocInfo = #{
        id => <<"doc1">>,
        rev => <<"2-winner">>,
        deleted => false,
        revtree => #{} %% Empty revtree means no conflicts in current impl
    },
    ok = barrel_changes:write_change(StoreRef, DbName, Seq, DocInfo),

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

    %% Empty database
    ?assertEqual({0, 0}, barrel_changes:get_last_seq(StoreRef, DbName)),

    %% After adding changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Seq = {0, N},
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Seq, DocInfo)
        end,
        lists:seq(1, 5)
    ),

    ?assertEqual({0, 5}, barrel_changes:get_last_seq(StoreRef, DbName)),

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
            Seq = {0, N},
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Seq, DocInfo)
        end,
        lists:seq(1, 10)
    ),

    %% Count from beginning
    ?assertEqual(10, barrel_changes:count_changes_since(StoreRef, DbName, {0, 0})),

    %% Count from middle
    ?assertEqual(5, barrel_changes:count_changes_since(StoreRef, DbName, {0, 5})),

    %% Count from end
    ?assertEqual(0, barrel_changes:count_changes_since(StoreRef, DbName, {0, 10})),

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

    %% Write some changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Seq = {0, N},
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Seq, DocInfo)
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

    %% Write some changes
    lists:foreach(
        fun(N) ->
            DocId = iolist_to_binary(["doc", integer_to_list(N)]),
            Seq = {0, N},
            DocInfo = #{id => DocId, rev => <<"1-abc">>, deleted => false},
            ok = barrel_changes:write_change(StoreRef, DbName, Seq, DocInfo)
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
