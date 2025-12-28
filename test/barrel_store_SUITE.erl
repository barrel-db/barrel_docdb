%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb storage layer
%%%
%%% Tests the storage behaviour, key encoding, and RocksDB backend.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_store_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([
    %% Key encoding tests
    key_db_meta/1,
    key_doc_info/1,
    key_doc_seq/1,
    key_encoding_order/1,

    %% RocksDB backend tests
    rocksdb_open_close/1,
    rocksdb_put_get/1,
    rocksdb_delete/1,
    rocksdb_batch/1,
    rocksdb_fold/1,
    rocksdb_fold_range/1,
    rocksdb_snapshot/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, keys}, {group, rocksdb}].

groups() ->
    [
        {keys, [sequence], [
            key_db_meta,
            key_doc_info,
            key_doc_seq,
            key_encoding_order
        ]},
        {rocksdb, [sequence], [
            rocksdb_open_close,
            rocksdb_put_get,
            rocksdb_delete,
            rocksdb_batch,
            rocksdb_fold,
            rocksdb_fold_range,
            rocksdb_snapshot
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(rocksdb, Config) ->
    %% Create a temporary directory for RocksDB
    TestDir = "/tmp/barrel_store_test_" ++ integer_to_list(erlang:system_time(millisecond)),
    [{test_dir, TestDir} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(rocksdb, Config) ->
    %% Cleanup test directory
    TestDir = proplists:get_value(test_dir, Config),
    os:cmd("rm -rf " ++ TestDir),
    Config;
end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases - Key Encoding
%%====================================================================

key_db_meta(_Config) ->
    DbName = <<"testdb">>,

    %% Test UID key
    UidKey = barrel_store_keys:db_uid(DbName),
    ?assert(is_binary(UidKey)),

    %% Test docs count key
    DocsCountKey = barrel_store_keys:db_docs_count(DbName),
    ?assert(is_binary(DocsCountKey)),

    %% Test del count key
    DelCountKey = barrel_store_keys:db_del_count(DbName),
    ?assert(is_binary(DelCountKey)),

    %% Keys should be different
    ?assertNotEqual(UidKey, DocsCountKey),
    ?assertNotEqual(DocsCountKey, DelCountKey),

    ok.

key_doc_info(_Config) ->
    DbName = <<"testdb">>,
    DocId = <<"doc1">>,

    %% Test doc_info key
    InfoKey = barrel_store_keys:doc_info(DbName, DocId),
    ?assert(is_binary(InfoKey)),

    %% Test prefix
    Prefix = barrel_store_keys:doc_info_prefix(DbName),
    ?assert(is_binary(Prefix)),

    %% Key should start with prefix
    PrefixLen = byte_size(Prefix),
    <<Prefix:PrefixLen/binary, _/binary>> = InfoKey,

    %% Test decode
    DecodedDocId = barrel_store_keys:decode_doc_id(DbName, InfoKey),
    ?assertEqual(DocId, DecodedDocId),

    ok.

key_doc_seq(_Config) ->
    DbName = <<"testdb">>,

    %% Test sequence encoding with {Epoch, Counter} tuples
    Seq1 = {0, 1},
    Seq2 = {0, 100},
    Seq3 = {1, 0},  %% New epoch

    SeqKey1 = barrel_store_keys:doc_seq(DbName, Seq1),
    SeqKey2 = barrel_store_keys:doc_seq(DbName, Seq2),
    SeqKey3 = barrel_store_keys:doc_seq(DbName, Seq3),

    %% Keys should be in order (for proper iteration)
    ?assert(SeqKey1 < SeqKey2),
    ?assert(SeqKey2 < SeqKey3),

    %% Test decode
    DecodedSeq1 = barrel_store_keys:decode_seq_key(DbName, SeqKey1),
    DecodedSeq2 = barrel_store_keys:decode_seq_key(DbName, SeqKey2),
    DecodedSeq3 = barrel_store_keys:decode_seq_key(DbName, SeqKey3),

    ?assertEqual(Seq1, DecodedSeq1),
    ?assertEqual(Seq2, DecodedSeq2),
    ?assertEqual(Seq3, DecodedSeq3),

    ok.

key_encoding_order(_Config) ->
    DbName = <<"testdb">>,

    %% Different key types should sort properly
    MetaKey = barrel_store_keys:db_uid(DbName),
    DocInfoKey = barrel_store_keys:doc_info(DbName, <<"doc1">>),
    DocRevKey = barrel_store_keys:doc_rev(DbName, <<"doc1">>, <<"1-abc">>),
    DocSeqKey = barrel_store_keys:doc_seq(DbName, {0, 1}),
    LocalKey = barrel_store_keys:local_doc(DbName, <<"_local/test">>),

    %% Meta keys should come first (prefix 0x01)
    ?assert(MetaKey < DocInfoKey),

    %% Doc info before rev (prefix 0x02 < 0x03)
    ?assert(DocInfoKey < DocRevKey),

    %% Rev before seq (prefix 0x03 < 0x04)
    ?assert(DocRevKey < DocSeqKey),

    %% Seq before local (prefix 0x04 < 0x05)
    ?assert(DocSeqKey < LocalKey),

    ok.

%%====================================================================
%% Test Cases - RocksDB Backend
%%====================================================================

rocksdb_open_close(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/open_close_test",

    %% Open database
    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),
    ?assert(is_map(DbRef)),

    %% Close database
    ok = barrel_store_rocksdb:close(DbRef),

    ok.

rocksdb_put_get(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/put_get_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    Key = <<"test_key">>,
    Value = <<"test_value">>,

    %% Put value
    ok = barrel_store_rocksdb:put(DbRef, Key, Value),

    %% Get value
    {ok, RetrievedValue} = barrel_store_rocksdb:get(DbRef, Key),
    ?assertEqual(Value, RetrievedValue),

    %% Get non-existent key
    not_found = barrel_store_rocksdb:get(DbRef, <<"nonexistent">>),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_delete(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/delete_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    Key = <<"delete_key">>,
    Value = <<"delete_value">>,

    %% Put then delete
    ok = barrel_store_rocksdb:put(DbRef, Key, Value),
    {ok, Value} = barrel_store_rocksdb:get(DbRef, Key),

    ok = barrel_store_rocksdb:delete(DbRef, Key),
    not_found = barrel_store_rocksdb:get(DbRef, Key),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_batch(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/batch_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    %% Write batch of operations
    Operations = [
        {put, <<"key1">>, <<"value1">>},
        {put, <<"key2">>, <<"value2">>},
        {put, <<"key3">>, <<"value3">>}
    ],

    ok = barrel_store_rocksdb:write_batch(DbRef, Operations),

    %% Verify all keys exist
    {ok, <<"value1">>} = barrel_store_rocksdb:get(DbRef, <<"key1">>),
    {ok, <<"value2">>} = barrel_store_rocksdb:get(DbRef, <<"key2">>),
    {ok, <<"value3">>} = barrel_store_rocksdb:get(DbRef, <<"key3">>),

    %% Batch with delete
    DeleteOps = [
        {delete, <<"key2">>},
        {put, <<"key4">>, <<"value4">>}
    ],

    ok = barrel_store_rocksdb:write_batch(DbRef, DeleteOps),

    not_found = barrel_store_rocksdb:get(DbRef, <<"key2">>),
    {ok, <<"value4">>} = barrel_store_rocksdb:get(DbRef, <<"key4">>),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_fold(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/fold_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    %% Insert data with common prefix
    Prefix = <<"prefix:">>,
    Operations = [
        {put, <<Prefix/binary, "a">>, <<"val_a">>},
        {put, <<Prefix/binary, "b">>, <<"val_b">>},
        {put, <<Prefix/binary, "c">>, <<"val_c">>},
        {put, <<"other:x">>, <<"val_x">>}
    ],

    ok = barrel_store_rocksdb:write_batch(DbRef, Operations),

    %% Fold over prefix
    CollectFun = fun(Key, Value, Acc) ->
        {ok, [{Key, Value} | Acc]}
    end,

    Result = barrel_store_rocksdb:fold(DbRef, Prefix, CollectFun, []),

    %% Should have 3 items (not "other:x")
    ?assertEqual(3, length(Result)),

    %% Verify all items have the prefix
    lists:foreach(
        fun({Key, _Value}) ->
            PrefixLen = byte_size(Prefix),
            <<Prefix:PrefixLen/binary, _/binary>> = Key
        end,
        Result
    ),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_fold_range(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/fold_range_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    %% Insert data
    Operations = [
        {put, <<"a">>, <<"1">>},
        {put, <<"b">>, <<"2">>},
        {put, <<"c">>, <<"3">>},
        {put, <<"d">>, <<"4">>},
        {put, <<"e">>, <<"5">>}
    ],

    ok = barrel_store_rocksdb:write_batch(DbRef, Operations),

    %% Fold range b to d (exclusive end)
    CollectFun = fun(Key, Value, Acc) ->
        {ok, [{Key, Value} | Acc]}
    end,

    Result = barrel_store_rocksdb:fold_range(DbRef, <<"b">>, <<"d">>, CollectFun, []),

    %% Should have b and c (d is exclusive upper bound)
    ?assertEqual(2, length(Result)),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.

rocksdb_snapshot(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/snapshot_test",

    {ok, DbRef} = barrel_store_rocksdb:open(DbPath, #{}),

    Key = <<"snapshot_key">>,
    Value1 = <<"value1">>,
    Value2 = <<"value2">>,

    %% Put initial value
    ok = barrel_store_rocksdb:put(DbRef, Key, Value1),

    %% Create snapshot
    {ok, Snapshot} = barrel_store_rocksdb:snapshot(DbRef),

    %% Update value
    ok = barrel_store_rocksdb:put(DbRef, Key, Value2),

    %% Get with snapshot should return old value
    {ok, SnapshotValue} = barrel_store_rocksdb:get_with_snapshot(DbRef, Key, Snapshot),
    ?assertEqual(Value1, SnapshotValue),

    %% Get without snapshot should return new value
    {ok, CurrentValue} = barrel_store_rocksdb:get(DbRef, Key),
    ?assertEqual(Value2, CurrentValue),

    %% Release snapshot
    ok = barrel_store_rocksdb:release_snapshot(Snapshot),

    ok = barrel_store_rocksdb:close(DbRef),
    ok.
