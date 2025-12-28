%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb attachment layer
%%%
%%% Tests attachment storage with BlobDB backend.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
         init_per_group/2, end_per_group/2,
         init_per_testcase/2, end_per_testcase/2]).

%% Test cases - barrel_att_store
-export([
    att_store_open_close/1,
    att_store_put_get/1,
    att_store_delete/1,
    att_store_delete_all/1,
    att_store_fold/1,
    att_store_content_type/1
]).

%% Test cases - barrel_att
-export([
    att_put_get/1,
    att_delete/1,
    att_list/1,
    att_exists/1,
    att_validate_name/1,
    att_large_blob/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, att_store}, {group, att_api}].

groups() ->
    [
        {att_store, [sequence], [
            att_store_open_close,
            att_store_put_get,
            att_store_delete,
            att_store_delete_all,
            att_store_fold,
            att_store_content_type
        ]},
        {att_api, [sequence], [
            att_put_get,
            att_delete,
            att_list,
            att_exists,
            att_validate_name,
            att_large_blob
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    TestDir = "/tmp/barrel_att_test_" ++ integer_to_list(erlang:system_time(millisecond)),
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
%% Test Cases - barrel_att_store
%%====================================================================

att_store_open_close(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/store_open_close",

    %% Open store
    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),
    ?assert(is_map(AttRef)),

    %% Close store
    ok = barrel_att_store:close(AttRef),

    ok.

att_store_put_get(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/store_put_get",

    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),

    DbName = <<"testdb">>,
    DocId = <<"doc1">>,
    AttName = <<"image.png">>,
    Data = <<"fake png data">>,

    %% Put attachment
    {ok, AttInfo} = barrel_att_store:put(AttRef, DbName, DocId, AttName, Data),
    ?assertEqual(AttName, maps:get(name, AttInfo)),
    ?assertEqual(byte_size(Data), maps:get(length, AttInfo)),
    ?assert(is_binary(maps:get(digest, AttInfo))),
    ?assert(is_binary(maps:get(content_type, AttInfo))),

    %% Get attachment
    {ok, RetrievedData} = barrel_att_store:get(AttRef, DbName, DocId, AttName),
    ?assertEqual(Data, RetrievedData),

    %% Get non-existent attachment
    not_found = barrel_att_store:get(AttRef, DbName, DocId, <<"nonexistent">>),

    ok = barrel_att_store:close(AttRef),
    ok.

att_store_delete(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/store_delete",

    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),

    DbName = <<"testdb">>,
    DocId = <<"doc1">>,
    AttName = <<"file.txt">>,
    Data = <<"test content">>,

    %% Put then delete
    {ok, _} = barrel_att_store:put(AttRef, DbName, DocId, AttName, Data),
    {ok, Data} = barrel_att_store:get(AttRef, DbName, DocId, AttName),

    ok = barrel_att_store:delete(AttRef, DbName, DocId, AttName),
    not_found = barrel_att_store:get(AttRef, DbName, DocId, AttName),

    ok = barrel_att_store:close(AttRef),
    ok.

att_store_delete_all(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/store_delete_all",

    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),

    DbName = <<"testdb">>,
    DocId = <<"doc1">>,

    %% Put multiple attachments
    {ok, _} = barrel_att_store:put(AttRef, DbName, DocId, <<"file1.txt">>, <<"content1">>),
    {ok, _} = barrel_att_store:put(AttRef, DbName, DocId, <<"file2.txt">>, <<"content2">>),
    {ok, _} = barrel_att_store:put(AttRef, DbName, DocId, <<"file3.txt">>, <<"content3">>),

    %% Delete all
    ok = barrel_att_store:delete_all(AttRef, DbName, DocId),

    %% Verify all deleted
    not_found = barrel_att_store:get(AttRef, DbName, DocId, <<"file1.txt">>),
    not_found = barrel_att_store:get(AttRef, DbName, DocId, <<"file2.txt">>),
    not_found = barrel_att_store:get(AttRef, DbName, DocId, <<"file3.txt">>),

    ok = barrel_att_store:close(AttRef),
    ok.

att_store_fold(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/store_fold",

    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),

    DbName = <<"testdb">>,
    DocId = <<"doc1">>,

    %% Put multiple attachments
    {ok, _} = barrel_att_store:put(AttRef, DbName, DocId, <<"a.txt">>, <<"a">>),
    {ok, _} = barrel_att_store:put(AttRef, DbName, DocId, <<"b.txt">>, <<"b">>),
    {ok, _} = barrel_att_store:put(AttRef, DbName, DocId, <<"c.txt">>, <<"c">>),

    %% Put attachment for different doc (should not be included)
    {ok, _} = barrel_att_store:put(AttRef, DbName, <<"doc2">>, <<"other.txt">>, <<"other">>),

    %% Fold
    Result = barrel_att_store:fold(AttRef, DbName, DocId,
        fun(Name, _Data, Acc) -> {ok, [Name | Acc]} end,
        []),

    ?assertEqual(3, length(Result)),
    ?assert(lists:member(<<"a.txt">>, Result)),
    ?assert(lists:member(<<"b.txt">>, Result)),
    ?assert(lists:member(<<"c.txt">>, Result)),

    ok = barrel_att_store:close(AttRef),
    ok.

att_store_content_type(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/store_content_type",

    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),

    DbName = <<"testdb">>,
    DocId = <<"doc1">>,

    %% Test various content types via mimerl
    {ok, Info1} = barrel_att_store:put(AttRef, DbName, DocId, <<"test.html">>, <<"<html>">>),
    ?assertEqual(<<"text/html">>, maps:get(content_type, Info1)),

    {ok, Info2} = barrel_att_store:put(AttRef, DbName, DocId, <<"test.json">>, <<"{}">>),
    ?assertEqual(<<"application/json">>, maps:get(content_type, Info2)),

    {ok, Info3} = barrel_att_store:put(AttRef, DbName, DocId, <<"test.png">>, <<"PNG">>),
    ?assertEqual(<<"image/png">>, maps:get(content_type, Info3)),

    ok = barrel_att_store:close(AttRef),
    ok.

%%====================================================================
%% Test Cases - barrel_att API
%%====================================================================

att_put_get(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/att_put_get",

    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),

    DbName = <<"testdb">>,
    DocId = <<"doc1">>,
    AttName = <<"photo.jpg">>,
    Data = <<"fake jpeg data">>,

    %% Put via barrel_att
    {ok, AttInfo} = barrel_att:put_attachment(AttRef, DbName, DocId, AttName, Data),
    ?assertEqual(AttName, maps:get(name, AttInfo)),

    %% Get via barrel_att
    {ok, RetrievedData} = barrel_att:get_attachment(AttRef, DbName, DocId, AttName),
    ?assertEqual(Data, RetrievedData),

    %% Get non-existent
    {error, not_found} = barrel_att:get_attachment(AttRef, DbName, DocId, <<"nope">>),

    ok = barrel_att_store:close(AttRef),
    ok.

att_delete(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/att_delete",

    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),

    DbName = <<"testdb">>,
    DocId = <<"doc1">>,

    {ok, _} = barrel_att:put_attachment(AttRef, DbName, DocId, <<"file.txt">>, <<"data">>),

    %% Delete single
    ok = barrel_att:delete_attachment(AttRef, DbName, DocId, <<"file.txt">>),
    {error, not_found} = barrel_att:get_attachment(AttRef, DbName, DocId, <<"file.txt">>),

    %% Delete all
    {ok, _} = barrel_att:put_attachment(AttRef, DbName, DocId, <<"a.txt">>, <<"a">>),
    {ok, _} = barrel_att:put_attachment(AttRef, DbName, DocId, <<"b.txt">>, <<"b">>),

    ok = barrel_att:delete_doc_attachments(AttRef, DbName, DocId),
    ?assertEqual([], barrel_att:list_attachments(AttRef, DbName, DocId)),

    ok = barrel_att_store:close(AttRef),
    ok.

att_list(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/att_list",

    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),

    DbName = <<"testdb">>,
    DocId = <<"doc1">>,

    %% Initially empty
    ?assertEqual([], barrel_att:list_attachments(AttRef, DbName, DocId)),

    %% Add some
    {ok, _} = barrel_att:put_attachment(AttRef, DbName, DocId, <<"x.txt">>, <<"x">>),
    {ok, _} = barrel_att:put_attachment(AttRef, DbName, DocId, <<"y.txt">>, <<"y">>),

    List = barrel_att:list_attachments(AttRef, DbName, DocId),
    ?assertEqual(2, length(List)),
    ?assert(lists:member(<<"x.txt">>, List)),
    ?assert(lists:member(<<"y.txt">>, List)),

    ok = barrel_att_store:close(AttRef),
    ok.

att_exists(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/att_exists",

    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),

    DbName = <<"testdb">>,
    DocId = <<"doc1">>,

    ?assertNot(barrel_att:attachment_exists(AttRef, DbName, DocId, <<"test.txt">>)),

    {ok, _} = barrel_att:put_attachment(AttRef, DbName, DocId, <<"test.txt">>, <<"test">>),

    ?assert(barrel_att:attachment_exists(AttRef, DbName, DocId, <<"test.txt">>)),
    ?assertNot(barrel_att:attachment_exists(AttRef, DbName, DocId, <<"other.txt">>)),

    ok = barrel_att_store:close(AttRef),
    ok.

att_validate_name(_Config) ->
    %% Valid names
    ok = barrel_att:validate_att_name(<<"file.txt">>),
    ok = barrel_att:validate_att_name(<<"my-file.pdf">>),
    ok = barrel_att:validate_att_name(<<"file with spaces.doc">>),

    %% Invalid names
    {error, invalid_att_name} = barrel_att:validate_att_name(<<>>),
    {error, invalid_att_name} = barrel_att:validate_att_name(<<"path/to/file">>),
    {error, invalid_att_name} = barrel_att:validate_att_name(<<"path\\to\\file">>),
    {error, invalid_att_name} = barrel_att:validate_att_name(<<"file\0name">>),
    {error, invalid_att_name} = barrel_att:validate_att_name(not_binary),

    ok.

att_large_blob(Config) ->
    TestDir = proplists:get_value(test_dir, Config),
    DbPath = TestDir ++ "/att_large_blob",

    {ok, AttRef} = barrel_att_store:open(DbPath, #{}),

    DbName = <<"testdb">>,
    DocId = <<"doc1">>,
    AttName = <<"large.bin">>,

    %% Create a 1MB blob
    LargeData = crypto:strong_rand_bytes(1024 * 1024),

    {ok, AttInfo} = barrel_att:put_attachment(AttRef, DbName, DocId, AttName, LargeData),
    ?assertEqual(1024 * 1024, maps:get(length, AttInfo)),

    %% Retrieve and verify
    {ok, Retrieved} = barrel_att:get_attachment(AttRef, DbName, DocId, AttName),
    ?assertEqual(LargeData, Retrieved),

    ok = barrel_att_store:close(AttRef),
    ok.
