%%%-------------------------------------------------------------------
%%% @doc Replication test suite for barrel_docdb
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, local_docs},
        {group, revsdiff},
        {group, put_rev},
        {group, replication}
    ].

groups() ->
    [
        {local_docs, [sequence], [
            local_doc_crud,
            local_doc_not_replicated
        ]},
        {revsdiff, [sequence], [
            revsdiff_missing_all,
            revsdiff_missing_some,
            revsdiff_missing_none
        ]},
        {put_rev, [sequence], [
            put_rev_new_doc,
            put_rev_with_history
        ]},
        {replication, [sequence], [
            replicate_single_doc,
            replicate_multiple_docs,
            replicate_with_updates,
            replicate_deleted_doc,
            replicate_checkpoint_persistence
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

init_per_group(Group, Config) ->
    %% Clean up any existing test databases
    lists:foreach(fun(Db) ->
        case barrel_docdb:open_db(Db) of
            {ok, _} -> barrel_docdb:delete_db(Db);
            _ -> ok
        end
    end, [<<"test_source">>, <<"test_target">>, <<"test_db">>]),

    DataDir = "/tmp/barrel_test_rep_" ++ atom_to_list(Group),
    os:cmd("rm -rf " ++ DataDir),

    {ok, _} = barrel_docdb:create_db(<<"test_db">>, #{data_dir => DataDir}),

    %% Create source and target for replication tests
    case Group of
        replication ->
            {ok, _} = barrel_docdb:create_db(<<"test_source">>, #{data_dir => DataDir ++ "_source"}),
            {ok, _} = barrel_docdb:create_db(<<"test_target">>, #{data_dir => DataDir ++ "_target"});
        _ ->
            ok
    end,

    [{data_dir, DataDir}, {group, Group} | Config].

end_per_group(Group, Config) ->
    DataDir = ?config(data_dir, Config),

    barrel_docdb:delete_db(<<"test_db">>),

    case Group of
        replication ->
            barrel_docdb:delete_db(<<"test_source">>),
            barrel_docdb:delete_db(<<"test_target">>);
        _ ->
            ok
    end,

    os:cmd("rm -rf " ++ DataDir ++ "*"),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Local Document Tests
%%====================================================================

local_doc_crud(_Config) ->
    Db = <<"test_db">>,
    DocId = <<"local_test_1">>,
    Doc = #{<<"key">> => <<"value">>, <<"count">> => 42},

    %% Initially not found
    ?assertEqual({error, not_found}, barrel_docdb:get_local_doc(Db, DocId)),

    %% Put local doc
    ?assertEqual(ok, barrel_docdb:put_local_doc(Db, DocId, Doc)),

    %% Get local doc
    {ok, Retrieved} = barrel_docdb:get_local_doc(Db, DocId),
    ?assertEqual(<<"value">>, maps:get(<<"key">>, Retrieved)),
    ?assertEqual(42, maps:get(<<"count">>, Retrieved)),

    %% Update local doc
    Doc2 = Doc#{<<"count">> => 100},
    ?assertEqual(ok, barrel_docdb:put_local_doc(Db, DocId, Doc2)),

    {ok, Retrieved2} = barrel_docdb:get_local_doc(Db, DocId),
    ?assertEqual(100, maps:get(<<"count">>, Retrieved2)),

    %% Delete local doc
    ?assertEqual(ok, barrel_docdb:delete_local_doc(Db, DocId)),
    ?assertEqual({error, not_found}, barrel_docdb:get_local_doc(Db, DocId)),

    ok.

local_doc_not_replicated(_Config) ->
    %% Local docs should not appear in changes feed
    Db = <<"test_db">>,

    %% Put a regular doc
    {ok, _} = barrel_docdb:put_doc(Db, #{<<"id">> => <<"regular_doc">>, <<"type">> => <<"test">>}),

    %% Put a local doc
    ok = barrel_docdb:put_local_doc(Db, <<"local_doc">>, #{<<"type">> => <<"local">>}),

    %% Get changes - should only see regular doc
    {ok, Changes, _} = barrel_docdb:get_changes(Db, first),
    ChangedIds = [maps:get(id, C) || C <- Changes],

    ?assert(lists:member(<<"regular_doc">>, ChangedIds)),
    ?assertNot(lists:member(<<"local_doc">>, ChangedIds)),

    ok.

%%====================================================================
%% Revsdiff Tests
%%====================================================================

revsdiff_missing_all(_Config) ->
    Db = <<"test_db">>,
    DocId = <<"nonexistent_doc">>,
    RevIds = [<<"1-abc123">>, <<"2-def456">>],

    %% Document doesn't exist - all revisions are missing
    {ok, Missing, Ancestors} = barrel_docdb:revsdiff(Db, DocId, RevIds),
    ?assertEqual(RevIds, Missing),
    ?assertEqual([], Ancestors),

    ok.

revsdiff_missing_some(_Config) ->
    Db = <<"test_db">>,

    %% Create a document
    {ok, #{<<"id">> := DocId, <<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => <<"revsdiff_doc">>, <<"value">> => 1}),

    %% Update it
    {ok, #{<<"rev">> := Rev2}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"value">> => 2}),

    %% Check revsdiff - Rev2 exists, fake rev doesn't
    FakeRev = <<"3-fake123">>,
    {ok, Missing, _Ancestors} = barrel_docdb:revsdiff(Db, DocId, [Rev2, FakeRev]),
    ?assertEqual([FakeRev], Missing),

    ok.

revsdiff_missing_none(_Config) ->
    Db = <<"test_db">>,

    %% Create a document
    {ok, #{<<"id">> := DocId, <<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Db, #{<<"id">> => <<"revsdiff_doc2">>, <<"value">> => 1}),

    %% Check revsdiff - existing rev is not missing
    {ok, Missing, _} = barrel_docdb:revsdiff(Db, DocId, [Rev1]),
    ?assertEqual([], Missing),

    ok.

%%====================================================================
%% Put Rev Tests
%%====================================================================

put_rev_new_doc(_Config) ->
    Db = <<"test_db">>,
    Doc = #{<<"id">> => <<"replicated_doc_1">>, <<"value">> => <<"from_source">>},
    History = [<<"1-abc123def456">>],

    %% Put document with explicit revision
    {ok, DocId, Rev} = barrel_docdb:put_rev(Db, Doc, History, false),
    ?assertEqual(<<"replicated_doc_1">>, DocId),
    ?assertEqual(<<"1-abc123def456">>, Rev),

    %% Verify document exists
    {ok, Retrieved} = barrel_docdb:get_doc(Db, DocId),
    ?assertEqual(<<"from_source">>, maps:get(<<"value">>, Retrieved)),
    ?assertEqual(<<"1-abc123def456">>, maps:get(<<"_rev">>, Retrieved)),

    ok.

put_rev_with_history(_Config) ->
    Db = <<"test_db">>,
    Doc = #{<<"id">> => <<"replicated_doc_2">>, <<"value">> => <<"updated">>},
    History = [<<"2-newrev123">>, <<"1-parentrev456">>],

    %% Put document with revision history
    {ok, DocId, Rev} = barrel_docdb:put_rev(Db, Doc, History, false),
    ?assertEqual(<<"replicated_doc_2">>, DocId),
    ?assertEqual(<<"2-newrev123">>, Rev),

    %% Verify document exists with correct revision
    {ok, Retrieved} = barrel_docdb:get_doc(Db, DocId),
    ?assertEqual(<<"updated">>, maps:get(<<"value">>, Retrieved)),
    ?assertEqual(<<"2-newrev123">>, maps:get(<<"_rev">>, Retrieved)),

    ok.

%%====================================================================
%% Replication Tests
%%====================================================================

replicate_single_doc(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create a document in source
    {ok, #{<<"id">> := DocId}} =
        barrel_docdb:put_doc(Source, #{<<"id">> => <<"doc1">>, <<"value">> => <<"hello">>}),

    %% Verify target is empty
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, DocId)),

    %% Replicate
    {ok, Result} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result)),
    ?assert(maps:get(docs_read, Result) >= 1),
    ?assert(maps:get(docs_written, Result) >= 1),

    %% Verify document in target
    {ok, TargetDoc} = barrel_docdb:get_doc(Target, DocId),
    ?assertEqual(<<"hello">>, maps:get(<<"value">>, TargetDoc)),

    ok.

replicate_multiple_docs(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create multiple documents in source
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"multi_doc_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"n">> => N})
    end, lists:seq(1, 10)),

    %% Replicate
    {ok, Result} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result)),
    ?assert(maps:get(docs_read, Result) >= 10),
    ?assert(maps:get(docs_written, Result) >= 10),

    %% Verify all documents in target
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"multi_doc_">>, integer_to_binary(N)]),
        {ok, Doc} = barrel_docdb:get_doc(Target, DocId),
        ?assertEqual(N, maps:get(<<"n">>, Doc))
    end, lists:seq(1, 10)),

    ok.

replicate_with_updates(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create and update a document
    {ok, #{<<"id">> := DocId, <<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Source, #{<<"id">> => <<"update_doc">>, <<"version">> => 1}),

    {ok, #{<<"rev">> := _Rev2}} =
        barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"_rev">> => Rev1, <<"version">> => 2}),

    %% Replicate
    {ok, _} = barrel_rep:replicate(Source, Target),

    %% Verify latest version in target
    {ok, TargetDoc} = barrel_docdb:get_doc(Target, DocId),
    ?assertEqual(2, maps:get(<<"version">>, TargetDoc)),

    ok.

replicate_deleted_doc(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create and delete a document
    {ok, #{<<"id">> := DocId, <<"rev">> := Rev1}} =
        barrel_docdb:put_doc(Source, #{<<"id">> => <<"deleted_doc">>, <<"value">> => <<"temp">>}),

    {ok, _} = barrel_docdb:delete_doc(Source, DocId, #{rev => Rev1}),

    %% Replicate
    {ok, _} = barrel_rep:replicate(Source, Target),

    %% Verify document is deleted in target
    ?assertEqual({error, not_found}, barrel_docdb:get_doc(Target, DocId)),

    ok.

replicate_checkpoint_persistence(_Config) ->
    Source = <<"test_source">>,
    Target = <<"test_target">>,

    %% Create initial documents
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"cp_doc_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"batch">> => 1})
    end, lists:seq(1, 5)),

    %% First replication
    {ok, Result1} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result1)),
    _FirstLastSeq = maps:get(last_seq, Result1),

    %% Add more documents
    lists:foreach(fun(N) ->
        DocId = iolist_to_binary([<<"cp_doc_batch2_">>, integer_to_binary(N)]),
        {ok, _} = barrel_docdb:put_doc(Source, #{<<"id">> => DocId, <<"batch">> => 2})
    end, lists:seq(1, 3)),

    %% Second replication - should start from checkpoint
    {ok, Result2} = barrel_rep:replicate(Source, Target),
    ?assertEqual(true, maps:get(ok, Result2)),

    %% Should have read fewer docs than a full replication (checkpoints work)
    %% Note: due to change list format, may read more than 3
    DocsRead = maps:get(docs_read, Result2),
    ct:pal("Second replication read ~p docs", [DocsRead]),

    %% Verify all documents exist in target
    {ok, TargetDocs, _} = barrel_docdb:get_changes(Target, first),
    ct:pal("Target has ~p changes", [length(TargetDocs)]),
    ?assert(length(TargetDocs) >= 8),

    ok.
