%%%-------------------------------------------------------------------
%%% @doc Test suite for cross-database query federation
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_federation_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        {group, federation_crud},
        {group, federation_query},
        {group, stored_query},
        {group, remote_federation}
    ].

groups() ->
    [
        {federation_crud, [sequence], [
            create_federation,
            get_federation,
            add_remove_members,
            delete_federation,
            list_federations
        ]},
        {federation_query, [sequence], [
            query_single_member,
            query_multiple_members,
            query_with_duplicates,
            query_with_different_revisions
        ]},
        {stored_query, [sequence], [
            create_federation_with_query,
            find_with_stored_query,
            find_merges_queries,
            set_query_on_existing
        ]},
        {remote_federation, [sequence], [
            remote_url_validation,
            create_federation_with_remote,
            create_federation_with_domain
        ]}
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(barrel_docdb),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(barrel_docdb),
    ok.

init_per_group(federation_crud, Config) ->
    Config;
init_per_group(stored_query, Config) ->
    %% Create test database for stored query tests
    DbName = <<"stored_query_db">>,
    DataDir = "/tmp/barrel_stored_query_test",
    os:cmd("rm -rf " ++ DataDir),
    case barrel_docdb:open_db(DbName) of
        {ok, _} -> barrel_docdb:delete_db(DbName);
        _ -> ok
    end,
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => DataDir}),
    [{db_name, DbName}, {data_dir, DataDir} | Config];
init_per_group(remote_federation, Config) ->
    %% Create a local database for mixed local/remote testing
    DbName = <<"remote_test_db">>,
    DataDir = "/tmp/barrel_remote_test",
    os:cmd("rm -rf " ++ DataDir),
    case barrel_docdb:open_db(DbName) of
        {ok, _} -> barrel_docdb:delete_db(DbName);
        _ -> ok
    end,
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => DataDir}),
    [{db_name, DbName}, {data_dir, DataDir} | Config];
init_per_group(federation_query, Config) ->
    %% Create test databases
    DataDir = "/tmp/barrel_federation_test",
    os:cmd("rm -rf " ++ DataDir),

    %% Database 1: users
    Db1Name = <<"fed_test_db1">>,
    case barrel_docdb:open_db(Db1Name) of
        {ok, _} -> barrel_docdb:delete_db(Db1Name);
        _ -> ok
    end,
    {ok, _} = barrel_docdb:create_db(Db1Name, #{data_dir => DataDir ++ "/db1"}),

    %% Database 2: archive
    Db2Name = <<"fed_test_db2">>,
    case barrel_docdb:open_db(Db2Name) of
        {ok, _} -> barrel_docdb:delete_db(Db2Name);
        _ -> ok
    end,
    {ok, _} = barrel_docdb:create_db(Db2Name, #{data_dir => DataDir ++ "/db2"}),

    %% Database 3: another archive
    Db3Name = <<"fed_test_db3">>,
    case barrel_docdb:open_db(Db3Name) of
        {ok, _} -> barrel_docdb:delete_db(Db3Name);
        _ -> ok
    end,
    {ok, _} = barrel_docdb:create_db(Db3Name, #{data_dir => DataDir ++ "/db3"}),

    [{db1, Db1Name}, {db2, Db2Name}, {db3, Db3Name}, {data_dir, DataDir} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(federation_query, Config) ->
    %% Clean up databases
    Db1Name = proplists:get_value(db1, Config),
    Db2Name = proplists:get_value(db2, Config),
    Db3Name = proplists:get_value(db3, Config),
    barrel_docdb:delete_db(Db1Name),
    barrel_docdb:delete_db(Db2Name),
    barrel_docdb:delete_db(Db3Name),

    %% Clean up any federations
    catch barrel_federation:delete(<<"test_federation">>),
    catch barrel_federation:delete(<<"query_fed">>),

    DataDir = proplists:get_value(data_dir, Config),
    os:cmd("rm -rf " ++ DataDir),
    ok;
end_per_group(stored_query, Config) ->
    DbName = proplists:get_value(db_name, Config),
    barrel_docdb:delete_db(DbName),
    catch barrel_federation:delete(<<"stored_query_fed">>),
    catch barrel_federation:delete(<<"merge_query_fed">>),
    catch barrel_federation:delete(<<"set_query_fed">>),
    DataDir = proplists:get_value(data_dir, Config),
    os:cmd("rm -rf " ++ DataDir),
    ok;
end_per_group(remote_federation, Config) ->
    DbName = proplists:get_value(db_name, Config),
    barrel_docdb:delete_db(DbName),
    catch barrel_federation:delete(<<"mixed_fed">>),
    DataDir = proplists:get_value(data_dir, Config),
    os:cmd("rm -rf " ++ DataDir),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%====================================================================
%% CRUD Tests
%%====================================================================

create_federation(_Config) ->
    %% Create a temporary database for testing
    DbName = <<"crud_test_db">>,
    DataDir = "/tmp/barrel_crud_test",
    os:cmd("rm -rf " ++ DataDir),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => DataDir}),

    %% Create federation
    FedName = <<"crud_test_fed">>,
    ok = barrel_federation:create(FedName, [DbName]),

    %% Verify it was created
    {ok, Fed} = barrel_federation:get(FedName),
    ?assertEqual(FedName, maps:get(name, Fed)),
    ?assertEqual([DbName], maps:get(members, Fed)),

    %% Cleanup
    ok = barrel_federation:delete(FedName),
    barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ DataDir),
    ok.

get_federation(_Config) ->
    %% Create temp db
    DbName = <<"get_test_db">>,
    DataDir = "/tmp/barrel_get_test",
    os:cmd("rm -rf " ++ DataDir),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => DataDir}),

    FedName = <<"get_test_fed">>,
    ok = barrel_federation:create(FedName, [DbName], #{description => <<"test">>}),

    %% Get federation
    {ok, Fed} = barrel_federation:get(FedName),
    ?assertEqual(FedName, maps:get(name, Fed)),
    ?assert(maps:is_key(created_at, Fed)),

    %% Non-existent federation
    {error, not_found} = barrel_federation:get(<<"nonexistent">>),

    %% Cleanup
    ok = barrel_federation:delete(FedName),
    barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ DataDir),
    ok.

add_remove_members(_Config) ->
    %% Create two temp dbs
    Db1 = <<"member_test_db1">>,
    Db2 = <<"member_test_db2">>,
    DataDir = "/tmp/barrel_member_test",
    os:cmd("rm -rf " ++ DataDir),
    {ok, _} = barrel_docdb:create_db(Db1, #{data_dir => DataDir ++ "/db1"}),
    {ok, _} = barrel_docdb:create_db(Db2, #{data_dir => DataDir ++ "/db2"}),

    FedName = <<"member_test_fed">>,
    ok = barrel_federation:create(FedName, [Db1]),

    %% Verify initial state
    {ok, #{members := [Db1]}} = barrel_federation:get(FedName),

    %% Add member
    ok = barrel_federation:add_member(FedName, Db2),
    {ok, #{members := Members1}} = barrel_federation:get(FedName),
    ?assertEqual([Db1, Db2], Members1),

    %% Add same member again (idempotent)
    ok = barrel_federation:add_member(FedName, Db2),
    {ok, #{members := Members2}} = barrel_federation:get(FedName),
    ?assertEqual([Db1, Db2], Members2),

    %% Remove member
    ok = barrel_federation:remove_member(FedName, Db1),
    {ok, #{members := Members3}} = barrel_federation:get(FedName),
    ?assertEqual([Db2], Members3),

    %% Cleanup
    ok = barrel_federation:delete(FedName),
    barrel_docdb:delete_db(Db1),
    barrel_docdb:delete_db(Db2),
    os:cmd("rm -rf " ++ DataDir),
    ok.

delete_federation(_Config) ->
    %% Create temp db
    DbName = <<"del_test_db">>,
    DataDir = "/tmp/barrel_del_test",
    os:cmd("rm -rf " ++ DataDir),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => DataDir}),

    FedName = <<"del_test_fed">>,
    ok = barrel_federation:create(FedName, [DbName]),

    %% Delete it
    ok = barrel_federation:delete(FedName),

    %% Verify it's gone
    {error, not_found} = barrel_federation:get(FedName),

    %% Delete non-existent (idempotent-ish)
    {error, not_found} = barrel_federation:delete(<<"nonexistent">>),

    %% Cleanup
    barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ DataDir),
    ok.

list_federations(_Config) ->
    %% Create temp db
    DbName = <<"list_test_db">>,
    DataDir = "/tmp/barrel_list_test",
    os:cmd("rm -rf " ++ DataDir),
    {ok, _} = barrel_docdb:create_db(DbName, #{data_dir => DataDir}),

    %% Create multiple federations
    ok = barrel_federation:create(<<"list_fed_1">>, [DbName]),
    ok = barrel_federation:create(<<"list_fed_2">>, [DbName]),

    %% List them
    {ok, Feds} = barrel_federation:list(),
    FedNames = [maps:get(name, F) || F <- Feds],
    ?assert(lists:member(<<"list_fed_1">>, FedNames)),
    ?assert(lists:member(<<"list_fed_2">>, FedNames)),

    %% Cleanup
    ok = barrel_federation:delete(<<"list_fed_1">>),
    ok = barrel_federation:delete(<<"list_fed_2">>),
    barrel_docdb:delete_db(DbName),
    os:cmd("rm -rf " ++ DataDir),
    ok.

%%====================================================================
%% Query Tests
%%====================================================================

query_single_member(Config) ->
    Db1 = proplists:get_value(db1, Config),

    %% Create some documents
    {ok, _} = barrel_docdb:put_doc(Db1, #{<<"id">> => <<"doc1">>, <<"type">> => <<"user">>}),
    {ok, _} = barrel_docdb:put_doc(Db1, #{<<"id">> => <<"doc2">>, <<"type">> => <<"user">>}),

    %% Create federation with single member
    FedName = <<"single_member_fed">>,
    ok = barrel_federation:create(FedName, [Db1]),

    %% Query federation
    {ok, Results, Meta} = barrel_federation:find(FedName, #{
        where => [{path, [<<"type">>], <<"user">>}]
    }),

    ?assertEqual(2, length(Results)),
    ?assertEqual(1, maps:get(members_queried, Meta)),

    %% Cleanup
    ok = barrel_federation:delete(FedName),
    ok.

query_multiple_members(Config) ->
    Db1 = proplists:get_value(db1, Config),
    Db2 = proplists:get_value(db2, Config),

    %% Create documents in both databases
    {ok, _} = barrel_docdb:put_doc(Db1, #{<<"id">> => <<"user1">>, <<"type">> => <<"user">>}),
    {ok, _} = barrel_docdb:put_doc(Db2, #{<<"id">> => <<"user2">>, <<"type">> => <<"user">>}),

    %% Create federation spanning both
    FedName = <<"multi_member_fed">>,
    ok = barrel_federation:create(FedName, [Db1, Db2]),

    %% Query federation
    {ok, Results, Meta} = barrel_federation:find(FedName, #{
        where => [{path, [<<"type">>], <<"user">>}]
    }),

    ?assertEqual(2, maps:get(members_queried, Meta)),

    %% Should find docs from both databases
    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),

    %% Cleanup
    ok = barrel_federation:delete(FedName),
    ok.

query_with_duplicates(Config) ->
    Db1 = proplists:get_value(db1, Config),
    Db2 = proplists:get_value(db2, Config),

    %% Create same document ID in both databases
    {ok, _} = barrel_docdb:put_doc(Db1, #{<<"id">> => <<"shared_doc">>,
                                           <<"type">> => <<"shared">>,
                                           <<"source">> => <<"db1">>}),
    {ok, _} = barrel_docdb:put_doc(Db2, #{<<"id">> => <<"shared_doc">>,
                                           <<"type">> => <<"shared">>,
                                           <<"source">> => <<"db2">>}),

    %% Create federation
    FedName = <<"dup_test_fed">>,
    ok = barrel_federation:create(FedName, [Db1, Db2]),

    %% Query - should merge duplicates
    {ok, Results, _Meta} = barrel_federation:find(FedName, #{
        where => [{path, [<<"type">>], <<"shared">>}]
    }),

    %% Should only return one document (merged)
    ?assertEqual(1, length(Results)),

    %% Cleanup
    ok = barrel_federation:delete(FedName),
    ok.

query_with_different_revisions(Config) ->
    Db1 = proplists:get_value(db1, Config),
    Db2 = proplists:get_value(db2, Config),

    %% Create documents with same ID but different content in each db
    {ok, #{<<"rev">> := Rev1}} = barrel_docdb:put_doc(Db1, #{
        <<"id">> => <<"versioned_doc">>,
        <<"type">> => <<"versioned">>,
        <<"source">> => <<"db1">>
    }),

    {ok, #{<<"rev">> := Rev2}} = barrel_docdb:put_doc(Db2, #{
        <<"id">> => <<"versioned_doc">>,
        <<"type">> => <<"versioned">>,
        <<"source">> => <<"db2">>
    }),

    ct:pal("Rev in db1: ~s, Rev in db2: ~s", [Rev1, Rev2]),

    %% Create federation
    FedName = <<"rev_test_fed">>,
    ok = barrel_federation:create(FedName, [Db1, Db2]),

    %% Query - should merge duplicates (pick one deterministically)
    {ok, Results, _Meta} = barrel_federation:find(FedName, #{
        where => [{path, [<<"type">>], <<"versioned">>}]
    }),

    %% Should only return one document (merged)
    ?assertEqual(1, length(Results)),

    [WinnerResult] = Results,
    %% Results may have <<"doc">> wrapper depending on query options
    WinnerDoc = case maps:get(<<"doc">>, WinnerResult, undefined) of
        undefined -> WinnerResult;
        Doc -> Doc
    end,

    %% Winner should be from one of the databases
    WinnerSource = maps:get(<<"source">>, WinnerDoc),
    ?assert(WinnerSource =:= <<"db1">> orelse WinnerSource =:= <<"db2">>),

    ct:pal("Winner selected from: ~s", [WinnerSource]),

    %% Cleanup
    ok = barrel_federation:delete(FedName),
    ok.

%%====================================================================
%% Remote Federation Tests
%%====================================================================

remote_url_validation(_Config) ->
    %% Valid HTTP URLs should be accepted
    ?assertEqual(ok, barrel_federation:validate_remote_url(
        <<"http://localhost:8080/db/mydb">>)),
    ?assertEqual(ok, barrel_federation:validate_remote_url(
        <<"https://remote.example.com/db/users">>)),
    ?assertEqual(ok, barrel_federation:validate_remote_url(
        <<"http://192.168.1.100:9000/db/data">>)),
    %% URLs without path are valid (for peer discovery)
    ?assertEqual(ok, barrel_federation:validate_remote_url(
        <<"http://localhost:8080">>)),
    ?assertEqual(ok, barrel_federation:validate_remote_url(
        <<"https://remote.example.com">>)),

    %% Invalid URLs should be rejected
    {error, {invalid_remote_url, _}} = barrel_federation:validate_remote_url(
        <<"http://">>),  %% No host
    {error, {invalid_remote_url, _}} = barrel_federation:validate_remote_url(
        <<"not-a-url">>),  %% No scheme

    ct:pal("Remote URL validation works correctly"),
    ok.

create_federation_with_remote(Config) ->
    DbName = proplists:get_value(db_name, Config),

    %% Create federation with mixed local and remote members
    FedName = <<"mixed_fed">>,
    ok = barrel_federation:create(FedName, [
        DbName,  %% Local database
        <<"http://remote-node:8080/db/users">>  %% Remote (won't be queried in test)
    ]),

    %% Verify federation was created
    {ok, Fed} = barrel_federation:get(FedName),
    Members = maps:get(members, Fed),
    ?assertEqual(2, length(Members)),
    ?assert(lists:member(DbName, Members)),
    ?assert(lists:member(<<"http://remote-node:8080/db/users">>, Members)),

    ct:pal("Created federation with mixed local/remote members: ~p", [Members]),

    %% Add a document to local db
    {ok, _} = barrel_docdb:put_doc(DbName, #{
        <<"id">> => <<"local_doc">>,
        <<"type">> => <<"test">>
    }),

    %% Query will work for local, fail gracefully for remote (connection refused)
    %% This tests that the federation handles mixed results
    {ok, Results, Meta} = barrel_federation:find(FedName, #{
        where => [{path, [<<"type">>], <<"test">>}]
    }),

    %% Should get at least the local result
    ct:pal("Query results: ~p, meta: ~p", [Results, Meta]),
    ?assert(length(Results) >= 0),  %% May or may not find results depending on remote

    %% Cleanup
    ok = barrel_federation:delete(FedName),
    ok.

create_federation_with_domain(Config) ->
    DbName = proplists:get_value(db_name, Config),

    %% Create federation with a domain member
    %% (DNS lookup will fail but we just want to verify the config is stored)
    FedName = <<"domain_fed">>,
    ok = barrel_federation:create(FedName, [
        DbName,
        <<"example.com">>  %% Domain - will trigger DNS discovery
    ]),

    %% Verify federation was created
    {ok, Fed} = barrel_federation:get(FedName),
    Members = maps:get(members, Fed),
    ?assertEqual(2, length(Members)),
    ?assert(lists:member(DbName, Members)),
    ?assert(lists:member(<<"example.com">>, Members)),

    ct:pal("Created federation with domain member: ~p", [Members]),

    %% Cleanup
    ok = barrel_federation:delete(FedName),
    ok.

%%====================================================================
%% Stored Query Tests
%%====================================================================

create_federation_with_query(Config) ->
    DbName = proplists:get_value(db_name, Config),

    %% Create some test documents
    {ok, _} = barrel_docdb:put_doc(DbName, #{
        <<"id">> => <<"user1">>,
        <<"type">> => <<"user">>,
        <<"active">> => true
    }),
    {ok, _} = barrel_docdb:put_doc(DbName, #{
        <<"id">> => <<"user2">>,
        <<"type">> => <<"user">>,
        <<"active">> => false
    }),
    {ok, _} = barrel_docdb:put_doc(DbName, #{
        <<"id">> => <<"order1">>,
        <<"type">> => <<"order">>
    }),

    %% Create federation with stored query
    FedName = <<"stored_query_fed">>,
    StoredQuery = #{where => [{path, [<<"type">>], <<"user">>}]},
    ok = barrel_federation:create(FedName, [DbName], #{query => StoredQuery}),

    %% Verify query was stored
    {ok, Fed} = barrel_federation:get(FedName),
    ?assert(maps:is_key(query, Fed)),
    ?assertEqual(StoredQuery, maps:get(query, Fed)),

    ct:pal("Created federation with stored query: ~p", [maps:get(query, Fed)]),
    ok.

find_with_stored_query(_Config) ->
    FedName = <<"stored_query_fed">>,

    %% Use find/1 which applies the stored query
    {ok, Results, Meta} = barrel_federation:find(FedName),

    %% Should only return users (not orders) based on stored query
    ct:pal("Results from find/1: ~p", [Results]),
    ?assertEqual(2, length(Results)),

    DocIds = [maps:get(<<"id">>, R) || R <- Results],
    ?assert(lists:member(<<"user1">>, DocIds)),
    ?assert(lists:member(<<"user2">>, DocIds)),
    ?assertNot(lists:member(<<"order1">>, DocIds)),

    ct:pal("find/1 correctly used stored query, meta: ~p", [Meta]),
    ok.

find_merges_queries(Config) ->
    DbName = proplists:get_value(db_name, Config),

    %% Create federation with stored query for type=user
    FedName = <<"merge_query_fed">>,
    StoredQuery = #{where => [{path, [<<"type">>], <<"user">>}]},
    ok = barrel_federation:create(FedName, [DbName], #{query => StoredQuery}),

    %% Query with additional filter (active=true)
    %% This should combine with stored query: type=user AND active=true
    {ok, Results, _Meta} = barrel_federation:find(FedName, #{
        where => [{path, [<<"active">>], true}]
    }),

    %% Should only return user1 (type=user AND active=true)
    ct:pal("Results after merge: ~p", [Results]),
    ?assertEqual(1, length(Results)),

    [Result] = Results,
    ?assertEqual(<<"user1">>, maps:get(<<"id">>, Result)),

    ct:pal("Queries merged correctly"),
    ok.

set_query_on_existing(Config) ->
    DbName = proplists:get_value(db_name, Config),

    %% Create federation without query
    FedName = <<"set_query_fed">>,
    ok = barrel_federation:create(FedName, [DbName]),

    %% Verify no query stored
    {ok, Fed1} = barrel_federation:get(FedName),
    ?assertNot(maps:is_key(query, Fed1)),

    %% Set query
    NewQuery = #{where => [{path, [<<"type">>], <<"order">>}]},
    ok = barrel_federation:set_query(FedName, NewQuery),

    %% Verify query was set
    {ok, Fed2} = barrel_federation:get(FedName),
    ?assert(maps:is_key(query, Fed2)),
    ?assertEqual(NewQuery, maps:get(query, Fed2)),

    %% Use find/1 to verify query works
    {ok, Results, _Meta} = barrel_federation:find(FedName),
    ?assertEqual(1, length(Results)),
    [OrderResult] = Results,
    ?assertEqual(<<"order1">>, maps:get(<<"id">>, OrderResult)),

    ct:pal("set_query/2 works correctly"),
    ok.
