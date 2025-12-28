%%%-------------------------------------------------------------------
%%% @doc Test suite for barrel_docdb document core
%%%
%%% Tests barrel_doc and barrel_revtree modules.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_doc_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% CT callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1]).

%% Test cases - barrel_doc
-export([
    doc_id/1,
    doc_rev/1,
    doc_deleted/1,
    parse_revision/1,
    make_revision/1,
    revision_hash/1,
    compare_revisions/1,
    encode_revisions/1,
    parse_revisions/1,
    doc_without_meta/1,
    make_doc_record/1,
    generate_docid/1
]).

%% Test cases - barrel_revtree
-export([
    revtree_new/1,
    revtree_add/1,
    revtree_contains/1,
    revtree_parent/1,
    revtree_history/1,
    revtree_leaves/1,
    revtree_is_leaf/1,
    revtree_winning_revision/1,
    revtree_conflicts/1,
    revtree_prune/1
]).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [{group, doc}, {group, revtree}].

groups() ->
    [
        {doc, [sequence], [
            doc_id,
            doc_rev,
            doc_deleted,
            parse_revision,
            make_revision,
            revision_hash,
            compare_revisions,
            encode_revisions,
            parse_revisions,
            doc_without_meta,
            make_doc_record,
            generate_docid
        ]},
        {revtree, [sequence], [
            revtree_new,
            revtree_add,
            revtree_contains,
            revtree_parent,
            revtree_history,
            revtree_leaves,
            revtree_is_leaf,
            revtree_winning_revision,
            revtree_conflicts,
            revtree_prune
        ]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%====================================================================
%% Test Cases - barrel_doc
%%====================================================================

doc_id(_Config) ->
    %% Document with id
    Doc1 = #{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>},
    ?assertEqual(<<"doc1">>, barrel_doc:id(Doc1)),

    %% Document without id
    Doc2 = #{<<"name">> => <<"test">>},
    ?assertEqual(undefined, barrel_doc:id(Doc2)),

    ok.

doc_rev(_Config) ->
    %% Document with rev
    Doc1 = #{<<"id">> => <<"doc1">>, <<"_rev">> => <<"1-abc">>},
    ?assertEqual(<<"1-abc">>, barrel_doc:rev(Doc1)),

    %% Document without rev
    Doc2 = #{<<"id">> => <<"doc1">>},
    ?assertEqual(<<>>, barrel_doc:rev(Doc2)),

    ok.

doc_deleted(_Config) ->
    %% Deleted document
    Doc1 = #{<<"id">> => <<"doc1">>, <<"_deleted">> => true},
    ?assertEqual(true, barrel_doc:deleted(Doc1)),

    %% Not deleted
    Doc2 = #{<<"id">> => <<"doc1">>, <<"_deleted">> => false},
    ?assertEqual(false, barrel_doc:deleted(Doc2)),

    %% No deleted flag
    Doc3 = #{<<"id">> => <<"doc1">>},
    ?assertEqual(false, barrel_doc:deleted(Doc3)),

    ok.

parse_revision(_Config) ->
    %% Normal revision
    ?assertEqual({10, <<"abc123">>}, barrel_doc:parse_revision(<<"10-abc123">>)),

    %% First generation
    ?assertEqual({1, <<"xyz">>}, barrel_doc:parse_revision(<<"1-xyz">>)),

    %% Empty revision
    ?assertEqual({0, <<>>}, barrel_doc:parse_revision(<<>>)),

    %% String revision
    ?assertEqual({5, <<"def">>}, barrel_doc:parse_revision("5-def")),

    ok.

make_revision(_Config) ->
    Rev = barrel_doc:make_revision(3, <<"abc123">>),
    ?assertEqual(<<"3-abc123">>, Rev),

    Rev2 = barrel_doc:make_revision(1, <<"xyz">>),
    ?assertEqual(<<"1-xyz">>, Rev2),

    ok.

revision_hash(_Config) ->
    Doc = #{<<"name">> => <<"test">>},

    %% Hash is deterministic
    Hash1 = barrel_doc:revision_hash(Doc, <<>>, false),
    Hash2 = barrel_doc:revision_hash(Doc, <<>>, false),
    ?assertEqual(Hash1, Hash2),

    %% Different content produces different hash
    Doc2 = #{<<"name">> => <<"other">>},
    Hash3 = barrel_doc:revision_hash(Doc2, <<>>, false),
    ?assertNotEqual(Hash1, Hash3),

    %% Deleted flag affects hash
    Hash4 = barrel_doc:revision_hash(Doc, <<>>, true),
    ?assertNotEqual(Hash1, Hash4),

    %% Hash is hex encoded
    ?assert(is_binary(Hash1)),
    ?assertEqual(64, byte_size(Hash1)),  % SHA-256 = 32 bytes = 64 hex chars

    ok.

compare_revisions(_Config) ->
    %% Higher generation wins
    ?assertEqual(1, barrel_doc:compare_revisions(<<"2-abc">>, <<"1-abc">>)),
    ?assertEqual(-1, barrel_doc:compare_revisions(<<"1-abc">>, <<"2-abc">>)),

    %% Same generation, compare hash
    ?assertEqual(1, barrel_doc:compare_revisions(<<"2-xyz">>, <<"2-abc">>)),
    ?assertEqual(-1, barrel_doc:compare_revisions(<<"2-abc">>, <<"2-xyz">>)),

    %% Equal
    ?assertEqual(0, barrel_doc:compare_revisions(<<"1-abc">>, <<"1-abc">>)),

    ok.

encode_revisions(_Config) ->
    Revs = [<<"3-ccc">>, <<"2-bbb">>, <<"1-aaa">>],
    Encoded = barrel_doc:encode_revisions(Revs),

    ?assertEqual(3, maps:get(<<"start">>, Encoded)),
    ?assertEqual([<<"ccc">>, <<"bbb">>, <<"aaa">>], maps:get(<<"ids">>, Encoded)),

    %% Empty list
    Empty = barrel_doc:encode_revisions([]),
    ?assertEqual(0, maps:get(<<"start">>, Empty)),
    ?assertEqual([], maps:get(<<"ids">>, Empty)),

    ok.

parse_revisions(_Config) ->
    %% With revisions format
    Doc1 = #{
        <<"revisions">> => #{
            <<"start">> => 3,
            <<"ids">> => [<<"ccc">>, <<"bbb">>, <<"aaa">>]
        }
    },
    Revs1 = barrel_doc:parse_revisions(Doc1),
    ?assertEqual([<<"3-ccc">>, <<"2-bbb">>, <<"1-aaa">>], Revs1),

    %% With just _rev
    Doc2 = #{<<"_rev">> => <<"2-xyz">>},
    Revs2 = barrel_doc:parse_revisions(Doc2),
    ?assertEqual([<<"2-xyz">>], Revs2),

    %% Empty
    Revs3 = barrel_doc:parse_revisions(#{}),
    ?assertEqual([], Revs3),

    ok.

doc_without_meta(_Config) ->
    Doc = #{
        <<"id">> => <<"doc1">>,
        <<"name">> => <<"test">>,
        <<"_rev">> => <<"1-abc">>,
        <<"_deleted">> => false,
        <<"_attachments">> => #{}
    },

    Clean = barrel_doc:doc_without_meta(Doc),

    %% id is kept (not metadata)
    ?assertEqual(<<"doc1">>, maps:get(<<"id">>, Clean)),
    ?assertEqual(<<"test">>, maps:get(<<"name">>, Clean)),

    %% Meta fields removed
    ?assertEqual(error, maps:find(<<"_rev">>, Clean)),
    ?assertEqual(error, maps:find(<<"_deleted">>, Clean)),
    ?assertEqual(error, maps:find(<<"_attachments">>, Clean)),

    ok.

make_doc_record(_Config) ->
    %% New document
    Doc1 = #{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>},
    Record1 = barrel_doc:make_doc_record(Doc1),

    ?assertEqual(<<"doc1">>, maps:get(id, Record1)),
    ?assertEqual(false, maps:get(deleted, Record1)),
    ?assertEqual(#{<<"id">> => <<"doc1">>, <<"name">> => <<"test">>}, maps:get(doc, Record1)),
    ?assert(is_reference(maps:get(ref, Record1))),

    %% First revision
    [Rev1] = maps:get(revs, Record1),
    {Gen, _Hash} = barrel_doc:parse_revision(Rev1),
    ?assertEqual(1, Gen),

    %% Update with existing rev
    Doc2 = #{<<"id">> => <<"doc1">>, <<"_rev">> => <<"1-abc">>, <<"name">> => <<"updated">>},
    Record2 = barrel_doc:make_doc_record(Doc2),

    [NewRev, OldRev] = maps:get(revs, Record2),
    ?assertEqual(<<"1-abc">>, OldRev),
    {Gen2, _} = barrel_doc:parse_revision(NewRev),
    ?assertEqual(2, Gen2),

    ok.

generate_docid(_Config) ->
    Id1 = barrel_doc:generate_docid(),
    Id2 = barrel_doc:generate_docid(),

    %% IDs are binary
    ?assert(is_binary(Id1)),
    ?assert(is_binary(Id2)),

    %% IDs are unique
    ?assertNotEqual(Id1, Id2),

    %% IDs are hex encoded
    ?assertEqual(32, byte_size(Id1)),  % MD5 = 16 bytes = 32 hex chars

    ok.

%%====================================================================
%% Test Cases - barrel_revtree
%%====================================================================

revtree_new(_Config) ->
    %% Empty tree
    Tree1 = barrel_revtree:new(),
    ?assertEqual(#{}, Tree1),

    %% Tree with initial revision
    RevInfo = #{id => <<"1-abc">>, parent => undefined, deleted => false},
    Tree2 = barrel_revtree:new(RevInfo),
    ?assertEqual(1, maps:size(Tree2)),
    ?assert(barrel_revtree:contains(<<"1-abc">>, Tree2)),

    ok.

revtree_add(_Config) ->
    %% Start with root
    Root = #{id => <<"1-aaa">>, parent => undefined, deleted => false},
    Tree1 = barrel_revtree:new(Root),

    %% Add child
    Child = #{id => <<"2-bbb">>, parent => <<"1-aaa">>, deleted => false},
    Tree2 = barrel_revtree:add(Child, Tree1),

    ?assertEqual(2, maps:size(Tree2)),
    ?assert(barrel_revtree:contains(<<"2-bbb">>, Tree2)),

    %% Can't add duplicate
    ?assertExit({badrev, already_exists}, barrel_revtree:add(Child, Tree2)),

    %% Can't add with missing parent
    Orphan = #{id => <<"3-ccc">>, parent => <<"2-xxx">>, deleted => false},
    ?assertExit({badrev, missing_parent}, barrel_revtree:add(Orphan, Tree2)),

    ok.

revtree_contains(_Config) ->
    Tree = flat_tree(),

    ?assert(barrel_revtree:contains(<<"1-one">>, Tree)),
    ?assert(barrel_revtree:contains(<<"2-two">>, Tree)),
    ?assert(barrel_revtree:contains(<<"3-three">>, Tree)),
    ?assertNot(barrel_revtree:contains(<<"4-four">>, Tree)),

    ok.

revtree_parent(_Config) ->
    Tree = flat_tree(),

    ?assertEqual(undefined, barrel_revtree:parent(<<"1-one">>, Tree)),
    ?assertEqual(<<"1-one">>, barrel_revtree:parent(<<"2-two">>, Tree)),
    ?assertEqual(<<"2-two">>, barrel_revtree:parent(<<"3-three">>, Tree)),

    %% Non-existent revision
    ?assertEqual(undefined, barrel_revtree:parent(<<"4-four">>, Tree)),

    ok.

revtree_history(_Config) ->
    Tree = flat_tree(),

    History = barrel_revtree:history(<<"3-three">>, Tree),
    ?assertEqual([<<"3-three">>, <<"2-two">>, <<"1-one">>], History),

    %% With limit
    HistoryLimit = barrel_revtree:history(<<"3-three">>, Tree, 2),
    ?assertEqual([<<"3-three">>, <<"2-two">>], HistoryLimit),

    ok.

revtree_leaves(_Config) ->
    %% Flat tree has one leaf
    Tree1 = flat_tree(),
    Leaves1 = barrel_revtree:leaves(Tree1),
    ?assertEqual([<<"3-three">>], Leaves1),

    %% Branched tree has two leaves
    Tree2 = branched_tree(),
    Leaves2 = lists:sort(barrel_revtree:leaves(Tree2)),
    ?assertEqual([<<"3-three">>, <<"3-three-2">>], Leaves2),

    ok.

revtree_is_leaf(_Config) ->
    Tree = branched_tree(),

    ?assert(barrel_revtree:is_leaf(<<"3-three">>, Tree)),
    ?assert(barrel_revtree:is_leaf(<<"3-three-2">>, Tree)),
    ?assertNot(barrel_revtree:is_leaf(<<"2-two">>, Tree)),
    ?assertNot(barrel_revtree:is_leaf(<<"1-one">>, Tree)),

    ok.

revtree_winning_revision(_Config) ->
    %% Branched tree with conflict
    Tree1 = branched_tree(),
    {WinRev1, Branched1, Conflict1} = barrel_revtree:winning_revision(Tree1),
    ?assertEqual(<<"3-three-2">>, WinRev1),  % Higher hash wins
    ?assert(Branched1),
    ?assert(Conflict1),

    %% Add a higher generation
    Rev4 = #{id => <<"4-four">>, parent => <<"3-three">>, deleted => false},
    Tree2 = barrel_revtree:add(Rev4, Tree1),
    {WinRev2, _, _} = barrel_revtree:winning_revision(Tree2),
    ?assertEqual(<<"4-four">>, WinRev2),

    %% Delete one branch
    Rev5 = #{id => <<"5-five">>, parent => <<"4-four">>, deleted => true},
    Tree3 = barrel_revtree:add(Rev5, Tree2),
    {WinRev3, Branched3, Conflict3} = barrel_revtree:winning_revision(Tree3),
    ?assertEqual(<<"3-three-2">>, WinRev3),  % Active branch wins
    ?assert(Branched3),
    ?assertNot(Conflict3),  % Only one active branch

    ok.

revtree_conflicts(_Config) ->
    Tree = branched_tree(),

    Conflicts = barrel_revtree:conflicts(Tree),
    ?assertEqual(2, length(Conflicts)),

    %% Sorted by descending {not deleted, rev}
    [First | _] = Conflicts,
    ?assertEqual(<<"3-three-2">>, maps:get(id, First)),

    ok.

revtree_prune(_Config) ->
    %% Build a longer tree
    Tree0 = flat_tree(),
    Rev4 = #{id => <<"4-four">>, parent => <<"3-three">>, deleted => false},
    Rev5 = #{id => <<"5-five">>, parent => <<"4-four">>, deleted => false},
    Tree1 = barrel_revtree:add(Rev4, Tree0),
    Tree2 = barrel_revtree:add(Rev5, Tree1),

    ?assertEqual(5, maps:size(Tree2)),

    %% Prune to keep 3 revisions
    {Pruned, Tree3} = barrel_revtree:prune(3, Tree2),
    ?assertEqual(2, Pruned),
    ?assertEqual(3, maps:size(Tree3)),

    %% Root should now be at generation 3
    ?assertNot(barrel_revtree:contains(<<"1-one">>, Tree3)),
    ?assertNot(barrel_revtree:contains(<<"2-two">>, Tree3)),
    ?assert(barrel_revtree:contains(<<"3-three">>, Tree3)),

    %% New root has no parent
    ?assertEqual(undefined, barrel_revtree:parent(<<"3-three">>, Tree3)),

    ok.

%%====================================================================
%% Test Fixtures
%%====================================================================

%% 1-one -> 2-two -> 3-three
flat_tree() ->
    #{
        <<"1-one">> => #{id => <<"1-one">>, parent => undefined, deleted => false},
        <<"2-two">> => #{id => <<"2-two">>, parent => <<"1-one">>, deleted => false},
        <<"3-three">> => #{id => <<"3-three">>, parent => <<"2-two">>, deleted => false}
    }.

%%                  3-three
%%                /
%% 1-one -> 2-two
%%                \
%%                  3-three-2
branched_tree() ->
    #{
        <<"1-one">> => #{id => <<"1-one">>, parent => undefined, deleted => false},
        <<"2-two">> => #{id => <<"2-two">>, parent => <<"1-one">>, deleted => false},
        <<"3-three">> => #{id => <<"3-three">>, parent => <<"2-two">>, deleted => false},
        <<"3-three-2">> => #{id => <<"3-three-2">>, parent => <<"2-two">>, deleted => false}
    }.
