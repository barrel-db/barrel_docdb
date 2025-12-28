%%%-------------------------------------------------------------------
%%% @doc Revision tree implementation for barrel_docdb
%%%
%%% Provides MVCC revision tree operations for document versioning.
%%% The tree stores revision info with parent pointers and supports
%%% branching for conflict handling.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_revtree).

-include("barrel_docdb.hrl").

%% Tree operations
-export([
    new/0,
    new/1,
    add/2,
    contains/2,
    info/2,
    revisions/1
]).

%% Navigation
-export([
    parent/2,
    history/2,
    history/3
]).

%% Leaf operations
-export([
    fold_leafs/3,
    leaves/1,
    is_leaf/2
]).

%% Conflict handling
-export([
    missing_revs/2,
    conflicts/1,
    winning_revision/1,
    is_deleted/1
]).

%% Pruning
-export([
    prune/2,
    prune/3
]).

-define(DEFAULT_MAX_HISTORY, 200).
-define(IMAX1, 16#ffffFFFFffffFFFF).

%%====================================================================
%% Tree Operations
%%====================================================================

%% @doc Create an empty revision tree
-spec new() -> revtree().
new() -> #{}.

%% @doc Create a revision tree with initial revision
-spec new(rev_info()) -> revtree().
new(#{id := Id} = RevInfo) ->
    #{Id => RevInfo}.

%% @doc Add a revision to the tree
-spec add(rev_info(), revtree()) -> revtree().
add(RevInfo, Tree) ->
    #{id := Id, parent := Parent} = RevInfo,
    case maps:is_key(Id, Tree) of
        true -> exit({badrev, already_exists});
        false -> ok
    end,
    case maps:is_key(Parent, Tree) of
        true -> ok;
        false when Parent =:= undefined -> ok;
        false -> exit({badrev, missing_parent})
    end,
    Tree#{Id => RevInfo}.

%% @doc Check if revision exists in tree
-spec contains(revid(), revtree()) -> boolean().
contains(RevId, Tree) ->
    maps:is_key(RevId, Tree).

%% @doc Get revision info
-spec info(revid(), revtree()) -> {ok, rev_info()} | {error, not_found}.
info(RevId, Tree) ->
    case maps:find(RevId, Tree) of
        {ok, RevInfo} -> {ok, RevInfo};
        error -> {error, not_found}
    end.

%% @doc Get all revisions in tree
-spec revisions(revtree()) -> [revid()].
revisions(Tree) ->
    maps:keys(Tree).

%%====================================================================
%% Navigation
%%====================================================================

%% @doc Get parent revision
-spec parent(revid(), revtree()) -> revid() | undefined.
parent(RevId, Tree) ->
    case maps:find(RevId, Tree) of
        {ok, RevInfo} -> maps:get(parent, RevInfo, undefined);
        error -> undefined
    end.

%% @doc Get revision history (ancestors)
-spec history(revid(), revtree()) -> [revid()].
history(RevId, Tree) ->
    history(RevId, Tree, ?DEFAULT_MAX_HISTORY).

%% @doc Get revision history with limit
-spec history(revid(), revtree(), non_neg_integer()) -> [revid()].
history(RevId, Tree, Max) ->
    history_loop(maps:get(RevId, Tree, nil), Tree, [], Max).

history_loop(nil, _Tree, History, _Max) ->
    lists:reverse(History);
history_loop(#{id := Id, parent := Parent}, Tree, History, Max) ->
    NewHistory = [Id | History],
    Len = length(NewHistory),
    if
        Len >= Max ->
            lists:reverse(NewHistory);
        true ->
            history_loop(maps:get(Parent, Tree, nil), Tree, NewHistory, Max)
    end;
history_loop(#{id := Id}, _Tree, History, _Max) ->
    lists:reverse([Id | History]).

%%====================================================================
%% Leaf Operations
%%====================================================================

%% @doc Fold over leaf revisions (revisions with no children)
-spec fold_leafs(fun((rev_info(), term()) -> term()), term(), revtree()) -> term().
fold_leafs(Fun, AccIn, Tree) ->
    %% Find all revisions that are parents
    Parents = maps:fold(
        fun
            (_Id, #{parent := Parent}, Acc) when Parent /= undefined ->
                [Parent | Acc];
            (_Id, _RevInfo, Acc) ->
                Acc
        end,
        [],
        Tree
    ),
    %% Leaves are revisions that are not parents
    LeafsMap = maps:without(Parents, Tree),
    maps:fold(fun(_RevId, RevInfo, Acc) -> Fun(RevInfo, Acc) end, AccIn, LeafsMap).

%% @doc Get all leaf revisions
-spec leaves(revtree()) -> [revid()].
leaves(Tree) ->
    fold_leafs(fun(#{id := RevId}, Acc) -> [RevId | Acc] end, [], Tree).

%% @doc Check if revision is a leaf (has no children)
-spec is_leaf(revid(), revtree()) -> boolean().
is_leaf(RevId, Tree) ->
    case maps:is_key(RevId, Tree) of
        true -> is_leaf_loop(maps:values(Tree), RevId);
        false -> false
    end.

is_leaf_loop([#{parent := RevId} | _], RevId) -> false;
is_leaf_loop([_ | Rest], RevId) -> is_leaf_loop(Rest, RevId);
is_leaf_loop([], _) -> true.

%%====================================================================
%% Conflict Handling
%%====================================================================

%% @doc Find missing revisions not in tree
-spec missing_revs([revid()], revtree()) -> [revid()].
missing_revs(Revs, RevTree) ->
    Leaves = fold_leafs(
        fun(#{id := Id}, Acc) ->
            case lists:member(Id, Revs) of
                true -> [Id | Acc];
                false -> Acc
            end
        end,
        [],
        RevTree
    ),
    Revs -- Leaves.

%% @doc Check if revision is deleted
-spec is_deleted(rev_info() | revtree()) -> boolean().
is_deleted(#{deleted := Del}) -> Del;
is_deleted(_) -> false.

%% @doc Get sorted conflict revisions
-spec conflicts(revtree()) -> [rev_info()].
conflicts(Tree) ->
    Leaves = fold_leafs(
        fun(RevInfo, Acc) ->
            Deleted = is_deleted(RevInfo),
            [RevInfo#{deleted => Deleted} | Acc]
        end,
        [],
        Tree
    ),
    lists:sort(
        fun(#{id := RevIdA, deleted := DeletedA}, #{id := RevIdB, deleted := DeletedB}) ->
            RevA = barrel_doc:parse_revision(RevIdA),
            RevB = barrel_doc:parse_revision(RevIdB),
            {not DeletedA, RevA} > {not DeletedB, RevB}
        end,
        Leaves
    ).

%% @doc Find winning revision
%% Returns {WinningRev, Branched, Conflict}
-spec winning_revision(revtree()) -> {revid(), boolean(), boolean()}.
winning_revision(Tree) ->
    {Leaves, ActiveCount} = fold_leafs(
        fun(RevInfo, {Acc, Count}) ->
            Deleted = is_deleted(RevInfo),
            NewCount = case Deleted of
                true -> Count;
                false -> Count + 1
            end,
            {[RevInfo#{deleted => Deleted} | Acc], NewCount}
        end,
        {[], 0},
        Tree
    ),
    SortedRevInfos = lists:sort(
        fun(#{id := RevIdA, deleted := DeletedA}, #{id := RevIdB, deleted := DeletedB}) ->
            RevA = barrel_doc:parse_revision(RevIdA),
            RevB = barrel_doc:parse_revision(RevIdB),
            {not DeletedA, RevA} > {not DeletedB, RevB}
        end,
        Leaves
    ),
    [#{id := WinningRev} | _] = SortedRevInfos,
    Branched = length(Leaves) > 1,
    Conflict = ActiveCount > 1,
    {WinningRev, Branched, Conflict}.

%%====================================================================
%% Pruning
%%====================================================================

%% @doc Prune old revisions from tree
-spec prune(non_neg_integer(), revtree()) -> {non_neg_integer(), revtree()}.
prune(Depth, Tree) ->
    prune(Depth, undefined, Tree).

%% @doc Prune old revisions, keeping at least KeepRev
-spec prune(non_neg_integer(), revid() | undefined, revtree()) -> {non_neg_integer(), revtree()}.
prune(Depth, KeepRev, Tree) ->
    Sz = maps:size(Tree),
    if
        Sz =< Depth -> {0, Tree};
        true -> do_prune(Depth, KeepRev, Tree)
    end.

do_prune(Depth, KeepRev, Tree) ->
    {MinPos0, MaxDeletedPos} = fold_leafs(
        fun(#{id := RevId} = RevInfo, {MP, MDP}) ->
            Deleted = is_deleted(RevInfo),
            {Pos, _} = barrel_doc:parse_revision(RevId),
            case Deleted of
                true when Pos > MDP -> {MP, Pos};
                _ when Pos > 0, Pos < MP -> {Pos, MDP};
                _ -> {MP, MDP}
            end
        end,
        {?IMAX1, 0},
        Tree
    ),
    MinPos = if
        MinPos0 =:= ?IMAX1 -> MaxDeletedPos;
        true -> MinPos0
    end,
    MinPosToKeep0 = MinPos - Depth + 1,
    {PosToKeep, _} = case KeepRev of
        undefined -> {0, <<>>};
        _ -> barrel_doc:parse_revision(KeepRev)
    end,
    MinPosToKeep = if
        PosToKeep > 0, PosToKeep < MinPosToKeep0 -> PosToKeep;
        true -> MinPosToKeep0
    end,
    if
        MinPosToKeep > 1 ->
            maps:fold(
                fun(RevId, RevInfo, {N, NewTree}) ->
                    {Pos, _} = barrel_doc:parse_revision(RevId),
                    if
                        Pos < MinPosToKeep ->
                            {N + 1, maps:remove(RevId, NewTree)};
                        Pos =:= MinPosToKeep ->
                            RevInfo2 = RevInfo#{parent => undefined},
                            {N, NewTree#{RevId => RevInfo2}};
                        true ->
                            {N, NewTree}
                    end
                end,
                {0, Tree},
                Tree
            );
        true ->
            {0, Tree}
    end.
