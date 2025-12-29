%%%-------------------------------------------------------------------
%%% @doc Document path extraction for automatic indexing
%%%
%%% Extracts all paths from a document for automatic path indexing.
%%% Based on barrel_ars_view.erl from barrel apps branch.
%%%
%%% Path format: [field1, field2, ..., value]
%%% For arrays, index position is included: [field, 0, nested_field, value]
%%%
%%% Example:
%%% ```
%%% Doc = #{<<"type">> => <<"user">>,
%%%         <<"profile">> => #{<<"name">> => <<"Alice">>}},
%%% Paths = barrel_ars:analyze(Doc),
%%% %% [{[<<"type">>, <<"user">>], <<>>},
%%% %%  {[<<"profile">>, <<"name">>, <<"Alice">>], <<>>}]
%%% '''
%%%
%%% Performance optimizations:
%%% - Paths built in reverse then flipped (O(n) vs O(n^2) for ++)
%%% - Uses maps for O(1) diff lookups instead of sets
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ars).

-export([
    analyze/1,
    diff/2,
    short/1
]).

-define(MAX_VALUE_LENGTH, 100).

%%====================================================================
%% API
%%====================================================================

%% @doc Extract all paths from a document.
%% Returns a list of {Path, <<>>} tuples where Path is a list of
%% field names/indices ending with the value.
-spec analyze(map()) -> [{Path :: [term()], <<>>}].
analyze(Doc) when is_map(Doc) ->
    %% Build paths in reverse for O(n) complexity, then reverse each path
    analyze_doc(Doc, [], []);
analyze(_) ->
    [].

%% @doc Compute the difference between old and new paths.
%% Returns {Added, Removed} where:
%%   Added = paths in New but not in Old
%%   Removed = paths in Old but not in New
%%
%% Uses maps for O(1) membership tests instead of sets.
-spec diff(Old :: [{term(), term()}], New :: [{term(), term()}]) ->
    {Added :: [{term(), term()}], Removed :: [{term(), term()}]}.
diff(Old, New) ->
    %% Convert to maps for O(1) lookup
    OldMap = maps:from_list([{Path, true} || {Path, _} <- Old]),
    NewMap = maps:from_list([{Path, true} || {Path, _} <- New]),

    %% Find added: in New but not in Old
    Added = [{Path, <<>>} || {Path, _} <- New, not maps:is_key(Path, OldMap)],

    %% Find removed: in Old but not in New
    Removed = [{Path, <<>>} || {Path, _} <- Old, not maps:is_key(Path, NewMap)],

    {Added, Removed}.

%% @doc Truncate a value for indexing.
%% Binary values longer than 100 bytes are truncated.
-spec short(term()) -> term().
short(<<S:?MAX_VALUE_LENGTH/binary, _/binary>>) -> S;
short(S) when is_binary(S) -> S;
short(S) -> S.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Analyze document, building paths in reverse
-spec analyze_doc(map(), [term()], [{[term()], <<>>}]) -> [{[term()], <<>>}].
analyze_doc(Doc, RevPath, Acc) ->
    maps:fold(
        fun(K, V, Acc1) ->
            analyze_value(V, [K | RevPath], Acc1)
        end,
        Acc,
        Doc
    ).

%% @private Analyze a value (dispatch based on type)
-spec analyze_value(term(), [term()], [{[term()], <<>>}]) -> [{[term()], <<>>}].
analyze_value(V, RevPath, Acc) when is_map(V) ->
    analyze_doc(V, RevPath, Acc);
analyze_value(V, RevPath, Acc) when is_list(V) ->
    analyze_list(V, RevPath, 0, Acc);
analyze_value(V, RevPath, Acc) ->
    %% Leaf value - reverse path and add truncated value at end
    Path = lists:reverse([short(V) | RevPath]),
    [{Path, <<>>} | Acc].

%% @private Analyze a list/array with index tracking
-spec analyze_list(list(), [term()], non_neg_integer(), [{[term()], <<>>}]) ->
    [{[term()], <<>>}].
analyze_list([Item | Rest], RevPath, Index, Acc) ->
    Acc1 = analyze_value(Item, [Index | RevPath], Acc),
    analyze_list(Rest, RevPath, Index + 1, Acc1);
analyze_list([], _RevPath, _Index, Acc) ->
    Acc.
