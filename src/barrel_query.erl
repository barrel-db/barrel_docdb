%%%-------------------------------------------------------------------
%%% @doc Query compiler and executor for barrel_docdb
%%%
%%% Provides functions to compile Datalog-style query specifications
%%% into query plans, and execute them against the path index.
%%%
%%% Query Syntax:
%%% ```
%%% #{
%%%     where => [
%%%         {path, [<<"type">>], <<"user">>},           % equality
%%%         {path, [<<"org_id">>], '?Org'},              % bind variable
%%%         {compare, [<<"age">>], '>', 18},            % comparison
%%%         {'and', [...]},                              % conjunction
%%%         {'or', [...]}                                % disjunction
%%%     ],
%%%     select => ['?Org', '?Name'],   % fields/variables to return
%%%     order_by => '?Name',           % ordering
%%%     limit => 100,                   % max results
%%%     offset => 0                     % skip first N
%%% }
%%% '''
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_query).

-include("barrel_docdb.hrl").

%% API
-export([
    compile/1,
    validate_spec/1,
    execute/3,
    match/2,
    explain/1,
    extract_paths/1
]).

%% Internal exports for testing
-export([
    is_logic_var/1,
    normalize_condition/1
]).

%%====================================================================
%% Types
%%====================================================================

-type logic_var() :: atom().  % Atoms starting with '?'

-type path() :: [binary() | integer()].

-type value() :: binary() | number() | boolean() | null | logic_var().

-type compare_op() :: '>' | '<' | '>=' | '=<' | '=/=' | '=='.

-type condition() ::
    {path, path(), value()} |
    {compare, path(), compare_op(), value()} |
    {'and', [condition()]} |
    {'or', [condition()]} |
    {'not', condition()} |
    {in, path(), [value()]} |
    {contains, path(), value()} |
    {exists, path()} |
    {missing, path()} |
    {regex, path(), binary()} |
    {prefix, path(), binary()}.

-type projection() :: logic_var() | path() | '*'.

-type order_spec() :: logic_var() | path() | {logic_var() | path(), asc | desc}.

-type query_spec() :: #{
    where := [condition()],
    select => [projection()],
    order_by => order_spec() | [order_spec()],
    limit => pos_integer(),
    offset => non_neg_integer(),
    include_docs => boolean()
}.

-record(query_plan, {
    %% Normalized conditions
    conditions :: [condition()],
    %% Variables bound in conditions
    bindings :: #{logic_var() => path()},
    %% Fields/variables to project
    projections :: [projection()],
    %% Ordering specification
    order :: [{path() | logic_var(), asc | desc}],
    %% Result limit
    limit :: pos_integer() | undefined,
    %% Result offset
    offset :: non_neg_integer(),
    %% Include full documents
    include_docs :: boolean(),
    %% Index strategy hint
    strategy :: index_seek | index_scan | multi_index | full_scan
}).

-type query_plan() :: #query_plan{}.

-export_type([query_spec/0, query_plan/0, condition/0, logic_var/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Compile a query specification into a query plan.
%% Returns {ok, QueryPlan} or {error, Reason}.
-spec compile(query_spec()) -> {ok, query_plan()} | {error, term()}.
compile(Spec) when is_map(Spec) ->
    case validate_spec(Spec) of
        ok ->
            do_compile(Spec);
        {error, _} = Error ->
            Error
    end;
compile(_) ->
    {error, {invalid_spec, not_a_map}}.

%% @doc Validate a query specification.
%% Returns ok or {error, Reason}.
-spec validate_spec(query_spec()) -> ok | {error, term()}.
validate_spec(Spec) when is_map(Spec) ->
    case maps:get(where, Spec, undefined) of
        undefined ->
            {error, {missing_clause, where}};
        Where when is_list(Where) ->
            validate_conditions(Where);
        _ ->
            {error, {invalid_clause, where, must_be_list}}
    end;
validate_spec(_) ->
    {error, {invalid_spec, not_a_map}}.

%% @doc Execute a compiled query plan against a database.
%% Returns {ok, Results, LastSeq} or {error, Reason}.
-spec execute(barrel_store_rocksdb:db_ref(), db_name(), query_plan()) ->
    {ok, [map()], seq()} | {error, term()}.
execute(StoreRef, DbName, #query_plan{} = Plan) ->
    %% Execute based on strategy
    case Plan#query_plan.strategy of
        index_seek ->
            execute_index_seek(StoreRef, DbName, Plan);
        index_scan ->
            execute_index_scan(StoreRef, DbName, Plan);
        multi_index ->
            execute_multi_index(StoreRef, DbName, Plan);
        full_scan ->
            execute_full_scan(StoreRef, DbName, Plan)
    end.

%% @doc Check if a document matches a compiled query plan.
%% This is useful for filtering documents in-memory without index access.
-spec match(query_plan(), map()) -> boolean().
match(#query_plan{conditions = Conditions, bindings = Bindings}, Doc)
  when is_map(Doc) ->
    case matches_conditions(Doc, Conditions, Bindings) of
        {true, _BoundVars} -> true;
        false -> false
    end.

%% @doc Explain a query plan (for debugging/optimization).
%% Returns a map describing the execution strategy.
-spec explain(query_plan()) -> map().
explain(#query_plan{} = Plan) ->
    #{
        strategy => Plan#query_plan.strategy,
        conditions => Plan#query_plan.conditions,
        bindings => Plan#query_plan.bindings,
        projections => Plan#query_plan.projections,
        order => Plan#query_plan.order,
        limit => Plan#query_plan.limit,
        offset => Plan#query_plan.offset,
        include_docs => Plan#query_plan.include_docs
    }.

%% @doc Extract all paths referenced in a query plan.
%% Used for subscription optimization - only evaluate query when
%% a change affects one of these paths.
%% Returns MQTT-style path patterns for use with barrel_sub.
-spec extract_paths(query_plan()) -> [binary()].
extract_paths(#query_plan{conditions = Conditions}) ->
    Paths = extract_paths_from_conditions(Conditions, []),
    UniquePathPatterns = lists:usort([path_to_pattern(P) || P <- Paths]),
    UniquePathPatterns.

%% @private Extract paths from conditions recursively
extract_paths_from_conditions([], Acc) ->
    Acc;
extract_paths_from_conditions([{path, Path, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{compare, Path, _, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{'and', Nested} | Rest], Acc) ->
    NestedPaths = extract_paths_from_conditions(Nested, []),
    extract_paths_from_conditions(Rest, NestedPaths ++ Acc);
extract_paths_from_conditions([{'or', Nested} | Rest], Acc) ->
    NestedPaths = extract_paths_from_conditions(Nested, []),
    extract_paths_from_conditions(Rest, NestedPaths ++ Acc);
extract_paths_from_conditions([{'not', Condition} | Rest], Acc) ->
    NestedPaths = extract_paths_from_conditions([Condition], []),
    extract_paths_from_conditions(Rest, NestedPaths ++ Acc);
extract_paths_from_conditions([{in, Path, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{contains, Path, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{exists, Path} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{missing, Path} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{regex, Path, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([{prefix, Path, _} | Rest], Acc) ->
    extract_paths_from_conditions(Rest, [Path | Acc]);
extract_paths_from_conditions([_ | Rest], Acc) ->
    extract_paths_from_conditions(Rest, Acc).

%% @private Convert a path list to an MQTT-style pattern with # wildcard.
%% This allows matching any value at the path.
%% Example: [<<"type">>] -> <<"type/#">>
path_to_pattern([]) ->
    <<"#">>;
path_to_pattern(Path) ->
    Parts = [to_bin(P) || P <- Path],
    BasePath = iolist_to_binary(lists:join(<<"/">>, Parts)),
    <<BasePath/binary, "/#">>.

%%====================================================================
%% Internal - Compilation
%%====================================================================

do_compile(Spec) ->
    Where = maps:get(where, Spec),
    Select = maps:get(select, Spec, ['*']),
    OrderBy = maps:get(order_by, Spec, undefined),
    Limit = maps:get(limit, Spec, undefined),
    Offset = maps:get(offset, Spec, 0),
    IncludeDocs = maps:get(include_docs, Spec, false),

    %% Normalize conditions
    NormalizedConditions = [normalize_condition(C) || C <- Where],

    %% Extract variable bindings from conditions
    Bindings = extract_bindings(NormalizedConditions),

    %% Normalize projections
    Projections = normalize_projections(Select),

    %% Normalize order specification
    Order = normalize_order(OrderBy),

    %% Determine execution strategy
    Strategy = determine_strategy(NormalizedConditions),

    Plan = #query_plan{
        conditions = NormalizedConditions,
        bindings = Bindings,
        projections = Projections,
        order = Order,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs,
        strategy = Strategy
    },
    {ok, Plan}.

%% @doc Normalize a condition to canonical form
normalize_condition({path, Path, Value}) when is_list(Path) ->
    {path, Path, Value};
normalize_condition({compare, Path, Op, Value}) when is_list(Path) ->
    case lists:member(Op, ['>', '<', '>=', '=<', '=/=', '==']) of
        true -> {compare, Path, Op, Value};
        false -> {error, {invalid_operator, Op}}
    end;
normalize_condition({'and', Conditions}) when is_list(Conditions) ->
    {'and', [normalize_condition(C) || C <- Conditions]};
normalize_condition({'or', Conditions}) when is_list(Conditions) ->
    {'or', [normalize_condition(C) || C <- Conditions]};
normalize_condition({'not', Condition}) ->
    {'not', normalize_condition(Condition)};
normalize_condition({in, Path, Values}) when is_list(Path), is_list(Values) ->
    {in, Path, Values};
normalize_condition({contains, Path, Value}) when is_list(Path) ->
    {contains, Path, Value};
normalize_condition({exists, Path}) when is_list(Path) ->
    {exists, Path};
normalize_condition({missing, Path}) when is_list(Path) ->
    {missing, Path};
normalize_condition({regex, Path, Pattern}) when is_list(Path), is_binary(Pattern) ->
    {regex, Path, Pattern};
normalize_condition({prefix, Path, Prefix}) when is_list(Path), is_binary(Prefix) ->
    {prefix, Path, Prefix};
normalize_condition(Other) ->
    {error, {invalid_condition, Other}}.

%% @doc Extract variable bindings from conditions
extract_bindings(Conditions) ->
    extract_bindings(Conditions, #{}).

extract_bindings([], Acc) ->
    Acc;
extract_bindings([{path, Path, Value} | Rest], Acc) ->
    case is_logic_var(Value) of
        true ->
            extract_bindings(Rest, Acc#{Value => Path});
        false ->
            extract_bindings(Rest, Acc)
    end;
extract_bindings([{compare, Path, _Op, Value} | Rest], Acc) ->
    case is_logic_var(Value) of
        true ->
            extract_bindings(Rest, Acc#{Value => Path});
        false ->
            extract_bindings(Rest, Acc)
    end;
extract_bindings([{'and', Nested} | Rest], Acc) ->
    NestedBindings = extract_bindings(Nested, Acc),
    extract_bindings(Rest, NestedBindings);
extract_bindings([{'or', Branches} | Rest], Acc) ->
    %% For OR, we can only safely use bindings that appear in ALL branches
    %% Extract bindings from each branch and intersect them
    case Branches of
        [] ->
            extract_bindings(Rest, Acc);
        [First | RestBranches] ->
            FirstBindings = extract_bindings([First], #{}),
            CommonBindings = lists:foldl(
                fun(Branch, CommonAcc) ->
                    BranchBindings = extract_bindings([Branch], #{}),
                    maps:filter(
                        fun(Var, Path) ->
                            maps:get(Var, BranchBindings, undefined) =:= Path
                        end,
                        CommonAcc
                    )
                end,
                FirstBindings,
                RestBranches
            ),
            extract_bindings(Rest, maps:merge(Acc, CommonBindings))
    end;
extract_bindings([_ | Rest], Acc) ->
    extract_bindings(Rest, Acc).

%% @doc Check if a value is a logic variable (atom starting with '?')
-spec is_logic_var(term()) -> boolean().
is_logic_var(Atom) when is_atom(Atom) ->
    case atom_to_list(Atom) of
        [$? | _] -> true;
        _ -> false
    end;
is_logic_var(_) ->
    false.

%% @doc Normalize projections
normalize_projections(Select) when is_list(Select) ->
    Select;
normalize_projections(Single) ->
    [Single].

%% @doc Normalize order specification
normalize_order(undefined) ->
    [];
normalize_order(Spec) when is_atom(Spec); is_list(Spec), is_binary(hd(Spec)) ->
    [{Spec, asc}];
normalize_order({Spec, Dir}) when Dir =:= asc; Dir =:= desc ->
    [{Spec, Dir}];
normalize_order(Specs) when is_list(Specs) ->
    [normalize_order_item(S) || S <- Specs].

normalize_order_item({Spec, Dir}) when Dir =:= asc; Dir =:= desc ->
    {Spec, Dir};
normalize_order_item(Spec) ->
    {Spec, asc}.

%% @doc Determine the best execution strategy for the query
determine_strategy(Conditions) ->
    %% Analyze conditions to find indexed access paths
    case find_index_conditions(Conditions) of
        [] ->
            %% No index-friendly conditions - full scan
            full_scan;
        [_Single] ->
            %% Single index condition
            case has_range_condition(Conditions) of
                true -> index_scan;
                false -> index_seek
            end;
        _Multiple ->
            %% Multiple index conditions - intersection
            multi_index
    end.

%% @doc Find conditions that can use the path index
find_index_conditions(Conditions) ->
    find_index_conditions(Conditions, []).

find_index_conditions([], Acc) ->
    lists:reverse(Acc);
find_index_conditions([{path, Path, Value} | Rest], Acc) ->
    case is_logic_var(Value) of
        true ->
            %% Variable binding - can't use for initial index lookup
            find_index_conditions(Rest, Acc);
        false ->
            %% Concrete value - good for index
            find_index_conditions(Rest, [{path, Path, Value} | Acc])
    end;
find_index_conditions([{compare, Path, _Op, _Value} | Rest], Acc) ->
    %% Range comparison - can use index scan
    find_index_conditions(Rest, [{compare, Path} | Acc]);
find_index_conditions([{prefix, Path, _Prefix} | Rest], Acc) ->
    %% Prefix match - can use index scan
    find_index_conditions(Rest, [{prefix, Path} | Acc]);
find_index_conditions([{'and', Nested} | Rest], Acc) ->
    NestedIndexable = find_index_conditions(Nested),
    find_index_conditions(Rest, NestedIndexable ++ Acc);
find_index_conditions([_ | Rest], Acc) ->
    find_index_conditions(Rest, Acc).

%% @doc Check if conditions include range comparisons
has_range_condition([]) -> false;
has_range_condition([{compare, _, Op, _} | _]) when Op =/= '==' -> true;
has_range_condition([{prefix, _, _} | _]) -> true;
has_range_condition([{'and', Nested} | Rest]) ->
    has_range_condition(Nested) orelse has_range_condition(Rest);
has_range_condition([{'or', Nested} | Rest]) ->
    has_range_condition(Nested) orelse has_range_condition(Rest);
has_range_condition([_ | Rest]) ->
    has_range_condition(Rest).

%%====================================================================
%% Internal - Validation
%%====================================================================

validate_conditions([]) ->
    ok;
validate_conditions([Condition | Rest]) ->
    case validate_condition(Condition) of
        ok -> validate_conditions(Rest);
        {error, _} = Error -> Error
    end.

validate_condition({path, Path, _Value}) ->
    validate_path(Path);
validate_condition({compare, Path, Op, _Value}) ->
    case lists:member(Op, ['>', '<', '>=', '=<', '=/=', '==']) of
        true -> validate_path(Path);
        false -> {error, {invalid_operator, Op}}
    end;
validate_condition({'and', Conditions}) when is_list(Conditions) ->
    validate_conditions(Conditions);
validate_condition({'or', Conditions}) when is_list(Conditions) ->
    validate_conditions(Conditions);
validate_condition({'not', Condition}) ->
    validate_condition(Condition);
validate_condition({in, Path, Values}) when is_list(Values) ->
    validate_path(Path);
validate_condition({in, _Path, _}) ->
    {error, {invalid_in_values, must_be_list}};
validate_condition({contains, Path, _Value}) ->
    validate_path(Path);
validate_condition({exists, Path}) ->
    validate_path(Path);
validate_condition({missing, Path}) ->
    validate_path(Path);
validate_condition({regex, Path, Pattern}) when is_binary(Pattern) ->
    case re:compile(Pattern) of
        {ok, _} -> validate_path(Path);
        {error, Reason} -> {error, {invalid_regex, Reason}}
    end;
validate_condition({regex, _Path, _}) ->
    {error, {invalid_regex, must_be_binary}};
validate_condition({prefix, Path, Prefix}) when is_binary(Prefix) ->
    validate_path(Path);
validate_condition({prefix, _Path, _}) ->
    {error, {invalid_prefix, must_be_binary}};
validate_condition(Other) ->
    {error, {invalid_condition, Other}}.

validate_path(Path) when is_list(Path) ->
    case lists:all(fun is_valid_path_component/1, Path) of
        true -> ok;
        false -> {error, {invalid_path, Path}}
    end;
validate_path(Path) ->
    {error, {invalid_path, Path, must_be_list}}.

is_valid_path_component(C) when is_binary(C) -> true;
is_valid_path_component(C) when is_integer(C), C >= 0 -> true;
is_valid_path_component('*') -> true;  % Wildcard for array any
is_valid_path_component(_) -> false.

%%====================================================================
%% Internal - Execution
%%====================================================================

%% @doc Execute using direct index key lookup (fastest)
execute_index_seek(StoreRef, DbName, Plan) ->
    #query_plan{conditions = Conditions} = Plan,

    %% Find the first equality condition to use for index lookup
    case find_first_equality(Conditions) of
        {ok, {path, Path, Value}} ->
            %% Build full path with value for exact lookup
            FullPath = Path ++ [Value],
            DocIds = collect_docids_for_path(StoreRef, DbName, FullPath),

            %% Filter by remaining conditions and apply projections
            filter_and_project(StoreRef, DbName, DocIds, Plan);
        not_found ->
            %% Fallback to scan
            execute_index_scan(StoreRef, DbName, Plan)
    end.

%% @doc Execute using index prefix scan
execute_index_scan(StoreRef, DbName, Plan) ->
    #query_plan{conditions = Conditions} = Plan,

    %% Find best path prefix for scanning
    case find_best_scan_path(Conditions) of
        {ok, Path} ->
            DocIds = collect_docids_for_prefix(StoreRef, DbName, Path),
            filter_and_project(StoreRef, DbName, DocIds, Plan);
        not_found ->
            execute_full_scan(StoreRef, DbName, Plan)
    end.

%% @doc Execute using multiple index lookups with intersection
execute_multi_index(StoreRef, DbName, Plan) ->
    #query_plan{conditions = Conditions} = Plan,

    %% Find all indexable conditions
    IndexConditions = find_all_equality_conditions(Conditions),

    case IndexConditions of
        [] ->
            execute_full_scan(StoreRef, DbName, Plan);
        _ when length(IndexConditions) =< 2 ->
            %% For 1-2 conditions, skip cardinality lookup (overhead not worth it)
            [First | Rest] = IndexConditions,
            {path, Path1, Value1} = First,
            FullPath1 = Path1 ++ [Value1],
            InitialDocIds = collect_docids_for_path(StoreRef, DbName, FullPath1),
            FinalDocIds = intersect_conditions(StoreRef, DbName, Rest, InitialDocIds),
            filter_and_project(StoreRef, DbName, FinalDocIds, Plan);
        _ ->
            %% For 3+ conditions, order by cardinality (smallest first)
            %% Also short-circuit if any condition has 0 cardinality
            case order_by_cardinality(StoreRef, DbName, IndexConditions) of
                [] ->
                    %% All conditions have 0 cardinality - no matches possible
                    filter_and_project(StoreRef, DbName, [], Plan);
                [First | Rest] ->
                    %% Start with smallest cardinality condition
                    {path, Path1, Value1} = First,
                    FullPath1 = Path1 ++ [Value1],
                    InitialDocIds = collect_docids_for_path(StoreRef, DbName, FullPath1),
                    FinalDocIds = intersect_conditions(StoreRef, DbName, Rest, InitialDocIds),
                    filter_and_project(StoreRef, DbName, FinalDocIds, Plan)
            end
    end.

%% @doc Order conditions by cardinality (smallest first) for optimal intersection.
%% Returns empty list if any condition has 0 cardinality (short-circuit).
order_by_cardinality(StoreRef, DbName, Conditions) ->
    %% Build keys for all conditions
    Keys = [barrel_store_keys:path_stats_key(DbName, Path ++ [Value])
            || {path, Path, Value} <- Conditions],

    %% Batch fetch all cardinalities with multi_get
    Results = barrel_store_rocksdb:multi_get(StoreRef, Keys),

    %% Parse results and associate with conditions
    WithCardinality = lists:zipwith(
        fun(Cond, Result) ->
            Count = case Result of
                {ok, CountBin} -> max(0, binary_to_integer(CountBin));
                not_found -> 0
            end,
            {Count, Cond}
        end,
        Conditions, Results
    ),

    %% Check for any zero cardinality (short-circuit)
    case lists:any(fun({0, _}) -> true; (_) -> false end, WithCardinality) of
        true ->
            [];
        false ->
            %% Sort by cardinality ascending and extract conditions
            Sorted = lists:keysort(1, WithCardinality),
            [Cond || {_, Cond} <- Sorted]
    end.

%% @doc Intersect doc IDs from multiple conditions with short-circuit
intersect_conditions(_StoreRef, _DbName, _Conditions, []) ->
    %% Short-circuit: empty accumulator means no matches possible
    [];
intersect_conditions(_StoreRef, _DbName, [], AccDocIds) ->
    AccDocIds;
intersect_conditions(StoreRef, DbName, [{path, Path, Value} | Rest], AccDocIds) ->
    FullPath = Path ++ [Value],
    CondDocIds = collect_docids_for_path(StoreRef, DbName, FullPath),
    case CondDocIds of
        [] ->
            %% Short-circuit: this condition has no matches
            [];
        _ ->
            Intersection = sorted_intersection(AccDocIds, CondDocIds),
            intersect_conditions(StoreRef, DbName, Rest, Intersection)
    end.

%% @doc Merge-based intersection of two sorted lists - O(n+m)
sorted_intersection([], _) -> [];
sorted_intersection(_, []) -> [];
sorted_intersection([H | T1], [H | T2]) ->
    [H | sorted_intersection(T1, T2)];
sorted_intersection([H1 | T1], [H2 | _] = L2) when H1 < H2 ->
    sorted_intersection(T1, L2);
sorted_intersection(L1, [_ | T2]) ->
    sorted_intersection(L1, T2).

%% @doc Execute full document scan (slowest, last resort)
execute_full_scan(StoreRef, DbName, Plan) ->
    %% Collect all doc IDs by scanning doc_info keys
    DocIds = barrel_store_rocksdb:fold_range(
        StoreRef,
        barrel_store_keys:doc_info_prefix(DbName),
        barrel_store_keys:doc_info_end(DbName),
        fun(Key, _Value, Acc) ->
            DocId = barrel_store_keys:decode_doc_info_key(DbName, Key),
            {ok, [DocId | Acc]}
        end,
        []
    ),

    filter_and_project(StoreRef, DbName, DocIds, Plan).

%% @doc Collect document IDs matching an exact path+value
%% Returns sorted list by using reverse iteration with prepend
collect_docids_for_path(StoreRef, DbName, FullPath) ->
    barrel_ars_index:fold_path_reverse(
        StoreRef, DbName, FullPath,
        fun({_Path, DocId}, Acc) -> {ok, [DocId | Acc]} end,
        []
    ).

%% @doc Collect document IDs matching a path prefix
collect_docids_for_prefix(StoreRef, DbName, PathPrefix) ->
    barrel_ars_index:fold_path(
        StoreRef, DbName, PathPrefix,
        fun({_Path, DocId}, Acc) -> {ok, [DocId | Acc]} end,
        []
    ).

%% @doc Filter results by remaining conditions and apply projections
filter_and_project(StoreRef, DbName, DocIds, Plan) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        order = Order,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs
    } = Plan,

    %% Remove duplicates
    UniqueDocIds = lists:usort(DocIds),

    %% Batch fetch documents using multi_get
    Results0 = batch_fetch_and_filter(StoreRef, DbName, UniqueDocIds,
                                       Conditions, Bindings, Projections, IncludeDocs),

    %% Apply ordering
    Results1 = apply_order(Results0, Order),

    %% Apply offset and limit
    Results2 = apply_offset_limit(Results1, Offset, Limit),

    %% Get last sequence (for consistency tracking)
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results2, LastSeq}.

%% @doc Batch fetch documents using multi_get for better performance
batch_fetch_and_filter(StoreRef, DbName, DocIds, Conditions, Bindings, Projections, IncludeDocs) ->
    case DocIds of
        [] -> [];
        _ ->
            %% Step 1: Batch fetch all doc_info keys
            DocInfoKeys = [barrel_store_keys:doc_info(DbName, Id) || Id <- DocIds],
            DocInfoResults = barrel_store_rocksdb:multi_get(StoreRef, DocInfoKeys),

            %% Step 2: Filter deleted docs, collect doc_rev keys for non-deleted
            {ActiveDocs, RevKeys} = lists:foldl(
                fun({DocId, Result}, {AccDocs, AccKeys}) ->
                    case Result of
                        {ok, DocInfoBin} ->
                            DocInfo = binary_to_term(DocInfoBin),
                            case maps:get(deleted, DocInfo, false) of
                                true ->
                                    {AccDocs, AccKeys};
                                false ->
                                    Rev = maps:get(rev, DocInfo),
                                    RevKey = barrel_store_keys:doc_rev(DbName, DocId, Rev),
                                    {[DocId | AccDocs], [RevKey | AccKeys]}
                            end;
                        not_found ->
                            {AccDocs, AccKeys};
                        {error, _} ->
                            {AccDocs, AccKeys}
                    end
                end,
                {[], []},
                lists:zip(DocIds, DocInfoResults)
            ),

            %% Step 3: Batch fetch all doc bodies
            case RevKeys of
                [] -> [];
                _ ->
                    %% Reverse to maintain order correspondence
                    ReversedDocIds = lists:reverse(ActiveDocs),
                    ReversedRevKeys = lists:reverse(RevKeys),
                    DocBodyResults = barrel_store_rocksdb:multi_get(StoreRef, ReversedRevKeys),

                    %% Step 4: Match bodies with doc IDs and filter by conditions
                    lists:filtermap(
                        fun({DocId, BodyResult}) ->
                            case BodyResult of
                                {ok, DocBin} ->
                                    Doc = binary_to_term(DocBin),
                                    case matches_conditions(Doc, Conditions, Bindings) of
                                        {true, BoundVars} ->
                                            Result = project_result(Doc, DocId, Projections, BoundVars, IncludeDocs),
                                            {true, Result};
                                        false ->
                                            false
                                    end;
                                not_found ->
                                    false;
                                {error, _} ->
                                    false
                            end
                        end,
                        lists:zip(ReversedDocIds, DocBodyResults)
                    )
            end
    end.

%% @doc Check if a document matches all conditions
matches_conditions(Doc, Conditions, InitialBindings) ->
    matches_conditions(Doc, Conditions, InitialBindings, #{}).

matches_conditions(_Doc, [], _Bindings, BoundVars) ->
    {true, BoundVars};
matches_conditions(Doc, [Condition | Rest], Bindings, BoundVars) ->
    case match_condition(Doc, Condition, Bindings, BoundVars) of
        {true, NewBoundVars} ->
            matches_conditions(Doc, Rest, Bindings, NewBoundVars);
        false ->
            false
    end.

match_condition(Doc, {path, Path, Value}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} ->
            case is_logic_var(Value) of
                true ->
                    %% Bind the variable
                    {true, BoundVars#{Value => DocValue}};
                false ->
                    case DocValue =:= Value of
                        true -> {true, BoundVars};
                        false -> false
                    end
            end;
        not_found ->
            false
    end;

match_condition(Doc, {compare, Path, Op, Value}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} ->
            CompareValue = case is_logic_var(Value) of
                true -> maps:get(Value, BoundVars, undefined);
                false -> Value
            end,
            case compare_values(DocValue, Op, CompareValue) of
                true -> {true, BoundVars};
                false -> false
            end;
        not_found ->
            false
    end;

match_condition(Doc, {'and', Conditions}, Bindings, BoundVars) ->
    matches_conditions(Doc, Conditions, Bindings, BoundVars);

match_condition(Doc, {'or', Conditions}, Bindings, BoundVars) ->
    match_any(Doc, Conditions, Bindings, BoundVars);

match_condition(Doc, {'not', Condition}, Bindings, BoundVars) ->
    case match_condition(Doc, Condition, Bindings, BoundVars) of
        {true, _} -> false;
        false -> {true, BoundVars}
    end;

match_condition(Doc, {in, Path, Values}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} ->
            case lists:member(DocValue, Values) of
                true -> {true, BoundVars};
                false -> false
            end;
        not_found ->
            false
    end;

match_condition(Doc, {contains, Path, Value}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} when is_list(DocValue) ->
            case lists:member(Value, DocValue) of
                true -> {true, BoundVars};
                false -> false
            end;
        _ ->
            false
    end;

match_condition(Doc, {exists, Path}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, _} -> {true, BoundVars};
        not_found -> false
    end;

match_condition(Doc, {missing, Path}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, _} -> false;
        not_found -> {true, BoundVars}
    end;

match_condition(Doc, {regex, Path, Pattern}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} when is_binary(DocValue) ->
            case re:run(DocValue, Pattern) of
                {match, _} -> {true, BoundVars};
                nomatch -> false
            end;
        _ ->
            false
    end;

match_condition(Doc, {prefix, Path, Prefix}, _Bindings, BoundVars) ->
    case get_path_value(Doc, Path) of
        {ok, DocValue} when is_binary(DocValue) ->
            PrefixLen = byte_size(Prefix),
            case DocValue of
                <<Prefix:PrefixLen/binary, _/binary>> -> {true, BoundVars};
                _ -> false
            end;
        _ ->
            false
    end;

match_condition(_Doc, {error, _}, _Bindings, _BoundVars) ->
    false.

match_any(_Doc, [], _Bindings, _BoundVars) ->
    false;
match_any(Doc, [Condition | Rest], Bindings, BoundVars) ->
    case match_condition(Doc, Condition, Bindings, BoundVars) of
        {true, NewBoundVars} -> {true, NewBoundVars};
        false -> match_any(Doc, Rest, Bindings, BoundVars)
    end.

%% @doc Get a value from a document at the given path
get_path_value(Doc, []) ->
    {ok, Doc};
get_path_value(Doc, [Key | Rest]) when is_map(Doc), is_binary(Key) ->
    case maps:find(Key, Doc) of
        {ok, Value} -> get_path_value(Value, Rest);
        error -> not_found
    end;
get_path_value(Doc, [Index | Rest]) when is_list(Doc), is_integer(Index) ->
    case Index < length(Doc) of
        true ->
            Value = lists:nth(Index + 1, Doc),  % 0-based to 1-based
            get_path_value(Value, Rest);
        false ->
            not_found
    end;
get_path_value(_, _) ->
    not_found.

%% @doc Compare two values with an operator
compare_values(A, '>', B) when is_number(A), is_number(B) -> A > B;
compare_values(A, '<', B) when is_number(A), is_number(B) -> A < B;
compare_values(A, '>=', B) when is_number(A), is_number(B) -> A >= B;
compare_values(A, '=<', B) when is_number(A), is_number(B) -> A =< B;
compare_values(A, '=/=', B) -> A =/= B;
compare_values(A, '==', B) -> A =:= B;
compare_values(A, '>', B) when is_binary(A), is_binary(B) -> A > B;
compare_values(A, '<', B) when is_binary(A), is_binary(B) -> A < B;
compare_values(A, '>=', B) when is_binary(A), is_binary(B) -> A >= B;
compare_values(A, '=<', B) when is_binary(A), is_binary(B) -> A =< B;
compare_values(_, _, _) -> false.

%% @doc Project result fields from document
project_result(Doc, DocId, Projections, BoundVars, IncludeDocs) ->
    Result0 = #{<<"id">> => DocId},

    Result1 = case IncludeDocs of
        true -> Result0#{<<"doc">> => Doc};
        false -> Result0
    end,

    %% Add projected fields/variables
    lists:foldl(
        fun('*', Acc) ->
            %% Include all bound variables
            maps:fold(fun(Var, Val, A) ->
                VarName = atom_to_binary(Var, utf8),
                A#{VarName => Val}
            end, Acc, BoundVars);
           (Var, Acc) when is_atom(Var) ->
            case is_logic_var(Var) of
                true ->
                    VarName = atom_to_binary(Var, utf8),
                    case maps:find(Var, BoundVars) of
                        {ok, Val} -> Acc#{VarName => Val};
                        error -> Acc
                    end;
                false ->
                    Acc
            end;
           (Path, Acc) when is_list(Path) ->
            case get_path_value(Doc, Path) of
                {ok, Val} ->
                    PathKey = path_to_key(Path),
                    Acc#{PathKey => Val};
                not_found ->
                    Acc
            end
        end,
        Result1,
        Projections
    ).

path_to_key(Path) ->
    iolist_to_binary(lists:join(<<"/">>, [to_bin(P) || P <- Path])).

to_bin(B) when is_binary(B) -> B;
to_bin(N) when is_integer(N) -> integer_to_binary(N).

%% @doc Apply ordering to results
apply_order(Results, []) ->
    Results;
apply_order(Results, [{Field, Dir} | _Rest]) ->
    %% Simple single-field ordering for now
    Sorted = lists:sort(
        fun(A, B) ->
            ValA = get_sort_value(A, Field),
            ValB = get_sort_value(B, Field),
            case Dir of
                asc -> ValA =< ValB;
                desc -> ValA >= ValB
            end
        end,
        Results
    ),
    Sorted.

get_sort_value(Result, Field) when is_atom(Field) ->
    FieldKey = atom_to_binary(Field, utf8),
    maps:get(FieldKey, Result, null);
get_sort_value(Result, Path) when is_list(Path) ->
    PathKey = path_to_key(Path),
    maps:get(PathKey, Result, null).

%% @doc Apply offset and limit
apply_offset_limit(Results, Offset, Limit) ->
    Results1 = case Offset > 0 of
        true -> lists:nthtail(min(Offset, length(Results)), Results);
        false -> Results
    end,
    case Limit of
        undefined -> Results1;
        N -> lists:sublist(Results1, N)
    end.

%% @doc Find first equality condition
find_first_equality([]) ->
    not_found;
find_first_equality([{path, Path, Value} | _]) when not is_atom(Value) ->
    {ok, {path, Path, Value}};
find_first_equality([{path, Path, Value} | Rest]) ->
    case is_logic_var(Value) of
        false -> {ok, {path, Path, Value}};
        true -> find_first_equality(Rest)
    end;
find_first_equality([{'and', Nested} | Rest]) ->
    case find_first_equality(Nested) of
        {ok, _} = Result -> Result;
        not_found -> find_first_equality(Rest)
    end;
find_first_equality([_ | Rest]) ->
    find_first_equality(Rest).

%% @doc Find all equality conditions
find_all_equality_conditions(Conditions) ->
    find_all_equality_conditions(Conditions, []).

find_all_equality_conditions([], Acc) ->
    lists:reverse(Acc);
find_all_equality_conditions([{path, Path, Value} | Rest], Acc) ->
    case is_logic_var(Value) of
        false ->
            find_all_equality_conditions(Rest, [{path, Path, Value} | Acc]);
        true ->
            find_all_equality_conditions(Rest, Acc)
    end;
find_all_equality_conditions([{'and', Nested} | Rest], Acc) ->
    NestedEqs = find_all_equality_conditions(Nested),
    find_all_equality_conditions(Rest, NestedEqs ++ Acc);
find_all_equality_conditions([_ | Rest], Acc) ->
    find_all_equality_conditions(Rest, Acc).

%% @doc Find best path for scanning
find_best_scan_path([]) ->
    not_found;
find_best_scan_path([{path, Path, _} | _]) ->
    {ok, Path};
find_best_scan_path([{compare, Path, _, _} | _]) ->
    {ok, Path};
find_best_scan_path([{prefix, Path, _} | _]) ->
    {ok, Path};
find_best_scan_path([{'and', Nested} | Rest]) ->
    case find_best_scan_path(Nested) of
        {ok, _} = Result -> Result;
        not_found -> find_best_scan_path(Rest)
    end;
find_best_scan_path([_ | Rest]) ->
    find_best_scan_path(Rest).
