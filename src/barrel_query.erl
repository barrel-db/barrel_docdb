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
    execute/4,
    match/2,
    explain/1,
    extract_paths/1
]).

%% Internal exports for testing
-export([
    is_logic_var/1,
    normalize_condition/1
]).

%% Profiling (temporary)
-export([get_profile/0, reset_profile/0, dump_profile/0]).

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
    include_docs => boolean(),
    doc_format => binary | map | json,
    decoder_fun => fun((binary()) -> term())
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
    %% Document format for include_docs: map (default) | binary | json
    doc_format :: binary | map | json,
    %% Custom decoder function for documents
    decoder_fun :: undefined | fun((binary()) -> term()),
    %% Index strategy hint
    strategy :: index_seek | index_scan | multi_index | full_scan
}).

-type query_plan() :: #query_plan{}.

%% Chunked execution options
-type chunk_opts() :: #{
    chunk_size => pos_integer(),          %% Max results per chunk (default 1000)
    continuation => binary() | undefined, %% Resume from previous chunk
    eventual_consistency => boolean()     %% Skip snapshot for fresh reads (default false)
}.

%% Chunked execution result metadata
-type result_meta() :: #{
    last_seq := seq(),
    has_more := boolean(),
    continuation => binary()  %% Only present if has_more = true
}.

-export_type([query_spec/0, query_plan/0, condition/0, logic_var/0, chunk_opts/0, result_meta/0]).

%% Read profile type from storage layer
-type read_profile() :: barrel_store_rocksdb:read_profile().

%%====================================================================
%% Chunked Execution Constants
%%====================================================================

%% Target: ~1MB of doc data per chunk for good cache utilization
-define(TARGET_CHUNK_BYTES, 1048576).
-define(MIN_CHUNK_SIZE, 50).
-define(MAX_CHUNK_SIZE, 1000).
-define(INITIAL_CHUNK_SIZE, 200).

%%====================================================================
%% Adaptive Strategy Constants
%%====================================================================

%% Threshold for choosing between scan vs posting list strategies:
%% - Below threshold: use value_index scan (per-doc keys, early termination)
%% - Above threshold: use posting list decode (one seek, bulk decode)
-define(ADAPTIVE_CARDINALITY_THRESHOLD, 1000).

%% Maximum unbounded results before forcing pagination
%% For include_docs=true queries without limit, auto-paginate at this size
-define(MAX_UNBOUNDED_RESULTS, 1000).

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
%% Uses a snapshot for read consistency across all index and document reads.
%%
%% For unbounded include_docs queries, automatically paginates internally
%% to avoid excessive memory usage from large MultiGet operations.
-spec execute(barrel_store_rocksdb:db_ref(), db_name(), query_plan()) ->
    {ok, [map()], seq()} | {error, term()}.
execute(StoreRef, DbName, #query_plan{include_docs = true, limit = undefined} = Plan) ->
    %% Unbounded include_docs query - auto-paginate to avoid memory issues
    execute_with_auto_pagination(StoreRef, DbName, Plan);
execute(StoreRef, DbName, #query_plan{} = Plan) ->
    %% Bounded query or pure index query - execute directly
    {ok, Snapshot} = barrel_store_rocksdb:snapshot(StoreRef),
    try
        execute_with_snapshot(StoreRef, DbName, Plan, Snapshot)
    after
        barrel_store_rocksdb:release_snapshot(Snapshot)
    end.

%% @private Execute unbounded include_docs query with automatic pagination
%% Collects results in chunks to avoid memory pressure from large MultiGet
execute_with_auto_pagination(StoreRef, DbName, Plan) ->
    execute_auto_paginated_loop(StoreRef, DbName, Plan, undefined, []).

%% @private Direct execution bypassing auto-pagination
%% Used when chunked execution falls back to non-chunked for needs_body queries
execute_direct(StoreRef, DbName, Plan) ->
    {ok, Snapshot} = barrel_store_rocksdb:snapshot(StoreRef),
    try
        execute_with_snapshot(StoreRef, DbName, Plan, Snapshot)
    after
        barrel_store_rocksdb:release_snapshot(Snapshot)
    end.

%% @private Auto-pagination loop for unbounded include_docs queries
execute_auto_paginated_loop(StoreRef, DbName, Plan, Continuation, AccResults) ->
    Opts = case Continuation of
        undefined -> #{chunk_size => ?MAX_UNBOUNDED_RESULTS};
        Token -> #{chunk_size => ?MAX_UNBOUNDED_RESULTS, continuation => Token}
    end,
    case execute(StoreRef, DbName, Plan, Opts) of
        {ok, ChunkResults, #{has_more := false, last_seq := LastSeq}} ->
            %% Final chunk - combine all results
            AllResults = AccResults ++ ChunkResults,
            {ok, AllResults, LastSeq};
        {ok, ChunkResults, #{has_more := true, continuation := NextToken}} ->
            %% More results available - continue loop
            execute_auto_paginated_loop(StoreRef, DbName, Plan, NextToken, AccResults ++ ChunkResults);
        {error, _} = Error ->
            Error
    end.

%% @doc Execute a compiled query plan with chunked execution options.
%% Returns {ok, Results, ResultMeta} where ResultMeta includes:
%%   - last_seq: sequence number for change tracking
%%   - has_more: true if more results available
%%   - continuation: opaque token to resume (only if has_more = true)
%%
%% Options:
%%   - chunk_size: max results per chunk (default 1000)
%%   - continuation: resume token from previous call
%%   - eventual_consistency: if true, each chunk sees current data (default false)
%%
%% Example:
%% ```
%% %% First chunk
%% {ok, R1, #{has_more := true, continuation := Token}} =
%%     barrel_query:execute(Store, Db, Plan, #{chunk_size => 100}),
%%
%% %% Next chunk
%% {ok, R2, #{has_more := false}} =
%%     barrel_query:execute(Store, Db, Plan, #{continuation => Token}).
%% '''
-spec execute(barrel_store_rocksdb:db_ref(), db_name(), query_plan(), chunk_opts()) ->
    {ok, [map()], result_meta()} | {error, term()}.
execute(StoreRef, DbName, #query_plan{} = Plan, Opts) when is_map(Opts) ->
    case maps:get(continuation, Opts, undefined) of
        undefined ->
            %% Fresh query - create new snapshot
            execute_chunked_fresh(StoreRef, DbName, Plan, Opts);
        Token ->
            %% Resume from cursor
            execute_chunked_resume(StoreRef, DbName, Plan, Token, Opts)
    end.

%%====================================================================
%% Chunked Execution Implementation
%%====================================================================

%% @private Execute fresh chunked query (no continuation token)
execute_chunked_fresh(StoreRef, DbName, Plan, Opts) ->
    ChunkSize = maps:get(chunk_size, Opts, 1000),
    EventualConsistency = maps:get(eventual_consistency, Opts, false),

    %% Create snapshot for consistent reads (unless eventual consistency)
    Snapshot = case EventualConsistency of
        true -> undefined;
        false ->
            {ok, S} = barrel_store_rocksdb:snapshot(StoreRef),
            S
    end,

    try
        execute_chunked_with_snapshot(StoreRef, DbName, Plan, ChunkSize, undefined, Snapshot)
    catch
        Class:Reason:Stack ->
            %% Release snapshot on error
            maybe_release_snapshot(Snapshot),
            erlang:raise(Class, Reason, Stack)
    end.

%% @private Resume chunked query from cursor
execute_chunked_resume(StoreRef, DbName, Plan, Token, Opts) ->
    case barrel_query_cursor:lookup(Token) of
        {ok, Cursor} ->
            %% Get cursor fields
            #{snapshot := Snapshot, last_key := LastKey, query_type := QueryType} = cursor_to_map(Cursor),
            ChunkSize = maps:get(chunk_size, Opts, 1000),

            %% Validate cursor matches current query
            case validate_cursor_for_plan(QueryType, Plan) of
                ok ->
                    execute_chunked_with_snapshot(StoreRef, DbName, Plan, ChunkSize, LastKey, Snapshot);
                {error, _} = Error ->
                    barrel_query_cursor:release(Token),
                    Error
            end;
        {error, expired} ->
            {error, cursor_expired};
        {error, not_found} ->
            {error, cursor_not_found}
    end.

%% @private Execute chunked query with snapshot
execute_chunked_with_snapshot(StoreRef, DbName, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{conditions = Conditions} = Plan,

    %% Classify query type for pure index execution
    QueryType = classify_scan_query(Conditions, Plan),

    case QueryType of
        {pure_equality, Path, Value} ->
            execute_pure_equality_chunked(StoreRef, DbName, Path, Value, Plan, ChunkSize, StartKey, Snapshot);
        {pure_exists, Path} ->
            execute_pure_exists_chunked(StoreRef, DbName, Path, Plan, ChunkSize, StartKey, Snapshot);
        {pure_prefix, Path, Prefix} ->
            execute_pure_prefix_chunked(StoreRef, DbName, Path, Prefix, Plan, ChunkSize, StartKey, Snapshot);
        {pure_compare, Path, Op, Value} ->
            execute_pure_compare_chunked(StoreRef, DbName, Path, Op, Value, Plan, ChunkSize, StartKey, Snapshot);
        {multi_index, IndexConditions} ->
            execute_multi_index_chunked(StoreRef, DbName, IndexConditions, Plan, ChunkSize, StartKey, Snapshot);
        needs_body ->
            %% For body-fetch queries, fall back to non-chunked for now
            %% TODO: Implement chunked body-fetch queries
            %% Note: We call execute_direct to avoid infinite loop with auto-pagination
            maybe_release_snapshot(Snapshot),
            {ok, Results, LastSeq} = execute_direct(StoreRef, DbName, Plan),
            {ok, Results, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Execute chunked pure equality query
execute_pure_equality_chunked(StoreRef, DbName, Path, Value, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset} = Plan,

    %% Determine start key for iteration
    ActualStartKey = case StartKey of
        undefined ->
            barrel_store_keys:value_index_prefix(DbName, Value, Path);
        Key ->
            %% Resume after the last key
            <<Key/binary, 0>>
    end,
    EndKey = barrel_store_keys:value_index_end(DbName, Value, Path),
    PrefixLen = byte_size(barrel_store_keys:value_index_prefix(DbName, Value, Path)),

    %% Collect ChunkSize + 1 to detect if there are more results
    %% Track the last key of the ChunkSize-th result separately
    MaxCollect = ChunkSize + 1,
    FoldFun = fun(Key, _Val, {Count, ChunkLastK, Acc}) ->
        <<_:PrefixLen/binary, DocId/binary>> = Key,
        NewCount = Count + 1,
        if
            NewCount > MaxCollect ->
                %% Already collected enough, stop
                {stop, {Count, ChunkLastK, Acc}};
            NewCount =:= ChunkSize ->
                %% This is the ChunkSize-th result - save its key for cursor
                {ok, {NewCount, Key, [#{<<"id">> => DocId} | Acc]}};
            NewCount > ChunkSize ->
                %% Extra result to detect has_more - don't update ChunkLastK
                {ok, {NewCount, ChunkLastK, [#{<<"id">> => DocId} | Acc]}};
            true ->
                {ok, {NewCount, ChunkLastK, [#{<<"id">> => DocId} | Acc]}}
        end
    end,

    {CollectedCount, ChunkLastKey, Results} =
        case Snapshot of
            undefined ->
                barrel_store_rocksdb:fold_range(
                    StoreRef, ActualStartKey, EndKey, FoldFun, {0, undefined, []});
            _ ->
                barrel_store_rocksdb:fold_range_with_snapshot(
                    StoreRef, ActualStartKey, EndKey, FoldFun, {0, undefined, []}, Snapshot)
        end,

    %% Check if we have more results
    HasMore = CollectedCount > ChunkSize,

    %% Take only ChunkSize results (drop the extra one if present)
    FinalResults0 = case HasMore of
        true -> tl(Results);  %% Drop the extra one (results are reversed)
        false -> Results
    end,
    FinalResults = apply_offset_limit(lists:reverse(FinalResults0), Offset, ChunkSize),

    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore of
        true ->
            %% Create cursor with the key of the last result we're returning
            Token = barrel_query_cursor:create(
                StoreRef, DbName, pure_equality, ChunkLastKey, Snapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            %% No more results - release snapshot
            maybe_release_snapshot(Snapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Execute chunked pure exists query
execute_pure_exists_chunked(StoreRef, DbName, Path, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset} = Plan,

    %% Determine start key for iteration
    ActualStartKey = case StartKey of
        undefined ->
            barrel_store_keys:path_posting_prefix(DbName, Path);
        Key ->
            <<Key/binary, 0>>
    end,
    EndKey = barrel_store_keys:path_posting_end(DbName, Path),

    %% Collect ChunkSize + 1 to detect if there are more
    MaxCollect = ChunkSize + 1,

    %% Fold over posting lists
    FoldResult = barrel_store_rocksdb:fold_range_posting_with_snapshot(
        StoreRef, ActualStartKey, EndKey,
        fun(Key, DocIds, {Seen, Count, LastK, Acc}) ->
            process_exists_docids_chunked(DocIds, Key, Seen, Count, LastK, Acc, MaxCollect)
        end,
        {#{}, 0, undefined, []},
        case Snapshot of undefined -> barrel_store_rocksdb:snapshot(StoreRef); S -> {ok, S} end
    ),

    {_, CollectedCount, LastCollectedKey, Results} = FoldResult,

    HasMore = CollectedCount > ChunkSize,
    FinalResults0 = case HasMore of
        true -> tl(Results);
        false -> Results
    end,
    FinalResults = apply_offset_limit(lists:reverse(FinalResults0), Offset, ChunkSize),

    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore of
        true ->
            Token = barrel_query_cursor:create(
                StoreRef, DbName, pure_exists, LastCollectedKey, Snapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            maybe_release_snapshot(Snapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Execute chunked pure prefix query
execute_pure_prefix_chunked(StoreRef, DbName, Path, Prefix, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset} = Plan,

    %% Build prefix range
    FullPath = Path ++ [Prefix],
    ActualStartKey = case StartKey of
        undefined ->
            barrel_store_keys:path_posting_prefix(DbName, FullPath);
        Key ->
            <<Key/binary, 0>>
    end,

    %% End key for prefix scan: path + prefix + 0xFF
    EndKey = barrel_store_keys:path_posting_end(DbName, FullPath),

    MaxCollect = ChunkSize + 1,

    FoldResult = barrel_store_rocksdb:fold_range_posting_with_snapshot(
        StoreRef, ActualStartKey, EndKey,
        fun(Key, DocIds, {Seen, Count, LastK, Acc}) ->
            process_prefix_docids_chunked(DocIds, Key, Seen, Count, LastK, Acc, MaxCollect)
        end,
        {#{}, 0, undefined, []},
        case Snapshot of undefined -> barrel_store_rocksdb:snapshot(StoreRef); S -> {ok, S} end
    ),

    {_, CollectedCount, LastCollectedKey, Results} = FoldResult,

    HasMore = CollectedCount > ChunkSize,
    FinalResults0 = case HasMore of
        true -> tl(Results);
        false -> Results
    end,
    FinalResults = apply_offset_limit(lists:reverse(FinalResults0), Offset, ChunkSize),

    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore of
        true ->
            Token = barrel_query_cursor:create(
                StoreRef, DbName, pure_prefix, LastCollectedKey, Snapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            maybe_release_snapshot(Snapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Process DocIds for chunked exists query
process_exists_docids_chunked([], _Key, Seen, Count, LastKey, Acc, _MaxCollect) ->
    {ok, {Seen, Count, LastKey, Acc}};
process_exists_docids_chunked([DocId | Rest], Key, Seen, Count, _LastKey, Acc, MaxCollect) ->
    case maps:is_key(DocId, Seen) of
        true ->
            process_exists_docids_chunked(Rest, Key, Seen, Count, Key, Acc, MaxCollect);
        false ->
            NewSeen = Seen#{DocId => true},
            NewCount = Count + 1,
            NewAcc = [#{<<"id">> => DocId} | Acc],
            case NewCount >= MaxCollect of
                true ->
                    {stop, {NewSeen, NewCount, Key, NewAcc}};
                false ->
                    process_exists_docids_chunked(Rest, Key, NewSeen, NewCount, Key, NewAcc, MaxCollect)
            end
    end.

%% @private Process DocIds for chunked prefix query
process_prefix_docids_chunked([], _Key, Seen, Count, LastKey, Acc, _MaxCollect) ->
    {ok, {Seen, Count, LastKey, Acc}};
process_prefix_docids_chunked([DocId | Rest], Key, Seen, Count, _LastKey, Acc, MaxCollect) ->
    case maps:is_key(DocId, Seen) of
        true ->
            process_prefix_docids_chunked(Rest, Key, Seen, Count, Key, Acc, MaxCollect);
        false ->
            NewSeen = Seen#{DocId => true},
            NewCount = Count + 1,
            NewAcc = [#{<<"id">> => DocId} | Acc],
            case NewCount >= MaxCollect of
                true ->
                    {stop, {NewSeen, NewCount, Key, NewAcc}};
                false ->
                    process_prefix_docids_chunked(Rest, Key, NewSeen, NewCount, Key, NewAcc, MaxCollect)
            end
    end.

%% @private Execute chunked pure compare query (range scan)
execute_pure_compare_chunked(StoreRef, DbName, Path, Op, Value, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset} = Plan,

    %% Compute start/end keys for the range based on operator
    {RangeStart, RangeEnd} = compare_range_keys(DbName, Path, Op, Value),

    %% Use provided StartKey for pagination, otherwise use range start
    ActualStartKey = case StartKey of
        undefined -> RangeStart;
        Key -> <<Key/binary, 0>>
    end,

    MaxCollect = ChunkSize + 1,

    %% Get snapshot if not already present
    ActualSnapshot = case Snapshot of
        undefined ->
            {ok, S} = barrel_store_rocksdb:snapshot(StoreRef),
            S;
        S -> S
    end,

    FoldResult = barrel_store_rocksdb:fold_range_posting_with_snapshot(
        StoreRef, ActualStartKey, RangeEnd,
        fun(Key, DocIds, {Seen, Count, LastK, Acc}) ->
            process_compare_docids_chunked(DocIds, Key, Seen, Count, LastK, Acc, MaxCollect)
        end,
        {#{}, 0, undefined, []},
        {ok, ActualSnapshot}
    ),

    {_, CollectedCount, LastCollectedKey, Results} = FoldResult,

    HasMore = CollectedCount > ChunkSize,
    FinalResults0 = case HasMore of
        true -> tl(Results);
        false -> Results
    end,
    FinalResults = apply_offset_limit(lists:reverse(FinalResults0), Offset, ChunkSize),

    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore of
        true ->
            Token = barrel_query_cursor:create(
                StoreRef, DbName, pure_compare, LastCollectedKey, ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            maybe_release_snapshot(ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Compute range keys for compare operators
compare_range_keys(DbName, Path, '>', Value) ->
    %% > Value: start AFTER value, end at path end
    StartKey = barrel_store_keys:path_posting_end(DbName, Path ++ [Value]),
    EndKey = barrel_store_keys:path_posting_end(DbName, Path),
    {StartKey, EndKey};
compare_range_keys(DbName, Path, '>=', Value) ->
    %% >= Value: start AT value, end at path end
    StartKey = barrel_store_keys:path_posting_prefix(DbName, Path ++ [Value]),
    EndKey = barrel_store_keys:path_posting_end(DbName, Path),
    {StartKey, EndKey};
compare_range_keys(DbName, Path, '<', Value) ->
    %% < Value: start at path start, end BEFORE value
    StartKey = barrel_store_keys:path_posting_prefix(DbName, Path),
    EndKey = barrel_store_keys:path_posting_prefix(DbName, Path ++ [Value]),
    {StartKey, EndKey};
compare_range_keys(DbName, Path, '=<', Value) ->
    %% =< Value: start at path start, end AFTER value
    StartKey = barrel_store_keys:path_posting_prefix(DbName, Path),
    EndKey = barrel_store_keys:path_posting_end(DbName, Path ++ [Value]),
    {StartKey, EndKey}.

%% @private Process DocIds for chunked compare query
process_compare_docids_chunked([], _Key, Seen, Count, LastKey, Acc, _MaxCollect) ->
    {ok, {Seen, Count, LastKey, Acc}};
process_compare_docids_chunked([DocId | Rest], Key, Seen, Count, _LastKey, Acc, MaxCollect) ->
    case maps:is_key(DocId, Seen) of
        true ->
            process_compare_docids_chunked(Rest, Key, Seen, Count, Key, Acc, MaxCollect);
        false ->
            NewSeen = Seen#{DocId => true},
            NewCount = Count + 1,
            NewAcc = [#{<<"id">> => DocId} | Acc],
            case NewCount >= MaxCollect of
                true ->
                    {stop, {NewSeen, NewCount, Key, NewAcc}};
                false ->
                    process_compare_docids_chunked(Rest, Key, NewSeen, NewCount, Key, NewAcc, MaxCollect)
            end
    end.

%% @private Execute chunked multi-index query (intersection of posting lists)
execute_multi_index_chunked(StoreRef, DbName, Conditions, Plan, ChunkSize, StartKey, Snapshot) ->
    #query_plan{offset = Offset} = Plan,

    %% Get snapshot if not already present
    ActualSnapshot = case Snapshot of
        undefined ->
            {ok, S} = barrel_store_rocksdb:snapshot(StoreRef),
            S;
        S -> S
    end,

    %% Collect DocIds using bitmap-accelerated intersection
    IntersectedDocIds = intersect_docid_sets(StoreRef, DbName, Conditions),

    %% Apply pagination using StartKey
    PaginatedDocIds = case StartKey of
        undefined -> IntersectedDocIds;
        Key ->
            %% Skip until we pass the start key
            lists:dropwhile(fun(DocId) -> DocId =< Key end, IntersectedDocIds)
    end,

    %% Collect ChunkSize + 1 for has_more detection
    MaxCollect = ChunkSize + 1,
    {CollectedDocIds, HasMore} = case length(PaginatedDocIds) > MaxCollect of
        true ->
            {lists:sublist(PaginatedDocIds, MaxCollect), true};
        false ->
            {PaginatedDocIds, false}
    end,

    %% Build results
    Results0 = [#{<<"id">> => DocId} || DocId <- CollectedDocIds],
    FinalResults0 = case HasMore of
        true -> lists:droplast(Results0);
        false -> Results0
    end,
    FinalResults = apply_offset_limit(FinalResults0, Offset, ChunkSize),

    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    case HasMore of
        true ->
            LastDocId = lists:last(CollectedDocIds),
            Token = barrel_query_cursor:create(
                StoreRef, DbName, multi_index, LastDocId, ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => true, continuation => Token}};
        false ->
            maybe_release_snapshot(ActualSnapshot),
            {ok, FinalResults, #{last_seq => LastSeq, has_more => false}}
    end.

%% @private Collect DocIds for a single condition using index
collect_condition_docids(StoreRef, DbName, Cond) ->
    collect_condition_docids(StoreRef, DbName, Cond, infinity).

collect_condition_docids(StoreRef, DbName, {path, Path, Value}, infinity) ->
    FullPath = Path ++ [Value],
    barrel_ars_index:get_posting_list(StoreRef, DbName, FullPath);
collect_condition_docids(StoreRef, DbName, {path, Path, Value}, MaxCount) when is_integer(MaxCount) ->
    FullPath = Path ++ [Value],
    AllDocIds = barrel_ars_index:get_posting_list(StoreRef, DbName, FullPath),
    lists:sublist(AllDocIds, MaxCount);
collect_condition_docids(StoreRef, DbName, {exists, Path}, infinity) ->
    %% Collect all DocIds that have this path
    barrel_ars_index:fold_path(StoreRef, DbName, Path,
        fun({_P, DocId}, Acc) -> {ok, [DocId | Acc]} end, [], short_range);
collect_condition_docids(StoreRef, DbName, {exists, Path}, MaxCount) when is_integer(MaxCount) ->
    Profile = select_read_profile(MaxCount),
    {_, DocIds} = barrel_ars_index:fold_path(StoreRef, DbName, Path,
        fun({_P, DocId}, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false -> {ok, {Count + 1, [DocId | Acc]}}
            end
        end, {0, []}, Profile),
    DocIds;
collect_condition_docids(StoreRef, DbName, {prefix, Path, Prefix}, infinity) ->
    barrel_ars_index:fold_prefix(StoreRef, DbName, Path, Prefix,
        fun({_P, DocId}, Acc) -> {ok, [DocId | Acc]} end, [], short_range);
collect_condition_docids(StoreRef, DbName, {prefix, Path, Prefix}, MaxCount) when is_integer(MaxCount) ->
    Profile = select_read_profile(MaxCount),
    {_, DocIds} = barrel_ars_index:fold_prefix(StoreRef, DbName, Path, Prefix,
        fun({_P, DocId}, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false -> {ok, {Count + 1, [DocId | Acc]}}
            end
        end, {0, []}, Profile),
    DocIds;
collect_condition_docids(StoreRef, DbName, {compare, Path, Op, Value}, infinity) ->
    barrel_ars_index:fold_path_values_compare(StoreRef, DbName, Path, Op, Value,
        fun({_P, DocId}, Acc) -> {ok, [DocId | Acc]} end, []);
collect_condition_docids(StoreRef, DbName, {compare, Path, Op, Value}, MaxCount) when is_integer(MaxCount) ->
    {_, DocIds} = barrel_ars_index:fold_path_values_compare(StoreRef, DbName, Path, Op, Value,
        fun({_P, DocId}, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false -> {ok, {Count + 1, [DocId | Acc]}}
            end
        end, {0, []}),
    DocIds.

%% @private Intersect multiple DocId sets using optimized intersection
%% First collects from most selective condition, then filters against others
intersect_docid_sets(StoreRef, DbName, Conditions) ->
    %% Find the most selective condition (smallest posting list)
    {SmallestCond, _SmallestPath} = find_most_selective_condition(StoreRef, DbName, Conditions),

    %% Get docids from the smallest posting list
    DocIds = collect_condition_docids(StoreRef, DbName, SmallestCond),

    %% Verify remaining conditions using posting lists
    OtherConditions = Conditions -- [SmallestCond],
    verify_conditions(StoreRef, DbName, DocIds, OtherConditions).

%% @private Find the most selective condition based on cardinality
find_most_selective_condition(StoreRef, DbName, Conditions) ->
    ConditionsWithCard = lists:map(fun(Cond) ->
        {Card, Path} = case Cond of
            {path, P, V} ->
                FullPath = P ++ [V],
                case barrel_ars_index:get_path_cardinality(StoreRef, DbName, FullPath) of
                    {ok, C} -> {C, FullPath};
                    _ -> {999999999, FullPath}
                end;
            {exists, P} ->
                case barrel_ars_index:get_path_cardinality(StoreRef, DbName, P) of
                    {ok, C} -> {C, P};
                    _ -> {999999999, P}
                end;
            {compare, P, Op, Value} ->
                %% Estimate compare cardinality by sampling the index
                %% For '>' or '<', estimate roughly half the values match
                %% This is a heuristic - actual count would require full scan
                case barrel_ars_index:get_path_cardinality(StoreRef, DbName, P) of
                    {ok, TotalCard} ->
                        %% Assume 50% selectivity for range queries
                        EstCard = case Op of
                            '>' -> TotalCard div 2;
                            '<' -> TotalCard div 2;
                            '>=' -> (TotalCard div 2) + 1;
                            '=<' -> (TotalCard div 2) + 1
                        end,
                        {EstCard, P ++ [Value]};
                    _ ->
                        {999999998, P}
                end;
            {prefix, P, Prefix} ->
                %% Estimate prefix matches ~10% of path values
                case barrel_ars_index:get_path_cardinality(StoreRef, DbName, P) of
                    {ok, TotalCard} -> {TotalCard div 10, P ++ [Prefix]};
                    _ -> {999999997, P}
                end;
            _ ->
                {999999999, []}
        end,
        {Card, Path, Cond}
    end, Conditions),
    [{_, BestPath, BestCond} | _] = lists:sort(ConditionsWithCard),
    {BestCond, BestPath}.

%% @private Verify that docids match remaining conditions using posting lists
%% Uses point lookups for equality conditions (O(1) per docid) and set membership
%% for other condition types (O(m) to collect, then O(1) lookups).
verify_conditions(_StoreRef, _DbName, DocIds, []) ->
    lists:usort(DocIds);
verify_conditions(StoreRef, DbName, DocIds, [{path, Path, Value} | Rest]) ->
    %% Use point lookup for equality - O(1) per docid via value-first index
    FilteredDocIds = lists:filter(fun(DocId) ->
        barrel_ars_index:docid_has_value(StoreRef, DbName, Path, Value, DocId)
    end, DocIds),
    verify_conditions(StoreRef, DbName, FilteredDocIds, Rest);
verify_conditions(StoreRef, DbName, DocIds, [Cond | Rest]) ->
    %% For other conditions (compare, exists, prefix), use set-based approach
    CondDocIds = sets:from_list(collect_condition_docids(StoreRef, DbName, Cond)),
    FilteredDocIds = lists:filter(fun(DocId) ->
        sets:is_element(DocId, CondDocIds)
    end, DocIds),
    verify_conditions(StoreRef, DbName, FilteredDocIds, Rest).

%% @private Release snapshot if present
maybe_release_snapshot(undefined) -> ok;
maybe_release_snapshot(Snapshot) ->
    catch barrel_store_rocksdb:release_snapshot(Snapshot),
    ok.

%% @private Convert cursor record to map for pattern matching
cursor_to_map(Cursor) ->
    %% Cursor is a record, extract fields
    #{
        snapshot => element(5, Cursor),    %% #cursor.snapshot
        last_key => element(6, Cursor),    %% #cursor.last_key
        query_type => element(7, Cursor)   %% #cursor.query_type
    }.

%% @private Validate cursor matches current query plan
validate_cursor_for_plan(_QueryType, _Plan) ->
    %% For now, just accept - could add stricter validation
    ok.

%% @doc Execute query with a snapshot for read consistency
execute_with_snapshot(StoreRef, DbName, Plan, Snapshot) ->
    #query_plan{order = Order, limit = Limit, conditions = Conditions} = Plan,

    %% Check if we can use indexed order for ORDER BY + LIMIT optimization
    %% Most beneficial when: no filter conditions, small limit, large dataset
    case can_use_indexed_order(Order, Limit) of
        {true, OrderPath, Dir} ->
            case should_use_indexed_order(OrderPath, Conditions) of
                true ->
                    execute_with_indexed_order(StoreRef, DbName, OrderPath, Dir, Plan, Snapshot);
                false ->
                    execute_by_strategy(StoreRef, DbName, Plan, Snapshot)
            end;
        false ->
            execute_by_strategy(StoreRef, DbName, Plan, Snapshot)
    end.

%% @doc Execute based on query strategy
execute_by_strategy(StoreRef, DbName, Plan, Snapshot) ->
    case Plan#query_plan.strategy of
        index_seek ->
            execute_index_seek(StoreRef, DbName, Plan, Snapshot);
        index_scan ->
            execute_index_scan(StoreRef, DbName, Plan, Snapshot);
        multi_index ->
            execute_multi_index(StoreRef, DbName, Plan, Snapshot);
        full_scan ->
            execute_full_scan(StoreRef, DbName, Plan, Snapshot)
    end.

%% @doc Decide if indexed order should be used
%% The Top-K optimization is most beneficial when:
%% - No filter conditions (pure ORDER BY + LIMIT)
%% - This avoids sorting all documents
%% With filter conditions, the standard path with early limit + batch fetch is often faster
should_use_indexed_order(_OrderPath, []) ->
    %% No conditions - pure ORDER BY + LIMIT, definitely use indexed order
    %% This is the main use case: "get latest N" without filtering
    true;
should_use_indexed_order(_OrderPath, _Conditions) ->
    %% With filter conditions, the standard path is usually better because:
    %% 1. It uses batch fetching (multi_get)
    %% 2. The early limit optimization already helps
    %% 3. Filter conditions typically reduce the result set significantly
    false.

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
    %% Default doc_format is 'map' for backwards compatibility
    DocFormat = maps:get(doc_format, Spec, map),

    %% Convert doc_format to decoder_fun at compile time for simpler execution
    DecoderFun = case maps:get(decoder_fun, Spec, undefined) of
        undefined -> make_decoder_fun(DocFormat);
        CustomFun -> CustomFun
    end,

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
        doc_format = DocFormat,
        decoder_fun = DecoderFun,
        strategy = Strategy
    },
    {ok, Plan}.

%% @doc Create a decoder function from doc_format option
-spec make_decoder_fun(binary | map | json) -> fun((binary()) -> term()).
make_decoder_fun(binary) -> fun(Bin) -> Bin end;
make_decoder_fun(map) -> fun barrel_docdb_codec_cbor:decode/1;
make_decoder_fun(json) -> fun barrel_docdb_codec_cbor:to_json/1.

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
find_index_conditions([{exists, Path} | Rest], Acc) ->
    %% Exists check - can use index scan on path prefix
    find_index_conditions(Rest, [{exists, Path} | Acc]);
find_index_conditions([{'and', Nested} | Rest], Acc) ->
    NestedIndexable = find_index_conditions(Nested),
    find_index_conditions(Rest, NestedIndexable ++ Acc);
find_index_conditions([_ | Rest], Acc) ->
    find_index_conditions(Rest, Acc).

%% @doc Check if conditions include range comparisons
has_range_condition([]) -> false;
has_range_condition([{compare, _, Op, _} | _]) when Op =/= '==' -> true;
has_range_condition([{prefix, _, _} | _]) -> true;
has_range_condition([{exists, _} | _]) -> true;
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

%% @doc Select read profile based on limit and cardinality.
%% - point: Small result sets (limit <= 10), keep blocks in cache
%% - short_range: Medium scans (limit <= 200 or cardinality <= 200), auto-readahead
%% - long_scan: Large/unbounded scans, prefetch aggressively, avoid cache pollution
-spec select_read_profile(undefined | pos_integer(), non_neg_integer()) -> read_profile().
select_read_profile(Limit, _Cardinality) when is_integer(Limit), Limit =< 10 ->
    point;
select_read_profile(Limit, _Cardinality) when is_integer(Limit), Limit =< 200 ->
    short_range;
select_read_profile(_Limit, Cardinality) when Cardinality =< 200 ->
    short_range;
select_read_profile(_, _) ->
    long_scan.

%% @doc Select read profile based on limit only (for collectors)
select_read_profile(MaxCount) when is_integer(MaxCount), MaxCount =< 10 ->
    point;
select_read_profile(MaxCount) when is_integer(MaxCount), MaxCount =< 200 ->
    short_range;
select_read_profile(_) ->
    long_scan.

%% @doc Execute using direct index key lookup (fastest)
%% Uses prefix bloom filters for O(1) skip of non-matching SST blocks.
execute_index_seek(StoreRef, DbName, Plan, Snapshot) ->
    #query_plan{conditions = Conditions, include_docs = IncludeDocs,
                projections = Projections} = Plan,

    %% Find the first equality condition to use for index lookup
    case find_first_equality(Conditions) of
        {ok, {path, Path, Value} = IndexCond} ->
            FullPath = Path ++ [Value],

            %% Compute remaining conditions (index condition already satisfied by iteration)
            RemainingConds = Conditions -- [IndexCond],

            %% Check for pure equality: single condition, no doc body needed
            NeedsBody = IncludeDocs orelse needs_body_for_projection(Projections),
            case {RemainingConds, NeedsBody} of
                {[], false} ->
                    %% Pure equality - return DocIds directly from posting list
                    execute_pure_equality(StoreRef, DbName, Path, Value, Plan, Snapshot);
                _ ->
                    %% Need doc fetch - original path
                    execute_index_seek_with_fetch(StoreRef, DbName, FullPath, RemainingConds, Plan, Snapshot)
            end;
        not_found ->
            %% Fallback to scan
            execute_index_scan(StoreRef, DbName, Plan, Snapshot)
    end.

%% @doc Execute index seek that requires document fetching
execute_index_seek_with_fetch(StoreRef, DbName, FullPath, RemainingConds, Plan, Snapshot) ->
    #query_plan{order = Order, limit = Limit, offset = Offset} = Plan,

    %% O(1) cardinality check - skip iteration if no matches
    Cardinality = case barrel_ars_index:get_path_cardinality(StoreRef, DbName, FullPath) of
        {ok, C} -> C;
        {error, _} -> 1  %% Assume at least 1 if error
    end,
    case Cardinality of
        0 ->
            %% No matches - skip iteration entirely
            filter_and_project(StoreRef, DbName, [], Plan, Snapshot);
        _ ->
            %% Index condition already satisfied - use remaining conditions only
            FilterPlan = Plan#query_plan{conditions = RemainingConds},
            case Limit of
                undefined ->
                    %% Unbounded query - use adaptive chunked execution
                    execute_index_seek_chunked(StoreRef, DbName, FullPath, FilterPlan, Snapshot);
                _ when is_integer(Limit) ->
                    %% Limited query - check streaming vs batch
                    case should_use_streaming(Limit, Cardinality) of
                        true ->
                            %% Streaming: fetch/match documents one-by-one
                            execute_index_seek_streaming(StoreRef, DbName, FullPath, FilterPlan, Snapshot);
                        false ->
                            %% Batch: collect DocIds then fetch/filter
                            %% PROFILING: Index iteration
                            T0 = erlang:monotonic_time(microsecond),
                            EarlyLimitResult = can_use_early_limit(Order, RemainingConds, Limit),
                            DocIds = case EarlyLimitResult of
                                {true, MaxCollect} ->
                                    collect_docids_for_path_limited(StoreRef, DbName, FullPath, MaxCollect + Offset);
                                false ->
                                    collect_docids_for_path(StoreRef, DbName, FullPath)
                            end,
                            T1 = erlang:monotonic_time(microsecond),
                            put(profile_index_iter, pdict_get(profile_index_iter, 0) + (T1 - T0)),
                            put(profile_doc_count, length(DocIds)),
                            filter_and_project(StoreRef, DbName, DocIds, FilterPlan, Snapshot)
                    end
            end
    end.

%% @doc Execute index seek using streaming approach
%% Iterates index entries and fetches/matches documents one-by-one
%% Stops early when enough results are collected
%% Much faster than batch-fetch for small limits on high-cardinality indexes
execute_index_seek_streaming(StoreRef, DbName, FullPath, Plan, _Snapshot) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs,
        decoder_fun = _DecoderFun
    } = Plan,

    %% Calculate how many results we need
    %% Use 1.5x to account for deleted docs (less than batch approach)
    MaxCollect = round((Limit + Offset) * 1.5),

    %% Iterate index entries and fetch/match documents one-by-one
    %% Use point profile since streaming is for small limits
    {_Count, Results} = barrel_ars_index:fold_path_reverse(
        StoreRef, DbName, FullPath,
        fun({_Path, DocId}, {Count, Acc}) ->
            case Count >= MaxCollect of
                true ->
                    {stop, {Count, Acc}};
                false ->
                    %% Fetch and filter document
                    case fetch_and_match_doc(StoreRef, DbName, DocId, Conditions, Bindings, IncludeDocs) of
                        {ok, Doc, BoundVars} ->
                            Result = project_result(Doc, DocId, Projections, BoundVars, IncludeDocs),
                            {ok, {Count + 1, [Result | Acc]}};
                        skip ->
                            {ok, {Count, Acc}}
                    end
            end
        end,
        {0, []},
        point
    ),

    %% Results are in reverse order due to prepend - reverse them
    OrderedResults = lists:reverse(Results),

    %% Apply offset and limit
    FinalResults = apply_offset_limit(OrderedResults, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @doc Execute index seek with adaptive chunked processing for unbounded queries.
%% Processes index entries in chunks, batch-fetching each chunk for efficiency.
%% Chunk size adapts based on average document size to target ~1MB per batch.
execute_index_seek_chunked(StoreRef, DbName, FullPath, Plan, Snapshot) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        order = Order,
        offset = Offset,
        include_docs = IncludeDocs,
        decoder_fun = _DecoderFun
    } = Plan,

    %% Fold index in chunks, batch-fetch and filter each chunk
    %% Use long_scan profile for unbounded queries (prefetch, avoid cache pollution)
    {ResultChunks, _TotalBytes, _ChunkCount} = barrel_ars_index:fold_path_chunked(
        StoreRef, DbName, FullPath, ?INITIAL_CHUNK_SIZE,
        fun(DocIdChunk, {Acc, TotalBytes, ChunkCount}) ->
            {ChunkResults, ChunkBytes} = batch_fetch_and_filter_with_size(
                StoreRef, DbName, DocIdChunk,
                Conditions, Bindings, Projections, IncludeDocs, Snapshot),

            %% Calculate new chunk size based on average doc size
            NewTotalBytes = TotalBytes + ChunkBytes,
            NewChunkCount = ChunkCount + 1,
            TotalDocs = length(DocIdChunk) * NewChunkCount,
            AvgDocSize = case TotalDocs > 0 of
                true -> NewTotalBytes div TotalDocs;
                false -> 0
            end,
            NewChunkSize = calculate_chunk_size(AvgDocSize),

            {ok, {[ChunkResults | Acc], NewTotalBytes, NewChunkCount}, NewChunkSize}
        end,
        {[], 0, 0},
        long_scan
    ),

    %% Flatten result chunks (they're in reverse order)
    FlatResults = lists:append(lists:reverse(ResultChunks)),

    %% Apply ordering if specified
    OrderedResults = apply_order(FlatResults, Order),

    %% Apply offset (no limit for unbounded queries)
    FinalResults = apply_offset_limit(OrderedResults, Offset, undefined),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @doc Calculate optimal chunk size based on average doc size
%% Targets ~1MB of doc data per chunk for good cache utilization
calculate_chunk_size(AvgDocSize) when AvgDocSize > 0 ->
    Optimal = ?TARGET_CHUNK_BYTES div AvgDocSize,
    max(?MIN_CHUNK_SIZE, min(?MAX_CHUNK_SIZE, Optimal));
calculate_chunk_size(_) ->
    ?INITIAL_CHUNK_SIZE.

%% @doc Batch fetch with size tracking for adaptive chunking
batch_fetch_and_filter_with_size(StoreRef, DbName, DocIds, Conditions, Bindings, Projections, IncludeDocs, Snapshot) ->
    Results = batch_fetch_and_filter(StoreRef, DbName, DocIds, Conditions, Bindings, Projections, IncludeDocs, Snapshot),
    %% Estimate bytes from doc count (conservative ~500 bytes avg per doc)
    EstimatedBytes = length(DocIds) * 500,
    {Results, EstimatedBytes}.

%% @private Helper for process dictionary get with default
pdict_get(Key, Default) ->
    case erlang:get(Key) of
        undefined -> Default;
        Value -> Value
    end.

%% @doc Check if early limit optimization can be used
%% Returns {true, MaxToCollect} or false
can_use_early_limit([], [], Limit) when is_integer(Limit), Limit > 0 ->
    %% No ORDER BY and no remaining conditions - safe to limit early
    %% Collect limit + small buffer to account for deleted docs (reduced from 2x)
    {true, max(Limit + 5, round(Limit * 1.2))};
can_use_early_limit([], RemainingConds, Limit) when is_integer(Limit), Limit > 0, RemainingConds =/= [] ->
    %% No ORDER BY but has remaining conditions - over-collect to handle filter losses
    %% Use 3x multiplier to account for filtering (conservative estimate)
    {true, Limit * 3};
can_use_early_limit(_, _, _) ->
    %% Has ORDER BY - need full scan to sort candidates
    false.

%% @doc Check if streaming execution should be used for index seek
%% Streaming is better when: small limit + high cardinality index
%% Returns true if streaming should be used
should_use_streaming(Limit, Cardinality) when is_integer(Limit), Limit > 0, Limit =< 100 ->
    %% Use streaming when cardinality is much higher than limit
    %% This avoids batch-fetching many documents we won't use
    Cardinality > Limit * 10;
should_use_streaming(_, _) ->
    false.

%% @doc Check if we can use indexed order for ORDER BY + LIMIT
%% Returns {true, Path, Dir} if ORDER BY can use index iteration order
can_use_indexed_order([], _Limit) ->
    false;
can_use_indexed_order(_Order, undefined) ->
    false;
can_use_indexed_order([{Path, Dir}], Limit) when is_list(Path), is_integer(Limit), Limit > 0 ->
    %% Single ORDER BY on a path - can use index order
    {true, Path, Dir};
can_use_indexed_order([{Var, _Dir}], Limit) when is_atom(Var), is_integer(Limit), Limit > 0 ->
    %% ORDER BY on a variable - check if it's bound to a path
    %% For now, we only support direct path ordering
    case is_logic_var(Var) of
        true -> false;  %% Variable - can't use directly
        false -> false
    end;
can_use_indexed_order(_, _) ->
    false.

%% @doc Execute query using indexed order for ORDER BY + LIMIT
%% Iterates the index in the requested order and stops early
%% Uses CBOR iterator for condition matching and projection
execute_with_indexed_order(StoreRef, DbName, OrderPath, Dir, Plan, _Snapshot) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs,
        decoder_fun = _DecoderFun
    } = Plan,

    %% Calculate how many results we need (accounting for filtering)
    %% Collect 3x to handle filtering and deleted docs
    MaxCollect = (Limit + Offset) * 3,

    %% Select read profile based on limit (ORDER BY + LIMIT is typically small)
    Profile = select_read_profile(Limit, MaxCollect),

    %% Choose iteration direction based on ORDER BY
    FoldFun = case Dir of
        desc -> fun barrel_ars_index:fold_path_values_reverse/6;
        asc -> fun barrel_ars_index:fold_path_values/6
    end,

    %% Collect matching documents with early termination
    {_Count, Results} = FoldFun(
        StoreRef, DbName, OrderPath,
        fun({_Path, DocId}, {Count, Acc}) ->
            case Count >= MaxCollect of
                true ->
                    {stop, {Count, Acc}};
                false ->
                    %% Fetch and filter document
                    case fetch_and_match_doc(StoreRef, DbName, DocId, Conditions, Bindings, IncludeDocs) of
                        {ok, Doc, BoundVars} ->
                            Result = project_result(Doc, DocId, Projections, BoundVars, IncludeDocs),
                            {ok, {Count + 1, [Result | Acc]}};
                        skip ->
                            {ok, {Count, Acc}}
                    end
            end
        end,
        {0, []},
        Profile
    ),

    %% Results are in reverse order due to prepend - reverse them
    %% Note: For DESC we iterate high-to-low, prepend gives low-to-high, so reverse gives high-to-low
    %% For ASC we iterate low-to-high, prepend gives high-to-low, so reverse gives low-to-high
    OrderedResults = lists:reverse(Results),

    %% Apply offset and limit (no sorting needed - already in order)
    FinalResults = apply_offset_limit(OrderedResults, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @doc Fetch a document and check if it matches conditions (column-wide storage)
%% Returns {ok, Doc, BoundVars} or {ok_cbor, CborBin, BoundVars} or skip
%% When IncludeDocs is false, returns CBOR binary to avoid full decode
fetch_and_match_doc(StoreRef, DbName, DocId, Conditions, Bindings, IncludeDocs) ->
    DocCurrentKey = barrel_store_keys:doc_current(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, DocCurrentKey) of
        {ok, CurrentBin} ->
            {Rev, Deleted, _Hlc} = binary_to_term(CurrentBin),
            case Deleted of
                true ->
                    skip;
                false ->
                    %% Fetch doc body from body store (BlobDB)
                    case barrel_doc_body_store:get_body(DbName, DocId, Rev) of
                        {ok, CborBin} ->
                            %% Decode to map for condition matching
                            %% Plain CBOR or indexed - decode_any handles both
                            Doc = barrel_docdb_codec_cbor:decode_any(CborBin),
                            case matches_conditions(Doc, Conditions, Bindings) of
                                {true, BoundVars} ->
                                    case IncludeDocs of
                                        true ->
                                            {ok, Doc, BoundVars};
                                        false ->
                                            %% Return decoded map for projection
                                            {ok, Doc, BoundVars}
                                    end;
                                false ->
                                    skip
                            end;
                        _ ->
                            skip
                    end
            end;
        _ ->
            skip
    end.

%% @doc Execute using index prefix scan with adaptive strategy selection
execute_index_scan(StoreRef, DbName, Plan, Snapshot) ->
    #query_plan{conditions = Conditions, limit = Limit} = Plan,
    %% Use adaptive classification for equality queries
    %% For limited queries, always prefer scan (early termination)
    %% For unlimited high-cardinality, prefer bulk decode
    case classify_scan_query_adaptive(StoreRef, DbName, Conditions, Plan) of
        {scan_equality, Path, Value} ->
            %% Low cardinality or has limit: iterate value_index keys
            execute_pure_equality(StoreRef, DbName, Path, Value, Plan, Snapshot);
        {bulk_equality, Path, Value} ->
            %% High cardinality without limit: single posting list decode
            case Limit of
                undefined ->
                    execute_bulk_equality(StoreRef, DbName, Path, Value, Plan, Snapshot);
                _ ->
                    %% Has limit, prefer scan for early termination
                    execute_pure_equality(StoreRef, DbName, Path, Value, Plan, Snapshot)
            end;
        {pure_exists, Path} ->
            execute_pure_exists(StoreRef, DbName, Path, Plan, Snapshot);
        {pure_prefix, Path, Prefix} ->
            execute_pure_prefix(StoreRef, DbName, Path, Prefix, Plan, Snapshot);
        {pure_compare, Path, Op, Value} ->
            %% Range query using index scan
            execute_pure_compare(StoreRef, DbName, Path, Op, Value, Plan, Snapshot);
        {multi_index, IndexConditions} ->
            %% Multi-condition with all index-friendly conditions
            execute_multi_index(StoreRef, DbName, IndexConditions, Plan, Snapshot);
        needs_body ->
            %% Extract limit from Plan for early termination
            #query_plan{limit = Limit, offset = Offset} = Plan,
            MaxCount = case Limit of
                undefined -> infinity;
                L when is_integer(L) -> (L + Offset) * 3  %% Over-collect to handle filtering
            end,
            DocIds = collect_scan_docids(StoreRef, DbName, Conditions, Snapshot, MaxCount),
            filter_and_project(StoreRef, DbName, DocIds, Plan, Snapshot)
    end.

%% @doc Classify scan query for pure index execution.
%% Returns:
%%   {pure_equality, Path, Value} - exact match, return DocIds from posting list
%%   {pure_exists, Path} - exists check, return DocIds from posting lists
%%   {pure_prefix, Path, Prefix} - prefix match, return DocIds from posting lists
%%   needs_body - requires doc body fetch for filtering/projection
-spec classify_scan_query([condition()], query_plan()) ->
    {pure_equality, [term()], term()} |
    {pure_exists, [term()]} |
    {pure_prefix, [term()], binary()} |
    {pure_compare, [term()], compare_op(), term()} |
    {multi_index, [condition()]} |
    needs_body.
classify_scan_query(Conditions, Plan) ->
    #query_plan{include_docs = IncludeDocs, projections = Projections} = Plan,
    NeedsBody = IncludeDocs orelse needs_body_for_projection(Projections),
    case {Conditions, NeedsBody} of
        {[{path, Path, Value}], false} ->
            %% Pure equality - only if value is concrete (not a logic var)
            case is_logic_var(Value) of
                true -> needs_body;
                false -> {pure_equality, Path, Value}
            end;
        {[{exists, Path}], false} ->
            {pure_exists, Path};
        {[{prefix, Path, Prefix}], false} ->
            {pure_prefix, Path, Prefix};
        {[{compare, Path, Op, Value}], false} when Op =:= '>' orelse Op =:= '<'
                                                   orelse Op =:= '>=' orelse Op =:= '=<' ->
            %% Pure compare - use index range scan
            case is_logic_var(Value) of
                true -> needs_body;
                false -> {pure_compare, Path, Op, Value}
            end;
        {MultiConds, false} when length(MultiConds) > 1 ->
            %% Multiple conditions - check if all are index-friendly
            case all_index_conditions(MultiConds) of
                true -> {multi_index, MultiConds};
                false -> needs_body
            end;
        _ ->
            needs_body
    end.

%% @doc Check if all conditions can be evaluated using index
all_index_conditions([]) -> true;
all_index_conditions([{path, _, Value} | Rest]) ->
    case is_logic_var(Value) of
        true -> false;
        false -> all_index_conditions(Rest)
    end;
all_index_conditions([{exists, _} | Rest]) ->
    all_index_conditions(Rest);
all_index_conditions([{prefix, _, _} | Rest]) ->
    all_index_conditions(Rest);
all_index_conditions([{compare, _, Op, Value} | Rest])
  when Op =:= '>' orelse Op =:= '<' orelse Op =:= '>=' orelse Op =:= '=<' ->
    case is_logic_var(Value) of
        true -> false;
        false -> all_index_conditions(Rest)
    end;
all_index_conditions(_) -> false.

%% @doc Check if projections require document body
needs_body_for_projection(['*']) -> false;
needs_body_for_projection([]) -> false;
needs_body_for_projection(Projections) ->
    %% Projections require body if they include path references
    lists:any(fun(P) -> is_list(P) end, Projections).

%% @doc Adaptive query classification with cardinality-based strategy selection
%% For equality queries, chooses between:
%% - scan_equality: iterate value_index keys (good for selective queries, early termination)
%% - bulk_equality: decode posting list (good for high-cardinality, one seek)
-spec classify_scan_query_adaptive(barrel_store_rocksdb:db_ref(), db_name(), [condition()], query_plan()) ->
    {scan_equality, [term()], term()} |
    {bulk_equality, [term()], term()} |
    {pure_exists, [term()]} |
    {pure_prefix, [term()], binary()} |
    {pure_compare, [term()], compare_op(), term()} |
    {multi_index, [condition()]} |
    needs_body.
classify_scan_query_adaptive(StoreRef, DbName, Conditions, Plan) ->
    #query_plan{include_docs = IncludeDocs, projections = Projections} = Plan,
    NeedsBody = IncludeDocs orelse needs_body_for_projection(Projections),
    case {Conditions, NeedsBody} of
        {[{path, Path, Value}], false} ->
            case is_logic_var(Value) of
                true ->
                    needs_body;
                false ->
                    %% Check cardinality to choose strategy
                    choose_equality_strategy(StoreRef, DbName, Path, Value)
            end;
        {[{exists, Path}], false} ->
            {pure_exists, Path};
        {[{prefix, Path, Prefix}], false} ->
            {pure_prefix, Path, Prefix};
        {[{compare, Path, Op, Value}], false} when Op =:= '>' orelse Op =:= '<'
                                                   orelse Op =:= '>=' orelse Op =:= '=<' ->
            case is_logic_var(Value) of
                true -> needs_body;
                false -> {pure_compare, Path, Op, Value}
            end;
        {MultiConds, false} when length(MultiConds) > 1 ->
            %% Multiple conditions - check if all are index-friendly
            case all_index_conditions(MultiConds) of
                true -> {multi_index, MultiConds};
                false -> needs_body
            end;
        _ ->
            needs_body
    end.

%% @doc Choose between scan vs bulk strategy based on estimated cardinality
-spec choose_equality_strategy(barrel_store_rocksdb:db_ref(), db_name(), [term()], term()) ->
    {scan_equality, [term()], term()} | {bulk_equality, [term()], term()}.
choose_equality_strategy(StoreRef, DbName, Path, Value) ->
    case estimate_cardinality(StoreRef, DbName, Path, Value) of
        {ok, Count} when Count =< ?ADAPTIVE_CARDINALITY_THRESHOLD ->
            %% Low cardinality: use value_index scan with early termination
            {scan_equality, Path, Value};
        {ok, _HighCount} ->
            %% High cardinality: use posting list bulk decode
            {bulk_equality, Path, Value};
        {error, _} ->
            %% Error getting stats, fall back to scan (safer default)
            {scan_equality, Path, Value}
    end.

%% @doc Estimate cardinality for a path+value combination
%% Uses stored path statistics for O(1) lookup
-spec estimate_cardinality(barrel_store_rocksdb:db_ref(), db_name(), [term()], term()) ->
    {ok, non_neg_integer()} | {error, term()}.
estimate_cardinality(StoreRef, DbName, Path, Value) ->
    FullPath = Path ++ [Value],
    barrel_ars_index:get_path_cardinality(StoreRef, DbName, FullPath).

%% @doc Execute pure exists query - iterate posting lists directly
%% Path index only contains non-deleted docs, so no doc_current check needed
%% Uses fold_posting which gets list of DocIds per key - much more efficient
execute_pure_exists(StoreRef, DbName, Path, Plan, Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Calculate how many unique docs we need
    MaxCollect = case Limit of
        undefined -> undefined;
        L -> L + Offset
    end,

    %% Iterate posting lists directly with snapshot for consistency
    %% Each key contains a list of DocIds
    {_, _, Results} = barrel_ars_index:fold_posting_with_snapshot(
        StoreRef, DbName, Path,
        fun(_Key, DocIds, {Seen, Count, Acc}) ->
            %% Process all DocIds from this posting list (ignore Key)
            process_exists_docids(DocIds, Seen, Count, Acc, MaxCollect)
        end,
        {#{}, 0, []},
        Snapshot
    ),

    %% Apply offset and limit (results are in reverse order)
    Results1 = lists:reverse(Results),
    Results2 = apply_offset_limit(Results1, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results2, LastSeq}.

%% @private Process DocIds from a posting list for exists query
process_exists_docids([], Seen, Count, Acc, _MaxCollect) ->
    {ok, {Seen, Count, Acc}};
process_exists_docids([DocId | Rest], Seen, Count, Acc, MaxCollect) ->
    case maps:is_key(DocId, Seen) of
        true ->
            %% Already seen this doc, skip
            process_exists_docids(Rest, Seen, Count, Acc, MaxCollect);
        false ->
            %% New doc
            NewSeen = Seen#{DocId => true},
            NewCount = Count + 1,
            NewAcc = [#{<<"id">> => DocId} | Acc],
            case MaxCollect =/= undefined andalso NewCount >= MaxCollect of
                true ->
                    {stop, {NewSeen, NewCount, NewAcc}};
                false ->
                    process_exists_docids(Rest, NewSeen, NewCount, NewAcc, MaxCollect)
            end
    end.

%% @doc Execute pure prefix query - iterate posting lists directly
%% No doc_current check needed since path index only contains non-deleted docs
%% Uses fold_prefix_posting which gets raw DocId lists per posting list key
execute_pure_prefix(StoreRef, DbName, Path, Prefix, Plan, Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Calculate how many unique docs we need
    MaxCollect = case Limit of
        undefined -> undefined;
        L -> L + Offset
    end,

    %% Iterate posting lists directly with snapshot for consistency
    %% Each key contains a list of DocIds
    {_, _, Results} = barrel_ars_index:fold_prefix_posting_with_snapshot(
        StoreRef, DbName, Path, Prefix,
        fun(_Key, DocIds, {Seen, Count, Acc}) ->
            %% Process all DocIds from this posting list
            process_prefix_docids(DocIds, Seen, Count, Acc, MaxCollect)
        end,
        {#{}, 0, []},
        Snapshot
    ),

    %% Apply offset and limit (results are in reverse order)
    Results1 = lists:reverse(Results),
    Results2 = apply_offset_limit(Results1, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results2, LastSeq}.

%% @doc Execute pure compare query - iterates posting lists for range matches
%% Uses fold_path_values_compare which scans values matching the operator
execute_pure_compare(StoreRef, DbName, Path, Op, Value, Plan, Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Calculate how many unique docs we need
    MaxCollect = case Limit of
        undefined -> undefined;
        L -> L + Offset
    end,

    %% Iterate posting lists for values matching the compare condition
    FoldFun = fun({_FullPath, DocId}, {Seen, Count, Acc}) ->
        case maps:is_key(DocId, Seen) of
            true ->
                {ok, {Seen, Count, Acc}};
            false ->
                NewSeen = Seen#{DocId => true},
                NewCount = Count + 1,
                NewAcc = [#{<<"id">> => DocId} | Acc],
                case MaxCollect =/= undefined andalso NewCount >= MaxCollect of
                    true ->
                        {stop, {NewSeen, NewCount, NewAcc}};
                    false ->
                        {ok, {NewSeen, NewCount, NewAcc}}
                end
        end
    end,

    %% Use fold_path_values_compare with snapshot
    %% For now, use non-snapshot version and handle snapshot separately
    %% TODO: Add snapshot support to fold_path_values_compare
    _ = Snapshot,  %% Snapshot not used yet for this path
    {_, _, Results} = barrel_ars_index:fold_path_values_compare(
        StoreRef, DbName, Path, Op, Value, FoldFun, {#{}, 0, []}
    ),

    %% Apply offset and limit (results are in reverse order)
    Results1 = lists:reverse(Results),
    Results2 = apply_offset_limit(Results1, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results2, LastSeq}.

%% @doc Execute multi-index query - intersects posting lists from all conditions
execute_multi_index(StoreRef, DbName, Conditions, Plan, Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Collect DocIds using posting list intersection
    IntersectedDocIds = intersect_docid_sets(StoreRef, DbName, Conditions),

    %% Build results with limit applied
    LimitedDocIds = case Limit of
        undefined -> IntersectedDocIds;
        L -> lists:sublist(IntersectedDocIds, L + Offset)
    end,
    Results0 = [#{<<"id">> => DocId} || DocId <- LimitedDocIds],
    Results = apply_offset_limit(Results0, Offset, Limit),

    %% Get last sequence
    _ = Snapshot,  %% Snapshot not used yet for this path
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results, LastSeq}.

%% @doc Execute pure equality query - iterates value-first index with early termination
%% Uses prefix scan on individual keys, stops after collecting enough results
execute_pure_equality(StoreRef, DbName, Path, Value, Plan, Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Calculate how many docs we need to collect
    MaxCollect = case Limit of
        undefined -> undefined;
        L -> L + Offset
    end,

    %% Fold over value-first index with early termination
    %% Path is the field path, Value is what we're searching for
    FoldFun = fun(DocId, {Count, Acc}) ->
        NewCount = Count + 1,
        NewAcc = [#{<<"id">> => DocId} | Acc],
        case MaxCollect =/= undefined andalso NewCount >= MaxCollect of
            true ->
                %% Collected enough, stop iteration
                {stop, {NewCount, NewAcc}};
            false ->
                {ok, {NewCount, NewAcc}}
        end
    end,
    {_, Results} = barrel_ars_index:fold_value_index(
        StoreRef, DbName, Value, Path, FoldFun, {0, []}, Snapshot),

    %% Results are in reverse order, fix and apply offset
    FinalResults = apply_offset_limit(lists:reverse(Results), Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @doc Execute bulk equality query - single seek + posting list decode
%% Used for high-cardinality queries where iterating individual keys is slower
%% than decoding a posting list in one go.
execute_bulk_equality(StoreRef, DbName, Path, Value, Plan, _Snapshot) ->
    #query_plan{
        limit = Limit,
        offset = Offset
    } = Plan,

    %% Single O(1) posting list lookup
    FullPath = Path ++ [Value],
    DocIds = barrel_ars_index:get_posting_list(StoreRef, DbName, FullPath),

    %% Build results and apply offset/limit
    Results = [#{<<"id">> => Id} || Id <- DocIds],
    FinalResults = apply_offset_limit(Results, Offset, Limit),

    %% Get last sequence
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, FinalResults, LastSeq}.

%% @private Process DocIds from a posting list for prefix query
process_prefix_docids([], Seen, Count, Acc, _MaxCollect) ->
    {ok, {Seen, Count, Acc}};
process_prefix_docids([DocId | Rest], Seen, Count, Acc, MaxCollect) ->
    case maps:is_key(DocId, Seen) of
        true ->
            %% Already seen this doc, skip
            process_prefix_docids(Rest, Seen, Count, Acc, MaxCollect);
        false ->
            %% New doc
            NewSeen = Seen#{DocId => true},
            NewCount = Count + 1,
            NewAcc = [#{<<"id">> => DocId} | Acc],
            case MaxCollect =/= undefined andalso NewCount >= MaxCollect of
                true ->
                    {stop, {NewSeen, NewCount, NewAcc}};
                false ->
                    process_prefix_docids(Rest, NewSeen, NewCount, NewAcc, MaxCollect)
            end
    end.

%% @doc Collect DocIds for scan-based execution
%% Tries optimized paths: multi-index intersection, exists/prefix, then path prefix scan
%% MaxCount limits collection for early termination (use infinity for no limit)
collect_scan_docids(StoreRef, DbName, Conditions, _Snapshot, MaxCount) ->
    %% First check if we have multiple index-friendly conditions for intersection
    IndexConditions = [C || C <- Conditions, is_index_condition(C)],
    case IndexConditions of
        [_, _ | _] ->
            %% Multiple index conditions - use bitmap intersection for efficiency
            intersect_docid_sets(StoreRef, DbName, IndexConditions);
        _ ->
            %% Single or no index conditions - use existing path
            case find_exists_condition(Conditions) of
                {ok, Path} ->
                    %% Exists check: collect docs that have any value at this path
                    collect_docids_for_path_exists(StoreRef, DbName, Path, MaxCount);
                not_found ->
                    case find_prefix_condition(Conditions) of
                        {ok, Path, Prefix} ->
                            %% Optimized interval scan for prefix queries
                            collect_docids_for_value_prefix(StoreRef, DbName, Path, Prefix, MaxCount);
                        not_found ->
                            case find_best_scan_path(Conditions) of
                                {ok, Path} ->
                                    collect_docids_for_prefix(StoreRef, DbName, Path, MaxCount);
                                not_found ->
                                    collect_all_docids(StoreRef, DbName, MaxCount)
                            end
                    end
            end
    end.

%% @private Check if a condition can use index for intersection
is_index_condition({path, _Path, Value}) ->
    not is_logic_var(Value);
is_index_condition({compare, _Path, _Op, Value}) ->
    not is_logic_var(Value);
is_index_condition({prefix, _Path, _Prefix}) ->
    true;
is_index_condition({exists, _Path}) ->
    true;
is_index_condition(_) ->
    false.

%% @doc Find an exists condition for optimized path scan
find_exists_condition([]) ->
    not_found;
find_exists_condition([{exists, Path} | _]) ->
    {ok, Path};
find_exists_condition([{'and', Nested} | Rest]) ->
    case find_exists_condition(Nested) of
        {ok, _} = Found -> Found;
        not_found -> find_exists_condition(Rest)
    end;
find_exists_condition([_ | Rest]) ->
    find_exists_condition(Rest).

%% @doc Collect all DocIds that have any value at the given path
%% Uses the path index to find docs with the path without fetching full docs
%% MaxCount limits collection for early termination (use infinity for no limit)
collect_docids_for_path_exists(StoreRef, DbName, Path, infinity) ->
    %% Use long_scan profile since exists queries typically return many docs
    barrel_ars_index:fold_path_values(
        StoreRef, DbName, Path,
        fun({_FullPath, DocId}, Acc) -> {ok, [DocId | Acc]} end,
        [],
        long_scan
    );
collect_docids_for_path_exists(StoreRef, DbName, Path, MaxCount) when is_integer(MaxCount) ->
    %% Limited collection with early termination
    Profile = select_read_profile(MaxCount),
    {_, DocIds} = barrel_ars_index:fold_path_values(
        StoreRef, DbName, Path,
        fun({_FullPath, DocId}, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false -> {ok, {Count + 1, [DocId | Acc]}}
            end
        end,
        {0, []},
        Profile
    ),
    DocIds.

%% @doc Collect all document IDs (for full scan fallback)
%% MaxCount limits collection for early termination (use infinity for no limit)
collect_all_docids(StoreRef, DbName, infinity) ->
    %% Use long_scan profile for full table scans (prefetch, avoid cache pollution)
    barrel_store_rocksdb:fold_range(
        StoreRef,
        barrel_store_keys:doc_info_prefix(DbName),
        barrel_store_keys:doc_info_end(DbName),
        fun(Key, _Value, Acc) ->
            DocId = barrel_store_keys:decode_doc_info_key(DbName, Key),
            {ok, [DocId | Acc]}
        end,
        [],
        long_scan
    );
collect_all_docids(StoreRef, DbName, MaxCount) when is_integer(MaxCount) ->
    %% Limited full scan with early termination
    Profile = select_read_profile(MaxCount),
    {_, DocIds} = barrel_store_rocksdb:fold_range(
        StoreRef,
        barrel_store_keys:doc_info_prefix(DbName),
        barrel_store_keys:doc_info_end(DbName),
        fun(Key, _Value, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false ->
                    DocId = barrel_store_keys:decode_doc_info_key(DbName, Key),
                    {ok, {Count + 1, [DocId | Acc]}}
            end
        end,
        {0, []},
        Profile
    ),
    DocIds.

%% @doc Find a prefix condition for optimized interval scan
find_prefix_condition([]) ->
    not_found;
find_prefix_condition([{prefix, Path, Prefix} | _]) ->
    {ok, Path, Prefix};
find_prefix_condition([{'and', Nested} | Rest]) ->
    case find_prefix_condition(Nested) of
        {ok, _, _} = Found -> Found;
        not_found -> find_prefix_condition(Rest)
    end;
find_prefix_condition([_ | Rest]) ->
    find_prefix_condition(Rest).

%% @doc Collect DocIds using optimized prefix interval scan
%% MaxCount limits collection for early termination (use infinity for no limit)
collect_docids_for_value_prefix(StoreRef, DbName, Path, Prefix, infinity) ->
    %% Use short_range profile - prefix queries typically have moderate selectivity
    barrel_ars_index:fold_prefix(
        StoreRef, DbName, Path, Prefix,
        fun({_FullPath, DocId}, Acc) -> {ok, [DocId | Acc]} end,
        [],
        short_range
    );
collect_docids_for_value_prefix(StoreRef, DbName, Path, Prefix, MaxCount) when is_integer(MaxCount) ->
    %% Limited prefix scan with early termination
    Profile = select_read_profile(MaxCount),
    {_, DocIds} = barrel_ars_index:fold_prefix(
        StoreRef, DbName, Path, Prefix,
        fun({_FullPath, DocId}, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false -> {ok, {Count + 1, [DocId | Acc]}}
            end
        end,
        {0, []},
        Profile
    ),
    DocIds.

%% @doc Execute using multiple index lookups with intersection
%% Uses cardinality-ordered intersection for efficient multi-condition queries.
execute_multi_index(StoreRef, DbName, Plan, Snapshot) ->
    #query_plan{conditions = Conditions, limit = Limit, offset = Offset,
                include_docs = IncludeDocs, projections = Projections} = Plan,

    %% Find all indexable conditions (including compare, exists, prefix)
    IndexConditions = find_all_index_conditions(Conditions),

    case IndexConditions of
        [] ->
            execute_full_scan(StoreRef, DbName, Plan, Snapshot);
        _ ->
            %% Check if all conditions are equality (can use bitmap optimization)
            AllEquality = lists:all(fun({path, _, _}) -> true; (_) -> false end, IndexConditions),

            case AllEquality of
                true ->
                    %% All equality - use bitmap/sorted intersection path
                    case order_by_cardinality(StoreRef, DbName, IndexConditions) of
                        [] ->
                            filter_and_project(StoreRef, DbName, [], Plan, Snapshot);
                        OrderedConditions when length(OrderedConditions) =< 2 ->
                            execute_with_bitmap_filter(StoreRef, DbName, OrderedConditions, Plan, Snapshot);
                        OrderedConditions ->
                            [{path, Path1, Value1} | Rest] = OrderedConditions,
                            FullPath1 = Path1 ++ [Value1],
                            execute_sorted_intersection(StoreRef, DbName, FullPath1, Rest, Plan, Snapshot)
                    end;
                false ->
                    %% Mixed conditions (equality + compare/exists/prefix)
                    %% Use intersect_docid_sets which handles all condition types via index
                    IntersectedDocIds = intersect_docid_sets(StoreRef, DbName, IndexConditions),

                    %% Check if remaining conditions need body fetch
                    RemainingConds = Conditions -- IndexConditions,
                    NeedsBody = IncludeDocs orelse needs_body_for_projection(Projections)
                                orelse RemainingConds =/= [],

                    case NeedsBody of
                        false ->
                            %% Pure index query - no body fetch needed
                            LimitedDocIds = case Limit of
                                undefined -> IntersectedDocIds;
                                L -> lists:sublist(IntersectedDocIds, L + Offset)
                            end,
                            Results0 = [#{<<"id">> => DocId} || DocId <- LimitedDocIds],
                            Results = apply_offset_limit(Results0, Offset, Limit),
                            LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),
                            {ok, Results, LastSeq};
                        true ->
                            %% Need body fetch for remaining conditions or include_docs
                            %% Create a new plan with only remaining conditions (index already verified the rest)
                            FilterPlan = Plan#query_plan{conditions = RemainingConds},
                            filter_and_project(StoreRef, DbName, IntersectedDocIds, FilterPlan, Snapshot)
                    end
            end
    end.

%% @doc Execute multi-condition query using bitmap filtering
%% Tries to use bitmaps for fast pre-filtering, falls back to sorted intersection
execute_with_bitmap_filter(StoreRef, DbName, [First | Rest] = IndexConditions, Plan, Snapshot) ->
    {path, Path1, Value1} = First,
    FullPath1 = Path1 ++ [Value1],

    %% Remaining conditions not verified by index (e.g., OR, logic vars)
    #query_plan{conditions = AllConditions} = Plan,
    RemainingConds = AllConditions -- IndexConditions,
    FilterPlan = Plan#query_plan{conditions = RemainingConds},

    %% Check if all conditions have same bitmap size (same path depth category)
    AllSameSize = all_same_bitmap_size(IndexConditions),

    case AllSameSize andalso length(IndexConditions) > 1 of
        true ->
            %% Try to get bitmaps for all conditions
            FullPaths = [Path ++ [Value] || {path, Path, Value} <- IndexConditions],
            BitmapKeys = [barrel_store_keys:path_bitmap_key(DbName, FP) || FP <- FullPaths],
            BitmapResults = barrel_store_rocksdb:multi_get_bitmap(StoreRef, BitmapKeys),

            %% Check if all bitmaps are available and non-empty
            AllBitmaps = [B || {ok, B} <- BitmapResults, byte_size(B) > 0],
            case length(AllBitmaps) =:= length(IndexConditions) of
                true ->
                    %% All bitmaps available - use bitmap intersection for filtering
                    FilterBitmap = barrel_ars_index:bitmap_intersect(AllBitmaps),
                    %% Collect DocIds from first condition, filtered by bitmap
                    DocIds = collect_docids_with_bitmap_filter(StoreRef, DbName, FullPath1, FilterBitmap),
                    filter_and_project(StoreRef, DbName, DocIds, FilterPlan, Snapshot);
                false ->
                    %% Fallback: some bitmaps missing
                    execute_sorted_intersection(StoreRef, DbName, FullPath1, Rest, FilterPlan, Snapshot)
            end;
        false ->
            %% Fallback: different bitmap sizes or single condition
            execute_sorted_intersection(StoreRef, DbName, FullPath1, Rest, FilterPlan, Snapshot)
    end.

%% @doc Check if all conditions use the same bitmap size.
%% With global bitmap size, this always returns true.
all_same_bitmap_size(_Conditions) -> true.

%% @doc Execute using sorted intersection (fallback)
execute_sorted_intersection(StoreRef, DbName, FullPath1, Rest, FilterPlan, Snapshot) ->
    InitialDocIds = collect_docids_for_path(StoreRef, DbName, FullPath1),
    FinalDocIds = intersect_conditions(StoreRef, DbName, Rest, InitialDocIds),
    filter_and_project(StoreRef, DbName, FinalDocIds, FilterPlan, Snapshot).

%% @doc Collect DocIds from a path, filtering by bitmap
collect_docids_with_bitmap_filter(StoreRef, DbName, FullPath, FilterBitmap) ->
    %% Use short_range profile - bitmap-filtered scans are typically moderate size
    barrel_ars_index:fold_path_reverse(
        StoreRef, DbName, FullPath,
        fun({_Path, DocId}, Acc) ->
            Position = barrel_ars_index:doc_position(DocId, FullPath),
            case barrel_ars_index:bitmap_test_position(FilterBitmap, Position) of
                true -> {ok, [DocId | Acc]};
                false -> {ok, Acc}  %% Skip - doesn't match filter
            end
        end,
        [],
        short_range
    ).

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

%% @doc Execute full document scan (slowest, last resort, using column-wide storage)
execute_full_scan(StoreRef, DbName, Plan, Snapshot) ->
    %% Collect all doc IDs by scanning doc_current keys
    %% Use long_scan profile for full table scans (prefetch, avoid cache pollution)
    StartKey = barrel_store_keys:doc_current_prefix(DbName),
    EndKey = barrel_store_keys:doc_current_end(DbName),
    PrefixLen = byte_size(StartKey),
    DocIds = barrel_store_rocksdb:fold_range(
        StoreRef,
        StartKey,
        EndKey,
        fun(Key, _Value, Acc) ->
            %% Extract DocId from key (after prefix)
            DocId = binary:part(Key, PrefixLen, byte_size(Key) - PrefixLen),
            {ok, [DocId | Acc]}
        end,
        [],
        long_scan
    ),

    filter_and_project(StoreRef, DbName, DocIds, Plan, Snapshot).

%% @doc Collect document IDs matching an exact path+value
%% Uses direct posting list lookup - O(1) key fetch instead of iteration
%% Returns sorted list for intersection operations
collect_docids_for_path(StoreRef, DbName, FullPath) ->
    lists:sort(barrel_ars_index:get_posting_list(StoreRef, DbName, FullPath)).

%% @doc Collect document IDs with early termination at MaxCount
%% For LIMIT pushdown optimization
%% Uses direct posting list lookup with sublist - O(1) fetch + truncate
collect_docids_for_path_limited(StoreRef, DbName, FullPath, MaxCount) ->
    AllDocIds = barrel_ars_index:get_posting_list(StoreRef, DbName, FullPath),
    lists:sublist(AllDocIds, MaxCount).

%% @doc Collect document IDs matching a path prefix
%% MaxCount limits collection for early termination (use infinity for no limit)
collect_docids_for_prefix(StoreRef, DbName, PathPrefix, infinity) ->
    %% Use short_range profile - prefix scans have unknown but typically moderate size
    barrel_ars_index:fold_path(
        StoreRef, DbName, PathPrefix,
        fun({_Path, DocId}, Acc) -> {ok, [DocId | Acc]} end,
        [],
        short_range
    );
collect_docids_for_prefix(StoreRef, DbName, PathPrefix, MaxCount) when is_integer(MaxCount) ->
    %% Limited prefix scan with early termination
    Profile = select_read_profile(MaxCount),
    {_, DocIds} = barrel_ars_index:fold_path(
        StoreRef, DbName, PathPrefix,
        fun({_Path, DocId}, {Count, Acc}) ->
            case Count >= MaxCount of
                true -> {stop, {Count, Acc}};
                false -> {ok, {Count + 1, [DocId | Acc]}}
            end
        end,
        {0, []},
        Profile
    ),
    DocIds.

%% @doc Filter results by remaining conditions and apply projections
filter_and_project(StoreRef, DbName, DocIds, Plan, Snapshot) ->
    #query_plan{
        conditions = Conditions,
        bindings = Bindings,
        projections = Projections,
        order = Order,
        limit = Limit,
        offset = Offset,
        include_docs = IncludeDocs,
        decoder_fun = _DecoderFun
    } = Plan,

    %% Remove duplicates
    UniqueDocIds = lists:usort(DocIds),

    %% Batch fetch documents using multi_get with snapshot for consistency
    Results0 = batch_fetch_and_filter(StoreRef, DbName, UniqueDocIds,
                                       Conditions, Bindings, Projections, IncludeDocs, Snapshot),

    %% Apply ordering
    Results1 = apply_order(Results0, Order),

    %% Apply offset and limit
    Results2 = apply_offset_limit(Results1, Offset, Limit),

    %% Get last sequence (for consistency tracking)
    LastSeq = barrel_changes:get_last_seq(StoreRef, DbName),

    {ok, Results2, LastSeq}.

%% @doc Batch fetch documents using multi_get (column-wide storage)
%% Decodes documents to maps for condition matching and projection
batch_fetch_and_filter(StoreRef, DbName, DocIds, Conditions, Bindings, Projections, IncludeDocs, Snapshot) ->
    case DocIds of
        [] -> [];
        _ ->
            %% PROFILING: Doc current state fetch
            T0 = erlang:monotonic_time(microsecond),
            DocCurrentKeys = [barrel_store_keys:doc_current(DbName, Id) || Id <- DocIds],
            DocCurrentResults = barrel_store_rocksdb:multi_get_with_snapshot(StoreRef, DocCurrentKeys, Snapshot),
            T1 = erlang:monotonic_time(microsecond),
            put(profile_docinfo_fetch, pdict_get(profile_docinfo_fetch, 0) + (T1 - T0)),

            %% Step 2: Filter deleted docs, collect {DocId, Rev} pairs for body fetch
            {ActiveDocIdRevs, ActiveDocIds} = lists:foldl(
                fun({DocId, Result}, {AccPairs, AccDocs}) ->
                    case Result of
                        {ok, CurrentBin} ->
                            {Rev, Deleted, _Hlc} = binary_to_term(CurrentBin),
                            case Deleted of
                                true ->
                                    {AccPairs, AccDocs};
                                false ->
                                    {[{DocId, Rev} | AccPairs], [DocId | AccDocs]}
                            end;
                        not_found ->
                            {AccPairs, AccDocs};
                        {error, _} ->
                            {AccPairs, AccDocs}
                    end
                end,
                {[], []},
                lists:zip(DocIds, DocCurrentResults)
            ),

            %% Step 3: Batch fetch all CBOR doc bodies from body store (BlobDB)
            case ActiveDocIdRevs of
                [] -> [];
                _ ->
                    %% PROFILING: Doc body fetch
                    T2 = erlang:monotonic_time(microsecond),
                    ReversedDocIds = lists:reverse(ActiveDocIds),
                    ReversedDocIdRevs = lists:reverse(ActiveDocIdRevs),
                    DocBodyResults = barrel_doc_body_store:multi_get_bodies(DbName, ReversedDocIdRevs, #{}),
                    T3 = erlang:monotonic_time(microsecond),
                    put(profile_docbody_fetch, pdict_get(profile_docbody_fetch, 0) + (T3 - T2)),

                    %% PROFILING: Decode and condition matching + projection
                    T4 = erlang:monotonic_time(microsecond),
                    Results = lists:filtermap(
                        fun({DocId, BodyResult}) ->
                            case BodyResult of
                                {ok, CborBin} ->
                                    %% Decode to map (handles both indexed and plain CBOR)
                                    Doc = barrel_docdb_codec_cbor:decode_any(CborBin),
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
                    ),
                    T5 = erlang:monotonic_time(microsecond),
                    put(profile_deser_match, pdict_get(profile_deser_match, 0) + (T5 - T4)),
                    _ = Snapshot,  %% Snapshot is used for doc_current, body store has its own consistency
                    Results
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

%% @doc Find all index-friendly conditions (equality, compare, exists, prefix)
find_all_index_conditions(Conditions) ->
    find_all_index_conditions(Conditions, []).

find_all_index_conditions([], Acc) ->
    lists:reverse(Acc);
find_all_index_conditions([{path, Path, Value} | Rest], Acc) ->
    case is_logic_var(Value) of
        false ->
            find_all_index_conditions(Rest, [{path, Path, Value} | Acc]);
        true ->
            find_all_index_conditions(Rest, Acc)
    end;
find_all_index_conditions([{compare, Path, Op, Value} | Rest], Acc)
  when Op =:= '>' orelse Op =:= '<' orelse Op =:= '>=' orelse Op =:= '=<' ->
    case is_logic_var(Value) of
        false ->
            find_all_index_conditions(Rest, [{compare, Path, Op, Value} | Acc]);
        true ->
            find_all_index_conditions(Rest, Acc)
    end;
find_all_index_conditions([{exists, Path} | Rest], Acc) ->
    find_all_index_conditions(Rest, [{exists, Path} | Acc]);
find_all_index_conditions([{prefix, Path, Prefix} | Rest], Acc) ->
    find_all_index_conditions(Rest, [{prefix, Path, Prefix} | Acc]);
find_all_index_conditions([{'and', Nested} | Rest], Acc) ->
    NestedConds = find_all_index_conditions(Nested),
    find_all_index_conditions(Rest, NestedConds ++ Acc);
find_all_index_conditions([_ | Rest], Acc) ->
    find_all_index_conditions(Rest, Acc).

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


%%====================================================================
%% Profiling Functions (temporary)
%%====================================================================

%% @doc Get current profiling counters
get_profile() ->
    #{
        index_iter_us => pdict_get(profile_index_iter, 0),
        docinfo_fetch_us => pdict_get(profile_docinfo_fetch, 0),
        docbody_fetch_us => pdict_get(profile_docbody_fetch, 0),
        deser_match_us => pdict_get(profile_deser_match, 0),
        doc_count => pdict_get(profile_doc_count, 0)
    }.

%% @doc Reset profiling counters
reset_profile() ->
    erase(profile_index_iter),
    erase(profile_docinfo_fetch),
    erase(profile_docbody_fetch),
    erase(profile_deser_match),
    erase(profile_doc_count),
    ok.

%% @doc Dump profiling data to console
dump_profile() ->
    Profile = get_profile(),
    Total = maps:get(index_iter_us, Profile) +
            maps:get(docinfo_fetch_us, Profile) +
            maps:get(docbody_fetch_us, Profile) +
            maps:get(deser_match_us, Profile),
    io:format("~n=== Query Profile ===~n"),
    io:format("  Index iteration:     ~8.B us (~5.1f%)~n",
              [maps:get(index_iter_us, Profile),
               pct(maps:get(index_iter_us, Profile), Total)]),
    io:format("  Doc info fetch:      ~8.B us (~5.1f%)~n",
              [maps:get(docinfo_fetch_us, Profile),
               pct(maps:get(docinfo_fetch_us, Profile), Total)]),
    io:format("  Doc body fetch:      ~8.B us (~5.1f%)~n",
              [maps:get(docbody_fetch_us, Profile),
               pct(maps:get(docbody_fetch_us, Profile), Total)]),
    io:format("  Deser + matching:    ~8.B us (~5.1f%)~n",
              [maps:get(deser_match_us, Profile),
               pct(maps:get(deser_match_us, Profile), Total)]),
    io:format("  --------------------------~n"),
    io:format("  Total:               ~8.B us~n", [Total]),
    io:format("  Docs processed:      ~8.B~n", [maps:get(doc_count, Profile)]),
    ok.

pct(_, 0) -> 0.0;
pct(Part, Total) -> (Part / Total) * 100.

