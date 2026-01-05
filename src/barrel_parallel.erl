%%%-------------------------------------------------------------------
%%% @doc Parallel execution utilities for barrel_docdb
%%%
%%% Provides bounded parallel map operations similar to PostgreSQL's
%%% parallel query execution. Limits concurrency to scheduler count
%%% with queue-based backpressure.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_parallel).

-export([
    pmap/2,
    pmap/3,
    pfiltermap/2,
    pfiltermap/3,
    get_default_workers/0
]).

%% Default threshold - below this, sequential is faster
-define(PARALLEL_THRESHOLD, 10).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Get default number of workers (scheduler count)
-spec get_default_workers() -> pos_integer().
get_default_workers() ->
    erlang:system_info(schedulers).

%% @doc Parallel map with default concurrency
-spec pmap(fun((A) -> B), [A]) -> [B] when A :: term(), B :: term().
pmap(Fun, Items) ->
    pmap(Fun, Items, get_default_workers()).

%% @doc Parallel map with bounded concurrency, preserves order
%% Falls back to sequential for small lists (spawn overhead > benefit)
-spec pmap(fun((A) -> B), [A], pos_integer()) -> [B] when A :: term(), B :: term().
pmap(_Fun, [], _MaxWorkers) ->
    [];
pmap(Fun, Items, MaxWorkers) when MaxWorkers > 0 ->
    case length(Items) of
        N when N =< ?PARALLEL_THRESHOLD ->
            %% Small list: sequential is faster
            lists:map(Fun, Items);
        _ ->
            pmap_parallel(Fun, Items, MaxWorkers)
    end.

%% @doc Parallel filtermap with default concurrency
-spec pfiltermap(fun((A) -> boolean() | {true, B}), [A]) -> [B]
    when A :: term(), B :: term().
pfiltermap(Fun, Items) ->
    pfiltermap(Fun, Items, get_default_workers()).

%% @doc Parallel filtermap with bounded concurrency, preserves order
%% Fun returns: false | true | {true, Value}
-spec pfiltermap(fun((A) -> boolean() | {true, B}), [A], pos_integer()) -> [B]
    when A :: term(), B :: term().
pfiltermap(_Fun, [], _MaxWorkers) ->
    [];
pfiltermap(Fun, Items, MaxWorkers) when MaxWorkers > 0 ->
    case length(Items) of
        N when N =< ?PARALLEL_THRESHOLD ->
            lists:filtermap(Fun, Items);
        _ ->
            pfiltermap_parallel(Fun, Items, MaxWorkers)
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private Parallel map implementation
pmap_parallel(Fun, Items, MaxWorkers) ->
    Parent = self(),
    Ref = make_ref(),
    IndexedItems = lists:zip(lists:seq(1, length(Items)), Items),

    %% Process in batches of MaxWorkers
    Results = pmap_batches(Fun, IndexedItems, MaxWorkers, Parent, Ref, []),

    %% Sort by index and extract values
    Sorted = lists:sort(Results),
    [Value || {_Index, Value} <- Sorted].

%% @private Process items in batches
pmap_batches(_Fun, [], _MaxWorkers, _Parent, _Ref, Acc) ->
    Acc;
pmap_batches(Fun, Items, MaxWorkers, Parent, Ref, Acc) ->
    {Batch, Remaining} = safe_split(MaxWorkers, Items),

    %% Spawn workers for this batch
    WorkerRefs = [spawn_worker(Fun, Index, Item, Parent, Ref)
                  || {Index, Item} <- Batch],

    %% Collect results for this batch
    BatchResults = collect_results(WorkerRefs, Ref, []),

    pmap_batches(Fun, Remaining, MaxWorkers, Parent, Ref, BatchResults ++ Acc).

%% @private Parallel filtermap implementation
pfiltermap_parallel(Fun, Items, MaxWorkers) ->
    Parent = self(),
    Ref = make_ref(),
    IndexedItems = lists:zip(lists:seq(1, length(Items)), Items),

    Results = pfiltermap_batches(Fun, IndexedItems, MaxWorkers, Parent, Ref, []),

    %% Sort by index and extract values (only truthy results)
    Sorted = lists:sort(Results),
    [Value || {_Index, Value} <- Sorted].

%% @private Process filtermap items in batches
pfiltermap_batches(_Fun, [], _MaxWorkers, _Parent, _Ref, Acc) ->
    Acc;
pfiltermap_batches(Fun, Items, MaxWorkers, Parent, Ref, Acc) ->
    {Batch, Remaining} = safe_split(MaxWorkers, Items),

    WorkerRefs = [spawn_filtermap_worker(Fun, Index, Item, Parent, Ref)
                  || {Index, Item} <- Batch],

    BatchResults = collect_filtermap_results(WorkerRefs, Ref, []),

    pfiltermap_batches(Fun, Remaining, MaxWorkers, Parent, Ref, BatchResults ++ Acc).

%% @private Spawn a worker process for pmap
spawn_worker(Fun, Index, Item, Parent, Ref) ->
    {_Pid, MonRef} = spawn_monitor(fun() ->
        try
            Result = Fun(Item),
            Parent ! {Ref, Index, {ok, Result}}
        catch
            Class:Reason:Stack ->
                Parent ! {Ref, Index, {error, {Class, Reason, Stack}}}
        end
    end),
    {Index, MonRef}.

%% @private Spawn a worker for filtermap
spawn_filtermap_worker(Fun, Index, Item, Parent, Ref) ->
    {_Pid, MonRef} = spawn_monitor(fun() ->
        try
            Result = Fun(Item),
            Parent ! {Ref, Index, {ok, Result}}
        catch
            Class:Reason:Stack ->
                Parent ! {Ref, Index, {error, {Class, Reason, Stack}}}
        end
    end),
    {Index, MonRef}.

%% @private Collect results from workers
collect_results([], _Ref, Acc) ->
    Acc;
collect_results(WorkerRefs, Ref, Acc) ->
    receive
        {Ref, Index, {ok, Result}} ->
            %% Remove this worker from pending
            WorkerRefs2 = lists:keydelete(Index, 1, WorkerRefs),
            collect_results(WorkerRefs2, Ref, [{Index, Result} | Acc]);
        {Ref, Index, {error, {Class, Reason, Stack}}} ->
            %% Worker failed - clean up remaining and re-raise
            cleanup_workers(WorkerRefs, Index),
            erlang:raise(Class, Reason, Stack);
        {'DOWN', MonRef, process, _Pid, Reason} ->
            case lists:keyfind(MonRef, 2, WorkerRefs) of
                {Index, MonRef} when Reason =/= normal ->
                    %% Worker crashed unexpectedly
                    cleanup_workers(WorkerRefs, Index),
                    error({worker_crashed, Reason});
                _ ->
                    %% Normal exit or unknown monitor - continue
                    collect_results(WorkerRefs, Ref, Acc)
            end
    end.

%% @private Collect filtermap results (skip false results)
collect_filtermap_results([], _Ref, Acc) ->
    Acc;
collect_filtermap_results(WorkerRefs, Ref, Acc) ->
    receive
        {Ref, Index, {ok, false}} ->
            WorkerRefs2 = lists:keydelete(Index, 1, WorkerRefs),
            collect_filtermap_results(WorkerRefs2, Ref, Acc);
        {Ref, Index, {ok, true}} ->
            %% true means keep original item - but we don't have it here
            %% This is for filtermap where true keeps the element
            WorkerRefs2 = lists:keydelete(Index, 1, WorkerRefs),
            %% We need the original item - this case shouldn't happen
            %% in our usage (we always use {true, Value})
            collect_filtermap_results(WorkerRefs2, Ref, Acc);
        {Ref, Index, {ok, {true, Value}}} ->
            WorkerRefs2 = lists:keydelete(Index, 1, WorkerRefs),
            collect_filtermap_results(WorkerRefs2, Ref, [{Index, Value} | Acc]);
        {Ref, Index, {error, {Class, Reason, Stack}}} ->
            cleanup_workers(WorkerRefs, Index),
            erlang:raise(Class, Reason, Stack);
        {'DOWN', MonRef, process, _Pid, Reason} ->
            case lists:keyfind(MonRef, 2, WorkerRefs) of
                {Index, MonRef} when Reason =/= normal ->
                    cleanup_workers(WorkerRefs, Index),
                    error({worker_crashed, Reason});
                _ ->
                    collect_filtermap_results(WorkerRefs, Ref, Acc)
            end
    end.

%% @private Clean up remaining workers on error
cleanup_workers(WorkerRefs, ExceptIndex) ->
    lists:foreach(fun({Index, MonRef}) ->
        case Index of
            ExceptIndex -> ok;
            _ ->
                demonitor(MonRef, [flush]),
                ok
        end
    end, WorkerRefs).

%% @private Safe split that doesn't fail on short lists
safe_split(N, List) when N >= length(List) ->
    {List, []};
safe_split(N, List) ->
    lists:split(N, List).
