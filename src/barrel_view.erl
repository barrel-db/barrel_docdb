%%%-------------------------------------------------------------------
%%% @doc View API and gen_statem for barrel_docdb
%%%
%%% Provides secondary indexes (views) with map/reduce support.
%%% Each view is managed by a gen_statem process that:
%%% - Follows the changes feed to update the index
%%% - Uses snapshot-based iteration for consistency
%%% - Implements backpressure to avoid overloading the node
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_view).

-behaviour(gen_statem).

-include("barrel_docdb.hrl").

%% Behaviour definition
-callback version() -> pos_integer().
-callback map(Doc :: map()) -> [{Key :: term(), Value :: term()}].
-callback reduce(Keys :: [term()] | null, Values :: [term()], Rereduce :: boolean()) -> term().

-optional_callbacks([reduce/3]).

%% API - View management
-export([
    register/3,
    unregister/2,
    list/1
]).

%% API - Query
-export([
    query/3,
    refresh/2,
    refresh/3
]).

%% API - Process management
-export([
    start_link/3,
    stop/1,
    get_view_id/1
]).

%% gen_statem callbacks
-export([
    init/1,
    callback_mode/0,
    terminate/3
]).

%% State functions
-export([
    idle/3,
    indexing/3
]).

%% Internal exports for pipeline processes
-export([
    reader_loop/1,
    mapper_loop/1,
    writer_loop/1
]).

-define(BATCH_SIZE, 100).
-define(MAX_PENDING, 5).
-define(CHECK_INTERVAL, 1000).

%%====================================================================
%% API - View management
%%====================================================================

%% @doc Register a new view
-spec register(pid(), binary(), map()) -> ok | {error, term()}.
register(DbServer, ViewId, Config) ->
    barrel_db_server:register_view(DbServer, ViewId, Config).

%% @doc Unregister a view (removes definition and index)
-spec unregister(pid(), binary()) -> ok | {error, term()}.
unregister(DbServer, ViewId) ->
    barrel_db_server:unregister_view(DbServer, ViewId).

%% @doc List all registered views
-spec list(pid()) -> {ok, [map()]} | {error, term()}.
list(DbServer) ->
    barrel_db_server:list_views(DbServer).

%%====================================================================
%% API - Query
%%====================================================================

%% @doc Query a view
-spec query(pid(), binary(), map()) -> {ok, [map()]} | {error, term()}.
query(DbServer, ViewId, Opts) ->
    case barrel_db_server:get_view_pid(DbServer, ViewId) of
        {ok, Pid} ->
            gen_statem:call(Pid, {query, Opts});
        {error, _} = Error ->
            Error
    end.

%% @doc Wait for view to be up-to-date with current database sequence
-spec refresh(pid(), binary()) -> {ok, seq()} | {error, term()}.
refresh(DbServer, ViewId) ->
    case barrel_db_server:get_view_pid(DbServer, ViewId) of
        {ok, Pid} ->
            gen_statem:call(Pid, refresh, infinity);
        {error, _} = Error ->
            Error
    end.

%% @doc Wait for view to be up-to-date with given sequence
-spec refresh(pid(), binary(), seq()) -> {ok, seq()} | {error, term()}.
refresh(DbServer, ViewId, Seq) ->
    case barrel_db_server:get_view_pid(DbServer, ViewId) of
        {ok, Pid} ->
            gen_statem:call(Pid, {refresh, Seq}, infinity);
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% API - Process management
%%====================================================================

%% @doc Start a view process
-spec start_link(binary(), barrel_store_rocksdb:db_ref(), map()) ->
    {ok, pid()} | {error, term()}.
start_link(DbName, StoreRef, ViewConfig) ->
    gen_statem:start_link(?MODULE, [DbName, StoreRef, ViewConfig], []).

%% @doc Stop a view process
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_statem:stop(Pid).

%% @doc Get the view ID for a view process
-spec get_view_id(pid()) -> {ok, binary()} | {error, term()}.
get_view_id(Pid) ->
    gen_statem:call(Pid, get_view_id).

%%====================================================================
%% gen_statem callbacks
%%====================================================================

callback_mode() -> state_functions.

init([DbName, StoreRef, #{id := ViewId, module := Mod} = Config]) ->
    process_flag(trap_exit, true),

    %% Get reduce function (module callback or built-in)
    Reduce = get_reduce_fun(Config, Mod),

    %% Check stored version vs current module version
    StoredVersion = get_stored_version(StoreRef, DbName, ViewId),
    CurrentVersion = Mod:version(),

    Since = case {StoredVersion, CurrentVersion} of
        {undefined, _} ->
            %% New view - start from beginning
            ok = barrel_view_index:set_view_meta(StoreRef, DbName, ViewId, #{
                id => ViewId,
                module => Mod,
                version => CurrentVersion,
                reduce => Reduce =/= undefined
            }),
            first;
        {V, V} ->
            %% Same version - resume from last indexed seq
            barrel_view_index:get_indexed_seq(StoreRef, DbName, ViewId);
        {Old, New} when New > Old ->
            %% Version bumped - clear and rebuild
            ok = barrel_view_index:clear_all(StoreRef, DbName, ViewId),
            ok = update_stored_version(StoreRef, DbName, ViewId, New),
            first;
        _ ->
            %% Version went backwards - error
            {stop, {error, version_mismatch}}
    end,

    State = #{
        db_name => DbName,
        store_ref => StoreRef,
        view_id => ViewId,
        module => Mod,
        reduce => Reduce,
        since => Since,
        waiters => [],
        pipeline => undefined
    },

    %% Schedule initial check for updates
    {ok, idle, State, [{state_timeout, 0, check_updates}]}.

terminate(_Reason, _StateName, #{pipeline := Pipeline}) ->
    %% Stop pipeline processes if running
    stop_pipeline(Pipeline),
    ok.

%%====================================================================
%% State: idle
%%====================================================================

idle({call, From}, get_view_id, #{view_id := ViewId} = State) ->
    {keep_state, State, [{reply, From, {ok, ViewId}}]};

idle({call, From}, {query, Opts}, State) ->
    Result = do_query(Opts, State),
    {keep_state, State, [{reply, From, Result}]};

idle({call, From}, refresh, #{store_ref := StoreRef, db_name := DbName, since := Since} = State) ->
    %% Get current database sequence
    CurrentSeq = barrel_changes:get_last_seq(StoreRef, DbName),
    case needs_update(Since, CurrentSeq) of
        false ->
            {keep_state, State, [{reply, From, {ok, Since}}]};
        true ->
            %% Start indexing and add to waiters
            Waiters = [{From, CurrentSeq}],
            NewState = State#{waiters => Waiters},
            start_indexing(NewState)
    end;

idle({call, From}, {refresh, TargetSeq}, #{since := Since} = State) ->
    case needs_update(Since, TargetSeq) of
        false ->
            {keep_state, State, [{reply, From, {ok, Since}}]};
        true ->
            Waiters = [{From, TargetSeq}],
            NewState = State#{waiters => Waiters},
            start_indexing(NewState)
    end;

idle(state_timeout, check_updates, #{store_ref := StoreRef, db_name := DbName, since := Since} = State) ->
    CurrentSeq = barrel_changes:get_last_seq(StoreRef, DbName),
    case needs_update(Since, CurrentSeq) of
        false ->
            {keep_state, State, [{state_timeout, ?CHECK_INTERVAL, check_updates}]};
        true ->
            start_indexing(State)
    end;

idle(cast, stop, State) ->
    {stop, normal, State};

idle(_EventType, _Event, State) ->
    {keep_state, State}.

%%====================================================================
%% State: indexing
%%====================================================================

indexing({call, From}, get_view_id, #{view_id := ViewId} = State) ->
    {keep_state, State, [{reply, From, {ok, ViewId}}]};

indexing({call, From}, {query, Opts}, State) ->
    Result = do_query(Opts, State),
    {keep_state, State, [{reply, From, Result}]};

indexing({call, From}, refresh, #{store_ref := StoreRef, db_name := DbName, waiters := Waiters} = State) ->
    CurrentSeq = barrel_changes:get_last_seq(StoreRef, DbName),
    NewWaiters = [{From, CurrentSeq} | Waiters],
    {keep_state, State#{waiters => NewWaiters}};

indexing({call, From}, {refresh, TargetSeq}, #{waiters := Waiters} = State) ->
    NewWaiters = [{From, TargetSeq} | Waiters],
    {keep_state, State#{waiters => NewWaiters}};

indexing(info, {index_updated, NewSeq}, #{waiters := Waiters} = State) ->
    %% Notify waiters whose target seq has been reached
    {Satisfied, Remaining} = lists:partition(
        fun({_From, TargetSeq}) -> not needs_update(NewSeq, TargetSeq) end,
        Waiters
    ),
    lists:foreach(
        fun({From, _}) -> gen_statem:reply(From, {ok, NewSeq}) end,
        Satisfied
    ),
    {keep_state, State#{since => NewSeq, waiters => Remaining}};

indexing(info, {index_complete, FinalSeq}, #{waiters := Waiters} = State) ->
    %% Pipeline finished - notify remaining waiters and go idle
    lists:foreach(
        fun({From, _}) -> gen_statem:reply(From, {ok, FinalSeq}) end,
        Waiters
    ),
    NewState = State#{since => FinalSeq, waiters => [], pipeline => undefined},
    {next_state, idle, NewState, [{state_timeout, ?CHECK_INTERVAL, check_updates}]};

indexing(info, {'EXIT', Pid, Reason}, #{pipeline := #{reader := Pid}} = State) ->
    handle_pipeline_exit(Reason, State);

indexing(info, {'EXIT', Pid, Reason}, #{pipeline := #{mapper := Pid}} = State) ->
    handle_pipeline_exit(Reason, State);

indexing(info, {'EXIT', Pid, Reason}, #{pipeline := #{writer := Pid}} = State) ->
    handle_pipeline_exit(Reason, State);

indexing(cast, stop, State) ->
    {stop, normal, State};

indexing(_EventType, _Event, State) ->
    {keep_state, State}.

%%====================================================================
%% Internal - Indexing Pipeline
%%====================================================================

start_indexing(#{store_ref := StoreRef, db_name := DbName, view_id := ViewId,
                 module := Mod, since := Since} = State) ->
    Parent = self(),

    %% Take a snapshot for consistent reads
    {ok, Snapshot} = barrel_store_rocksdb:snapshot(StoreRef),

    %% Start pipeline processes
    WriterPid = spawn_link(fun() ->
        ?MODULE:writer_loop(#{
            parent => Parent,
            store_ref => StoreRef,
            db_name => DbName,
            view_id => ViewId,
            pending => 0
        })
    end),

    MapperPid = spawn_link(fun() ->
        ?MODULE:mapper_loop(#{
            parent => Parent,
            module => Mod,
            writer => WriterPid,
            pending => 0
        })
    end),

    ReaderPid = spawn_link(fun() ->
        ?MODULE:reader_loop(#{
            parent => Parent,
            store_ref => StoreRef,
            db_name => DbName,
            snapshot => Snapshot,
            since => Since,
            mapper => MapperPid
        })
    end),

    Pipeline = #{
        reader => ReaderPid,
        mapper => MapperPid,
        writer => WriterPid,
        snapshot => Snapshot
    },

    {next_state, indexing, State#{pipeline => Pipeline}}.

stop_pipeline(undefined) ->
    ok;
stop_pipeline(#{reader := R, mapper := M, writer := W, snapshot := Snap}) ->
    %% Kill pipeline processes
    catch exit(R, shutdown),
    catch exit(M, shutdown),
    catch exit(W, shutdown),
    %% Release snapshot
    catch barrel_store_rocksdb:release_snapshot(Snap),
    ok.

handle_pipeline_exit(normal, State) ->
    %% Normal exit - wait for index_complete message
    {keep_state, State};
handle_pipeline_exit({index_complete, Seq}, State) ->
    %% Pipeline completed successfully
    indexing(info, {index_complete, Seq}, State);
handle_pipeline_exit(Reason, #{waiters := Waiters, pipeline := Pipeline} = State) ->
    %% Pipeline failed - notify waiters and go idle
    stop_pipeline(Pipeline),
    lists:foreach(
        fun({From, _}) -> gen_statem:reply(From, {error, {indexing_failed, Reason}}) end,
        Waiters
    ),
    NewState = State#{waiters => [], pipeline => undefined},
    {next_state, idle, NewState, [{state_timeout, ?CHECK_INTERVAL, check_updates}]}.

%%====================================================================
%% Pipeline: Reader Process
%%====================================================================

reader_loop(#{store_ref := StoreRef, db_name := DbName, snapshot := Snapshot,
              since := Since, mapper := Mapper}) ->
    %% Fold changes using snapshot
    FoldFun = fun(Change, {Count, Batch, _LastSeq}) ->
        NewBatch = [Change | Batch],
        NewCount = Count + 1,
        ChangeSeq = maps:get(seq, Change),
        case NewCount >= ?BATCH_SIZE of
            true ->
                %% Send batch to mapper with backpressure
                send_batch(Mapper, lists:reverse(NewBatch), ChangeSeq),
                {ok, {0, [], ChangeSeq}};
            false ->
                {ok, {NewCount, NewBatch, ChangeSeq}}
        end
    end,

    {ok, {_, FinalBatch, FinalSeq}, _} = barrel_changes:fold_changes(
        StoreRef, DbName, Since, FoldFun, {0, [], Since}
    ),

    %% Send final batch if any
    case FinalBatch of
        [] -> ok;
        _ -> send_batch(Mapper, lists:reverse(FinalBatch), FinalSeq)
    end,

    %% Signal end of changes
    Mapper ! {done, FinalSeq},

    %% Release snapshot and exit
    barrel_store_rocksdb:release_snapshot(Snapshot),
    exit({index_complete, FinalSeq}).

send_batch(Mapper, Batch, Seq) ->
    Mapper ! {batch, self(), Batch, Seq},
    receive
        {ack, Mapper} -> ok
    end.

%%====================================================================
%% Pipeline: Mapper Process
%%====================================================================

mapper_loop(#{module := Mod, writer := Writer} = State) ->
    receive
        {batch, Reader, Changes, Seq} ->
            %% Map each document
            Mapped = lists:map(
                fun(Change) ->
                    DocId = maps:get(id, Change),
                    Doc = maps:get(doc, Change, #{}),
                    Deleted = maps:get(deleted, Change, false),
                    Entries = case Deleted of
                        true -> [];
                        false ->
                            try Mod:map(Doc)
                            catch _:_ -> []
                            end
                    end,
                    {DocId, Entries, Deleted}
                end,
                Changes
            ),

            %% Send to writer with backpressure
            Writer ! {mapped, self(), Mapped, Seq},
            receive
                {ack, Writer} -> ok
            end,

            %% Ack reader
            Reader ! {ack, self()},
            mapper_loop(State);

        {done, FinalSeq} ->
            Writer ! {done, FinalSeq},
            exit(normal)
    end.

%%====================================================================
%% Pipeline: Writer Process
%%====================================================================

writer_loop(#{store_ref := StoreRef, db_name := DbName, view_id := ViewId,
              parent := Parent} = State) ->
    receive
        {mapped, Mapper, Entries, Seq} ->
            %% Write entries to index
            lists:foreach(
                fun({DocId, KVs, Deleted}) ->
                    case Deleted of
                        true ->
                            barrel_view_index:delete_doc_entries(StoreRef, DbName, ViewId, DocId);
                        false ->
                            barrel_view_index:update_doc_entries(StoreRef, DbName, ViewId, DocId, KVs)
                    end
                end,
                Entries
            ),

            %% Update indexed sequence
            barrel_view_index:set_indexed_seq(StoreRef, DbName, ViewId, Seq),

            %% Notify parent of progress
            Parent ! {index_updated, Seq},

            %% Ack mapper
            Mapper ! {ack, self()},
            writer_loop(State);

        {done, FinalSeq} ->
            %% Notify parent of completion
            Parent ! {index_complete, FinalSeq},
            exit(normal)
    end.

%%====================================================================
%% Internal - Query
%%====================================================================

do_query(Opts, #{store_ref := StoreRef, db_name := DbName, view_id := ViewId,
                 reduce := ReduceFun}) ->
    %% Get query options
    StartKey = maps:get(start_key, Opts, undefined),
    EndKey = maps:get(end_key, Opts, undefined),
    Limit = maps:get(limit, Opts, infinity),
    Skip = maps:get(skip, Opts, 0),
    Descending = maps:get(descending, Opts, false),
    DoReduce = maps:get(reduce, Opts, ReduceFun =/= undefined),
    Group = maps:get(group, Opts, false),
    GroupLevel = maps:get(group_level, Opts, infinity),

    QueryOpts = #{
        start_key => StartKey,
        end_key => EndKey,
        limit => Limit,
        skip => Skip,
        descending => Descending
    },

    %% Fold index entries
    FoldFun = fun(Entry, Acc) -> {ok, [Entry | Acc]} end,
    {ok, Entries, _Count} = barrel_view_index:query_range(
        StoreRef, DbName, ViewId, QueryOpts, FoldFun
    ),

    Results = case DoReduce andalso ReduceFun =/= undefined of
        false ->
            %% No reduce - return raw entries
            lists:reverse(Entries);
        true ->
            %% Apply reduce
            apply_reduce(Entries, ReduceFun, Group, GroupLevel)
    end,

    {ok, Results}.

apply_reduce(Entries, ReduceFun, false, _GroupLevel) ->
    %% No grouping - reduce all values
    Keys = [maps:get(key, E) || E <- Entries],
    Values = [maps:get(value, E) || E <- Entries],
    ReducedValue = call_reduce(ReduceFun, Keys, Values, false),
    [#{key => null, value => ReducedValue}];

apply_reduce(Entries, ReduceFun, true, GroupLevel) ->
    %% Group by key (or key prefix for compound keys)
    Grouped = group_entries(Entries, GroupLevel),
    lists:map(
        fun({GroupKey, GroupEntries}) ->
            Keys = [maps:get(key, E) || E <- GroupEntries],
            Values = [maps:get(value, E) || E <- GroupEntries],
            ReducedValue = call_reduce(ReduceFun, Keys, Values, false),
            #{key => GroupKey, value => ReducedValue}
        end,
        Grouped
    ).

group_entries(Entries, GroupLevel) ->
    %% Group entries by key (or key prefix)
    lists:foldl(
        fun(Entry, Acc) ->
            Key = maps:get(key, Entry),
            GroupKey = get_group_key(Key, GroupLevel),
            case lists:keyfind(GroupKey, 1, Acc) of
                false ->
                    [{GroupKey, [Entry]} | Acc];
                {GroupKey, Existing} ->
                    lists:keyreplace(GroupKey, 1, Acc, {GroupKey, [Entry | Existing]})
            end
        end,
        [],
        Entries
    ).

get_group_key(Key, infinity) ->
    Key;
get_group_key(Key, Level) when is_list(Key) ->
    lists:sublist(Key, Level);
get_group_key(Key, _Level) ->
    Key.

call_reduce({builtin, '_count'}, _Keys, Values, _Rereduce) ->
    length(Values);
call_reduce({builtin, '_sum'}, _Keys, Values, _Rereduce) ->
    lists:sum(Values);
call_reduce({builtin, '_stats'}, _Keys, Values, false) ->
    #{
        sum => lists:sum(Values),
        count => length(Values),
        min => lists:min(Values),
        max => lists:max(Values),
        sumsqr => lists:sum([V * V || V <- Values])
    };
call_reduce({builtin, '_stats'}, _Keys, Values, true) ->
    %% Rereduce stats
    #{
        sum => lists:sum([maps:get(sum, V) || V <- Values]),
        count => lists:sum([maps:get(count, V) || V <- Values]),
        min => lists:min([maps:get(min, V) || V <- Values]),
        max => lists:max([maps:get(max, V) || V <- Values]),
        sumsqr => lists:sum([maps:get(sumsqr, V) || V <- Values])
    };
call_reduce({module, Mod}, Keys, Values, Rereduce) ->
    Mod:reduce(Keys, Values, Rereduce).

%%====================================================================
%% Internal - Helpers
%%====================================================================

get_reduce_fun(#{reduce := '_count'}, _Mod) -> {builtin, '_count'};
get_reduce_fun(#{reduce := '_sum'}, _Mod) -> {builtin, '_sum'};
get_reduce_fun(#{reduce := '_stats'}, _Mod) -> {builtin, '_stats'};
get_reduce_fun(_, Mod) ->
    %% Check if module exports reduce/3
    case erlang:function_exported(Mod, reduce, 3) of
        true -> {module, Mod};
        false -> undefined
    end.

get_stored_version(StoreRef, DbName, ViewId) ->
    case barrel_view_index:get_view_meta(StoreRef, DbName, ViewId) of
        {ok, #{version := V}} -> V;
        not_found -> undefined
    end.

update_stored_version(StoreRef, DbName, ViewId, Version) ->
    case barrel_view_index:get_view_meta(StoreRef, DbName, ViewId) of
        {ok, Meta} ->
            barrel_view_index:set_view_meta(StoreRef, DbName, ViewId, Meta#{version => Version});
        not_found ->
            ok
    end.

needs_update(first, _CurrentSeq) -> true;
needs_update(Since, CurrentSeq) ->
    barrel_sequence:compare(Since, CurrentSeq) < 0.
