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

%% Behaviour definition for module-based views
-callback version() -> pos_integer().
-callback map(Doc :: map()) -> [{Key :: term(), Value :: term()}].
-callback reduce(Keys :: [term()] | null, Values :: [term()], Rereduce :: boolean()) -> term().

-optional_callbacks([reduce/3]).

%% Query-based view types
-type view_key_spec() :: barrel_query:logic_var() | [barrel_query:logic_var()].
-type view_value_spec() :: barrel_query:logic_var() | literal | 1.
-type view_query_spec() :: #{
    where := [barrel_query:condition()],
    key := view_key_spec(),
    value => view_value_spec()
}.
-type refresh_mode() :: on_change | manual.

-export_type([view_query_spec/0, refresh_mode/0]).

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

%% API - Subscriptions
-export([
    subscribe/2,
    unsubscribe/2
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

%% @doc Wait for view to be up-to-date with current database HLC
-spec refresh(pid(), binary()) -> {ok, barrel_hlc:timestamp()} | {error, term()}.
refresh(DbServer, ViewId) ->
    case barrel_db_server:get_view_pid(DbServer, ViewId) of
        {ok, Pid} ->
            gen_statem:call(Pid, refresh, infinity);
        {error, _} = Error ->
            Error
    end.

%% @doc Wait for view to be up-to-date with given HLC
-spec refresh(pid(), binary(), barrel_hlc:timestamp()) -> {ok, barrel_hlc:timestamp()} | {error, term()}.
refresh(DbServer, ViewId, Hlc) ->
    case barrel_db_server:get_view_pid(DbServer, ViewId) of
        {ok, Pid} ->
            gen_statem:call(Pid, {refresh, Hlc}, infinity);
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% API - Subscriptions
%%====================================================================

%% @doc Subscribe to view index change notifications
%% Receives {barrel_view_change, DbName, ViewId, #{hlc := Hlc}} when view updates
-spec subscribe(pid(), binary()) -> {ok, reference()} | {error, term()}.
subscribe(DbServer, ViewId) ->
    case barrel_db_server:get_view_pid(DbServer, ViewId) of
        {ok, Pid} ->
            gen_statem:call(Pid, {subscribe, self()});
        {error, _} = Error ->
            Error
    end.

%% @doc Unsubscribe from view change notifications
-spec unsubscribe(pid(), reference()) -> ok | {error, term()}.
unsubscribe(DbServer, SubRef) ->
    %% We need to find the view that has this subscription
    %% For simplicity, broadcast to all views managed by this db server
    case barrel_db_server:list_views(DbServer) of
        {ok, Views} ->
            lists:foreach(fun(#{id := ViewId}) ->
                case barrel_db_server:get_view_pid(DbServer, ViewId) of
                    {ok, Pid} ->
                        gen_statem:cast(Pid, {unsubscribe, SubRef});
                    _ ->
                        ok
                end
            end, Views),
            ok;
        _ ->
            ok
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

init([DbName, StoreRef, #{id := ViewId} = Config]) ->
    process_flag(trap_exit, true),

    %% Determine view type: module-based or query-based
    InitResult = case Config of
        #{module := Mod} ->
            %% Module-based view
            ReduceFun = get_reduce_fun(Config, Mod),
            ModVersion = Mod:version(),
            {ok, module, {module, Mod}, ReduceFun, ModVersion};
        #{query := QuerySpec} ->
            %% Query-based view
            case compile_query_view(QuerySpec) of
                {ok, CompiledQuery} ->
                    ReduceFun = get_reduce_fun_from_config(Config),
                    %% Query-based views use hash of query spec as version
                    QueryVersion = erlang:phash2(QuerySpec),
                    {ok, query, {query, CompiledQuery}, ReduceFun, QueryVersion};
                {error, Reason} ->
                    {error, {invalid_query, Reason}}
            end
    end,

    case InitResult of
        {error, _} = Error ->
            {stop, Error};
        {ok, ViewType, MapFun, Reduce, CurrentVersion} ->
            %% Get refresh mode (defaults to on_change)
            RefreshMode = maps:get(refresh, Config, on_change),

            %% Check stored version vs current version
            StoredVersion = get_stored_version(StoreRef, DbName, ViewId),

            Since = case StoredVersion of
                undefined ->
                    %% New view - start from beginning
                    ok = barrel_view_index:set_view_meta(StoreRef, DbName, ViewId, #{
                        id => ViewId,
                        type => ViewType,
                        version => CurrentVersion,
                        reduce => Reduce =/= undefined,
                        refresh => RefreshMode
                    }),
                    first;
                CurrentVersion ->
                    %% Same version - resume from last indexed seq
                    barrel_view_index:get_indexed_seq(StoreRef, DbName, ViewId);
                OldVersion when OldVersion < CurrentVersion ->
                    %% Version bumped - clear and rebuild
                    ok = barrel_view_index:clear_all(StoreRef, DbName, ViewId),
                    ok = update_stored_version(StoreRef, DbName, ViewId, CurrentVersion),
                    first;
                _ ->
                    %% Version went backwards - clear and rebuild
                    ok = barrel_view_index:clear_all(StoreRef, DbName, ViewId),
                    ok = update_stored_version(StoreRef, DbName, ViewId, CurrentVersion),
                    first
            end,

            State = #{
                db_name => DbName,
                store_ref => StoreRef,
                view_id => ViewId,
                view_type => ViewType,
                map_fun => MapFun,
                reduce => Reduce,
                refresh_mode => RefreshMode,
                since => Since,
                waiters => [],
                pipeline => undefined,
                subscribers => #{},  % SubRef -> {Pid, MonRef}
                sub_monitors => #{}  % MonRef -> SubRef (for cleanup)
            },

            %% Schedule initial check for updates (skip for manual refresh)
            case RefreshMode of
                manual ->
                    {ok, idle, State};
                on_change ->
                    {ok, idle, State, [{state_timeout, 0, check_updates}]}
            end
    end.

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

idle({call, From}, {subscribe, Pid}, State) ->
    {Reply, NewState} = do_subscribe(Pid, State),
    {keep_state, NewState, [{reply, From, Reply}]};

idle(cast, {unsubscribe, SubRef}, State) ->
    NewState = do_unsubscribe(SubRef, State),
    {keep_state, NewState};

idle(info, {'DOWN', MonRef, process, _Pid, _Reason}, #{sub_monitors := SubMons} = State) ->
    case maps:get(MonRef, SubMons, undefined) of
        undefined ->
            {keep_state, State};
        SubRef ->
            NewState = do_unsubscribe(SubRef, State),
            {keep_state, NewState}
    end;

idle({call, From}, refresh, #{store_ref := StoreRef, db_name := DbName, since := Since} = State) ->
    %% Get current database HLC
    CurrentHlc = barrel_changes:get_last_hlc(StoreRef, DbName),
    case needs_update(Since, CurrentHlc) of
        false ->
            {keep_state, State, [{reply, From, {ok, Since}}]};
        true ->
            %% Start indexing and add to waiters
            Waiters = [{From, CurrentHlc}],
            NewState = State#{waiters => Waiters},
            start_indexing(NewState)
    end;

idle({call, From}, {refresh, TargetHlc}, #{since := Since} = State) ->
    case needs_update(Since, TargetHlc) of
        false ->
            {keep_state, State, [{reply, From, {ok, Since}}]};
        true ->
            Waiters = [{From, TargetHlc}],
            NewState = State#{waiters => Waiters},
            start_indexing(NewState)
    end;

idle(state_timeout, check_updates, #{store_ref := StoreRef, db_name := DbName, since := Since} = State) ->
    CurrentHlc = barrel_changes:get_last_hlc(StoreRef, DbName),
    case needs_update(Since, CurrentHlc) of
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

indexing({call, From}, {subscribe, Pid}, State) ->
    {Reply, NewState} = do_subscribe(Pid, State),
    {keep_state, NewState, [{reply, From, Reply}]};

indexing(cast, {unsubscribe, SubRef}, State) ->
    NewState = do_unsubscribe(SubRef, State),
    {keep_state, NewState};

indexing(info, {'DOWN', MonRef, process, _Pid, _Reason}, #{sub_monitors := SubMons} = State) ->
    case maps:get(MonRef, SubMons, undefined) of
        undefined ->
            {keep_state, State};
        SubRef ->
            NewState = do_unsubscribe(SubRef, State),
            {keep_state, NewState}
    end;

indexing({call, From}, refresh, #{store_ref := StoreRef, db_name := DbName, waiters := Waiters} = State) ->
    CurrentHlc = barrel_changes:get_last_hlc(StoreRef, DbName),
    NewWaiters = [{From, CurrentHlc} | Waiters],
    {keep_state, State#{waiters => NewWaiters}};

indexing({call, From}, {refresh, TargetHlc}, #{waiters := Waiters} = State) ->
    NewWaiters = [{From, TargetHlc} | Waiters],
    {keep_state, State#{waiters => NewWaiters}};

indexing(info, {index_updated, NewHlc}, #{waiters := Waiters} = State) ->
    %% Notify waiters whose target HLC has been reached
    {Satisfied, Remaining} = lists:partition(
        fun({_From, TargetHlc}) -> not needs_update(NewHlc, TargetHlc) end,
        Waiters
    ),
    lists:foreach(
        fun({From, _}) -> gen_statem:reply(From, {ok, NewHlc}) end,
        Satisfied
    ),
    %% Notify view subscribers
    notify_view_subscribers(NewHlc, State),
    {keep_state, State#{since => NewHlc, waiters => Remaining}};

indexing(info, {index_complete, FinalHlc}, #{waiters := Waiters} = State) ->
    %% Pipeline finished - notify remaining waiters and go idle
    lists:foreach(
        fun({From, _}) -> gen_statem:reply(From, {ok, FinalHlc}) end,
        Waiters
    ),
    %% Notify view subscribers
    notify_view_subscribers(FinalHlc, State),
    NewState = State#{since => FinalHlc, waiters => [], pipeline => undefined},
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
                 map_fun := MapFun, since := Since} = State) ->
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
            map_fun => MapFun,
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
handle_pipeline_exit({index_complete, Hlc}, State) ->
    %% Pipeline completed successfully
    indexing(info, {index_complete, Hlc}, State);
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
    FoldFun = fun(Change, {Count, Batch, _LastHlc}) ->
        NewBatch = [Change | Batch],
        NewCount = Count + 1,
        ChangeHlc = maps:get(hlc, Change),
        case NewCount >= ?BATCH_SIZE of
            true ->
                %% Send batch to mapper with backpressure
                send_batch(Mapper, lists:reverse(NewBatch), ChangeHlc),
                {ok, {0, [], ChangeHlc}};
            false ->
                {ok, {NewCount, NewBatch, ChangeHlc}}
        end
    end,

    {ok, {_, FinalBatch, FinalHlc}, _} = barrel_changes:fold_changes(
        StoreRef, DbName, Since, FoldFun, {0, [], Since}
    ),

    %% Send final batch if any
    case FinalBatch of
        [] -> ok;
        _ -> send_batch(Mapper, lists:reverse(FinalBatch), FinalHlc)
    end,

    %% Signal end of changes
    Mapper ! {done, FinalHlc},

    %% Release snapshot and exit
    barrel_store_rocksdb:release_snapshot(Snapshot),
    exit({index_complete, FinalHlc}).

send_batch(Mapper, Batch, Hlc) ->
    Mapper ! {batch, self(), Batch, Hlc},
    receive
        {ack, Mapper} -> ok
    end.

%%====================================================================
%% Pipeline: Mapper Process
%%====================================================================

mapper_loop(#{map_fun := MapFun, writer := Writer} = State) ->
    receive
        {batch, Reader, Changes, Hlc} ->
            %% Map each document using appropriate map function
            Mapped = lists:map(
                fun(Change) ->
                    DocId = maps:get(id, Change),
                    Doc = maps:get(doc, Change, #{}),
                    Deleted = maps:get(deleted, Change, false),
                    Entries = case Deleted of
                        true -> [];
                        false ->
                            apply_map_fun(MapFun, Doc)
                    end,
                    {DocId, Entries, Deleted}
                end,
                Changes
            ),

            %% Send to writer with backpressure
            Writer ! {mapped, self(), Mapped, Hlc},
            receive
                {ack, Writer} -> ok
            end,

            %% Ack reader
            Reader ! {ack, self()},
            mapper_loop(State);

        {done, FinalHlc} ->
            Writer ! {done, FinalHlc},
            exit(normal)
    end.

%% @doc Apply map function (module-based or query-based)
apply_map_fun({module, Mod}, Doc) ->
    try Mod:map(Doc)
    catch _:_ -> []
    end;
apply_map_fun({query, CompiledQuery}, Doc) ->
    query_map(Doc, CompiledQuery).

%% @doc Map a document using a compiled query
%% Returns [{Key, Value}] if document matches, [] otherwise
query_map(Doc, #{conditions := Conditions, bindings := Bindings,
                  key_spec := KeySpec, value_spec := ValueSpec}) ->
    case matches_query_conditions(Doc, Conditions, Bindings) of
        {true, BoundVars} ->
            Key = extract_key(KeySpec, BoundVars),
            Value = extract_value(ValueSpec, BoundVars),
            [{Key, Value}];
        false ->
            []
    end.

%% @doc Check if document matches query conditions
matches_query_conditions(Doc, Conditions, InitBindings) ->
    matches_conditions_loop(Doc, Conditions, InitBindings, #{}).

matches_conditions_loop(_Doc, [], _Bindings, BoundVars) ->
    {true, BoundVars};
matches_conditions_loop(Doc, [Condition | Rest], Bindings, BoundVars) ->
    case match_query_condition(Doc, Condition, Bindings, BoundVars) of
        {true, NewBoundVars} ->
            matches_conditions_loop(Doc, Rest, Bindings, NewBoundVars);
        false ->
            false
    end.

match_query_condition(Doc, {path, Path, Value}, _Bindings, BoundVars) ->
    case get_doc_path_value(Doc, Path) of
        {ok, DocValue} ->
            case barrel_query:is_logic_var(Value) of
                true ->
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

match_query_condition(Doc, {compare, Path, Op, Value}, _Bindings, BoundVars) ->
    case get_doc_path_value(Doc, Path) of
        {ok, DocValue} ->
            CompareValue = case barrel_query:is_logic_var(Value) of
                true -> maps:get(Value, BoundVars, undefined);
                false -> Value
            end,
            case compare_query_values(DocValue, Op, CompareValue) of
                true -> {true, BoundVars};
                false -> false
            end;
        not_found ->
            false
    end;

match_query_condition(Doc, {'and', Conditions}, Bindings, BoundVars) ->
    matches_conditions_loop(Doc, Conditions, Bindings, BoundVars);

match_query_condition(Doc, {'or', Conditions}, Bindings, BoundVars) ->
    match_query_any(Doc, Conditions, Bindings, BoundVars);

match_query_condition(Doc, {'not', Condition}, Bindings, BoundVars) ->
    case match_query_condition(Doc, Condition, Bindings, BoundVars) of
        {true, _} -> false;
        false -> {true, BoundVars}
    end;

match_query_condition(Doc, {in, Path, Values}, _Bindings, BoundVars) ->
    case get_doc_path_value(Doc, Path) of
        {ok, DocValue} ->
            case lists:member(DocValue, Values) of
                true -> {true, BoundVars};
                false -> false
            end;
        not_found ->
            false
    end;

match_query_condition(Doc, {contains, Path, Value}, _Bindings, BoundVars) ->
    case get_doc_path_value(Doc, Path) of
        {ok, DocValue} when is_list(DocValue) ->
            case lists:member(Value, DocValue) of
                true -> {true, BoundVars};
                false -> false
            end;
        _ ->
            false
    end;

match_query_condition(Doc, {exists, Path}, _Bindings, BoundVars) ->
    case get_doc_path_value(Doc, Path) of
        {ok, _} -> {true, BoundVars};
        not_found -> false
    end;

match_query_condition(Doc, {missing, Path}, _Bindings, BoundVars) ->
    case get_doc_path_value(Doc, Path) of
        {ok, _} -> false;
        not_found -> {true, BoundVars}
    end;

match_query_condition(Doc, {regex, Path, Pattern}, _Bindings, BoundVars) ->
    case get_doc_path_value(Doc, Path) of
        {ok, DocValue} when is_binary(DocValue) ->
            case re:run(DocValue, Pattern) of
                {match, _} -> {true, BoundVars};
                nomatch -> false
            end;
        _ ->
            false
    end;

match_query_condition(Doc, {prefix, Path, Prefix}, _Bindings, BoundVars) ->
    case get_doc_path_value(Doc, Path) of
        {ok, DocValue} when is_binary(DocValue) ->
            PrefixLen = byte_size(Prefix),
            case DocValue of
                <<Prefix:PrefixLen/binary, _/binary>> -> {true, BoundVars};
                _ -> false
            end;
        _ ->
            false
    end;

match_query_condition(_Doc, _, _Bindings, _BoundVars) ->
    false.

match_query_any(_Doc, [], _Bindings, _BoundVars) ->
    false;
match_query_any(Doc, [Condition | Rest], Bindings, BoundVars) ->
    case match_query_condition(Doc, Condition, Bindings, BoundVars) of
        {true, NewBoundVars} -> {true, NewBoundVars};
        false -> match_query_any(Doc, Rest, Bindings, BoundVars)
    end.

%% @doc Get a value from document at the given path
get_doc_path_value(Doc, []) ->
    {ok, Doc};
get_doc_path_value(Doc, [Key | Rest]) when is_map(Doc), is_binary(Key) ->
    case maps:find(Key, Doc) of
        {ok, Value} -> get_doc_path_value(Value, Rest);
        error -> not_found
    end;
get_doc_path_value(Doc, [Index | Rest]) when is_list(Doc), is_integer(Index) ->
    case Index < length(Doc) of
        true ->
            Value = lists:nth(Index + 1, Doc),
            get_doc_path_value(Value, Rest);
        false ->
            not_found
    end;
get_doc_path_value(_, _) ->
    not_found.

%% @doc Compare two values with an operator
compare_query_values(A, '>', B) when is_number(A), is_number(B) -> A > B;
compare_query_values(A, '<', B) when is_number(A), is_number(B) -> A < B;
compare_query_values(A, '>=', B) when is_number(A), is_number(B) -> A >= B;
compare_query_values(A, '=<', B) when is_number(A), is_number(B) -> A =< B;
compare_query_values(A, '=/=', B) -> A =/= B;
compare_query_values(A, '==', B) -> A =:= B;
compare_query_values(A, '>', B) when is_binary(A), is_binary(B) -> A > B;
compare_query_values(A, '<', B) when is_binary(A), is_binary(B) -> A < B;
compare_query_values(A, '>=', B) when is_binary(A), is_binary(B) -> A >= B;
compare_query_values(A, '=<', B) when is_binary(A), is_binary(B) -> A =< B;
compare_query_values(_, _, _) -> false.

%% @doc Extract key from bound variables
extract_key(KeySpec, BoundVars) when is_atom(KeySpec) ->
    maps:get(KeySpec, BoundVars, null);
extract_key(KeySpec, BoundVars) when is_list(KeySpec) ->
    [maps:get(K, BoundVars, null) || K <- KeySpec].

%% @doc Extract value from bound variables
extract_value(ValueSpec, BoundVars) when is_atom(ValueSpec) ->
    case barrel_query:is_logic_var(ValueSpec) of
        true -> maps:get(ValueSpec, BoundVars, null);
        false -> 1  % Default value for counting
    end;
extract_value(1, _BoundVars) ->
    1;
extract_value(literal, _BoundVars) ->
    1.

%%====================================================================
%% Pipeline: Writer Process
%%====================================================================

writer_loop(#{store_ref := StoreRef, db_name := DbName, view_id := ViewId,
              parent := Parent} = State) ->
    receive
        {mapped, Mapper, Entries, Hlc} ->
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

            %% Update indexed HLC
            barrel_view_index:set_indexed_seq(StoreRef, DbName, ViewId, Hlc),

            %% Notify parent of progress
            Parent ! {index_updated, Hlc},

            %% Ack mapper
            Mapper ! {ack, self()},
            writer_loop(State);

        {done, FinalHlc} ->
            %% Notify parent of completion
            Parent ! {index_complete, FinalHlc},
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

%% @doc Get reduce function from config (for query-based views)
get_reduce_fun_from_config(#{reduce := '_count'}) -> {builtin, '_count'};
get_reduce_fun_from_config(#{reduce := '_sum'}) -> {builtin, '_sum'};
get_reduce_fun_from_config(#{reduce := '_stats'}) -> {builtin, '_stats'};
get_reduce_fun_from_config(_) -> undefined.

%% @doc Compile a query spec for use in a view
%% The query spec must include:
%%   - where: list of conditions (same as barrel_query)
%%   - key: variable or list of variables to use as index key
%%   - value (optional): variable or 1 for counting
compile_query_view(#{where := Where, key := KeySpec} = QuerySpec) ->
    %% Validate the where clause
    case barrel_query:validate_spec(#{where => Where}) of
        ok ->
            %% Normalize conditions
            NormalizedConditions = [barrel_query:normalize_condition(C) || C <- Where],
            %% Extract bindings (variable -> path mappings)
            Bindings = extract_query_bindings(NormalizedConditions),
            %% Get value spec
            ValueSpec = maps:get(value, QuerySpec, 1),
            %% Validate key spec references bound variables
            case validate_key_spec(KeySpec, Bindings) of
                ok ->
                    {ok, #{
                        conditions => NormalizedConditions,
                        bindings => Bindings,
                        key_spec => KeySpec,
                        value_spec => ValueSpec
                    }};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end;
compile_query_view(#{where := _}) ->
    {error, {missing_key_spec, "Query-based views require a 'key' specification"}};
compile_query_view(_) ->
    {error, {missing_where_clause, "Query-based views require a 'where' clause"}}.

%% @doc Extract variable bindings from conditions
extract_query_bindings(Conditions) ->
    extract_query_bindings(Conditions, #{}).

extract_query_bindings([], Acc) ->
    Acc;
extract_query_bindings([{path, Path, Value} | Rest], Acc) ->
    case barrel_query:is_logic_var(Value) of
        true ->
            extract_query_bindings(Rest, Acc#{Value => Path});
        false ->
            extract_query_bindings(Rest, Acc)
    end;
extract_query_bindings([{compare, Path, _Op, Value} | Rest], Acc) ->
    case barrel_query:is_logic_var(Value) of
        true ->
            extract_query_bindings(Rest, Acc#{Value => Path});
        false ->
            extract_query_bindings(Rest, Acc)
    end;
extract_query_bindings([{'and', Nested} | Rest], Acc) ->
    NestedBindings = extract_query_bindings(Nested, Acc),
    extract_query_bindings(Rest, NestedBindings);
extract_query_bindings([{'or', _} | Rest], Acc) ->
    %% OR bindings are tricky - skip for now
    extract_query_bindings(Rest, Acc);
extract_query_bindings([_ | Rest], Acc) ->
    extract_query_bindings(Rest, Acc).

%% @doc Validate that key spec references bound variables
validate_key_spec(KeySpec, Bindings) when is_atom(KeySpec) ->
    case barrel_query:is_logic_var(KeySpec) of
        true ->
            case maps:is_key(KeySpec, Bindings) of
                true -> ok;
                false -> {error, {unbound_variable, KeySpec}}
            end;
        false ->
            {error, {invalid_key_spec, KeySpec}}
    end;
validate_key_spec(KeySpec, Bindings) when is_list(KeySpec) ->
    case lists:all(
        fun(K) ->
            barrel_query:is_logic_var(K) andalso maps:is_key(K, Bindings)
        end,
        KeySpec
    ) of
        true -> ok;
        false -> {error, {invalid_key_spec, KeySpec}}
    end;
validate_key_spec(KeySpec, _Bindings) ->
    {error, {invalid_key_spec, KeySpec}}.

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

needs_update(first, _CurrentHlc) -> true;
needs_update(Since, CurrentHlc) ->
    barrel_hlc:less(Since, CurrentHlc).

%%====================================================================
%% Internal - Subscriptions
%%====================================================================

%% @doc Subscribe a process to view change notifications
do_subscribe(Pid, #{subscribers := Subs, sub_monitors := SubMons} = State) ->
    SubRef = make_ref(),
    MonRef = erlang:monitor(process, Pid),
    NewSubs = maps:put(SubRef, {Pid, MonRef}, Subs),
    NewSubMons = maps:put(MonRef, SubRef, SubMons),
    {{ok, SubRef}, State#{subscribers => NewSubs, sub_monitors => NewSubMons}}.

%% @doc Unsubscribe from view change notifications
do_unsubscribe(SubRef, #{subscribers := Subs, sub_monitors := SubMons} = State) ->
    case maps:get(SubRef, Subs, undefined) of
        undefined ->
            State;
        {_Pid, MonRef} ->
            erlang:demonitor(MonRef, [flush]),
            NewSubs = maps:remove(SubRef, Subs),
            NewSubMons = maps:remove(MonRef, SubMons),
            State#{subscribers => NewSubs, sub_monitors => NewSubMons}
    end.

%% @doc Notify all view subscribers of an index update
notify_view_subscribers(Hlc, #{db_name := DbName, view_id := ViewId, subscribers := Subs}) ->
    Notification = {barrel_view_change, DbName, ViewId, #{hlc => Hlc}},
    maps:foreach(fun(_SubRef, {Pid, _MonRef}) ->
        Pid ! Notification
    end, Subs).
