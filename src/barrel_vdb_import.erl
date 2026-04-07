%%%-------------------------------------------------------------------
%%% @doc VDB Import - Import data from regular database to VDB
%%%
%%% Provides functionality to import documents from a regular barrel
%%% database to a Virtual Database (VDB), distributing them across shards.
%%%
%%% Example:
%%% ```
%%% %% Import all documents from "legacy_users" to VDB "users"
%%% {ok, Stats} = barrel_vdb_import:import(<<"legacy_users">>, <<"users">>, #{}).
%%%
%%% %% Import with filter
%%% {ok, Stats} = barrel_vdb_import:import(<<"legacy">>, <<"users">>, #{
%%%     filter => #{
%%%         where => [{path, [<<"type">>], <<"user">>}]
%%%     }
%%% }).
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vdb_import).

%% API
-export([
    import/3,
    import_async/3,
    get_status/1,
    cancel/1
]).

%% Internal
-export([import_worker/4]).

%%====================================================================
%% Types
%%====================================================================

-type import_opts() :: #{
    batch_size => pos_integer(),
    filter => map(),
    transform => fun((map()) -> map()),
    on_conflict => skip | overwrite | merge
}.

-type import_stats() :: #{
    docs_read => non_neg_integer(),
    docs_written => non_neg_integer(),
    docs_skipped => non_neg_integer(),
    errors => non_neg_integer(),
    started_at => integer(),
    finished_at => integer() | undefined,
    status => running | completed | failed | cancelled
}.

-export_type([import_opts/0, import_stats/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Import documents from a source database to a VDB (synchronous)
-spec import(binary(), binary(), import_opts()) -> {ok, import_stats()} | {error, term()}.
import(SourceDb, TargetVdb, Opts) when is_binary(SourceDb), is_binary(TargetVdb) ->
    %% Validate source exists
    case barrel_docdb:db_info(SourceDb) of
        {error, not_found} ->
            {error, {source_not_found, SourceDb}};
        {ok, _} ->
            %% Validate target VDB exists
            case barrel_vdb:exists(TargetVdb) of
                false ->
                    {error, {vdb_not_found, TargetVdb}};
                true ->
                    do_import(SourceDb, TargetVdb, Opts)
            end
    end.

%% @doc Import documents asynchronously, returns task ID
%% Uses spawn + monitor instead of spawn_link so:
%% - Caller exit doesn't kill the import
%% - Worker crash doesn't kill the caller
-spec import_async(binary(), binary(), import_opts()) -> {ok, binary()} | {error, term()}.
import_async(SourceDb, TargetVdb, Opts) when is_binary(SourceDb), is_binary(TargetVdb) ->
    %% Validate source exists
    case barrel_docdb:db_info(SourceDb) of
        {error, not_found} ->
            {error, {source_not_found, SourceDb}};
        {ok, _} ->
            %% Validate target VDB exists
            case barrel_vdb:exists(TargetVdb) of
                false ->
                    {error, {vdb_not_found, TargetVdb}};
                true ->
                    TaskId = generate_task_id(),
                    %% Use spawn (not spawn_link) for isolation
                    Pid = spawn(?MODULE, import_worker, [TaskId, SourceDb, TargetVdb, Opts]),
                    %% Monitor for crash detection
                    MonRef = erlang:monitor(process, Pid),
                    register_task(TaskId, Pid, MonRef),
                    {ok, TaskId}
            end
    end.

%% @doc Get status of an async import task
-spec get_status(binary()) -> {ok, import_stats()} | {error, not_found}.
get_status(TaskId) ->
    case get_task(TaskId) of
        {ok, Stats} -> {ok, Stats};
        error -> {error, not_found}
    end.

%% @doc Cancel an async import task
-spec cancel(binary()) -> ok | {error, not_found}.
cancel(TaskId) ->
    case get_task(TaskId) of
        {ok, #{pid := Pid, mon_ref := MonRef}} ->
            erlang:demonitor(MonRef, [flush]),
            exit(Pid, cancelled),
            update_task_status(TaskId, cancelled),
            %% Schedule cleanup
            schedule_cleanup(TaskId, 300000),
            ok;
        {ok, #{pid := Pid}} ->
            %% Old format without mon_ref
            exit(Pid, cancelled),
            update_task_status(TaskId, cancelled),
            schedule_cleanup(TaskId, 300000),
            ok;
        _ ->
            {error, not_found}
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Perform the actual import
do_import(SourceDb, TargetVdb, Opts) ->
    BatchSize = maps:get(batch_size, Opts, 100),
    Filter = maps:get(filter, Opts, undefined),
    Transform = maps:get(transform, Opts, fun(D) -> D end),
    OnConflict = maps:get(on_conflict, Opts, skip),

    Stats0 = #{
        docs_read => 0,
        docs_written => 0,
        docs_skipped => 0,
        errors => 0,
        started_at => erlang:system_time(millisecond),
        finished_at => undefined,
        status => running
    },

    %% Fold over source documents
    FoldFun = fun(Doc, Acc) ->
        #{docs_read := Read} = Acc,
        Acc1 = Acc#{docs_read => Read + 1},

        %% Apply filter if specified
        case should_include(Doc, Filter) of
            false ->
                #{docs_skipped := Skipped} = Acc1,
                {ok, Acc1#{docs_skipped => Skipped + 1}};
            true ->
                %% Transform document
                TransformedDoc = Transform(Doc),

                %% Remove revision for fresh insert
                DocToWrite = prepare_doc_for_import(TransformedDoc, OnConflict),

                %% Write to VDB (routed to correct shard)
                case barrel_vdb:put_doc(TargetVdb, DocToWrite) of
                    {ok, _} ->
                        #{docs_written := Written} = Acc1,
                        {ok, Acc1#{docs_written => Written + 1}};
                    {error, conflict} when OnConflict =:= skip ->
                        #{docs_skipped := Skipped} = Acc1,
                        {ok, Acc1#{docs_skipped => Skipped + 1}};
                    {error, _} ->
                        #{errors := Errors} = Acc1,
                        {ok, Acc1#{errors => Errors + 1}}
                end
        end
    end,

    {ok, FinalStats} = barrel_docdb:fold_docs(SourceDb, FoldFun, Stats0, #{batch_size => BatchSize}),
    {ok, FinalStats#{
          finished_at => erlang:system_time(millisecond),
          status => completed
        }}.

%% @private Check if document matches filter
should_include(_Doc, undefined) ->
    true;
should_include(Doc, #{where := Conditions}) ->
    barrel_query:matches(Doc, Conditions);
should_include(_Doc, _) ->
    true.

%% @private Prepare document for import
prepare_doc_for_import(Doc, _OnConflict) ->
    %% Remove internal revision fields for fresh insert
    Doc1 = maps:remove(<<"_rev">>, Doc),
    maps:remove(<<"rev">>, Doc1).

%% @private Worker for async import
import_worker(TaskId, SourceDb, TargetVdb, Opts) ->
    try
        {ok, Stats} = do_import(SourceDb, TargetVdb, Opts),
        store_task_result(TaskId, Stats),
        %% Schedule cleanup after a delay (keep result available for 1 hour)
        schedule_cleanup(TaskId, 3600000)
    catch
        exit:cancelled ->
            store_task_result(TaskId, #{
                status => cancelled,
                finished_at => erlang:system_time(millisecond)
            }),
            schedule_cleanup(TaskId, 300000);  %% 5 minutes for cancelled
        _:Error ->
            store_task_result(TaskId, #{
                status => failed,
                error => Error,
                finished_at => erlang:system_time(millisecond)
            }),
            schedule_cleanup(TaskId, 300000)  %% 5 minutes for failed
    end.

%% @private Generate unique task ID
generate_task_id() ->
    Rand = crypto:strong_rand_bytes(8),
    base64:encode(Rand, #{mode => urlsafe, padding => false}).

%% @private Task storage (using persistent_term for simplicity)
%% Tasks are automatically cleaned up after completion/cancellation/failure
register_task(TaskId, Pid, MonRef) ->
    persistent_term:put({vdb_import_task, TaskId}, #{
        pid => Pid,
        mon_ref => MonRef,
        status => running,
        started_at => erlang:system_time(millisecond)
    }).

get_task(TaskId) ->
    try
        persistent_term:get({vdb_import_task, TaskId})
    of
        Stats -> {ok, Stats}
    catch
        error:badarg -> error
    end.

update_task_status(TaskId, Status) ->
    case get_task(TaskId) of
        {ok, Stats} ->
            persistent_term:put({vdb_import_task, TaskId}, Stats#{status => Status});
        error ->
            ok
    end.

store_task_result(TaskId, Stats) ->
    %% Preserve pid and mon_ref from original registration
    case get_task(TaskId) of
        {ok, #{pid := Pid, mon_ref := MonRef}} ->
            persistent_term:put({vdb_import_task, TaskId},
                                Stats#{pid => Pid, mon_ref => MonRef});
        _ ->
            persistent_term:put({vdb_import_task, TaskId}, Stats)
    end.

%% @private Schedule cleanup of task data
schedule_cleanup(TaskId, DelayMs) ->
    spawn(fun() ->
        timer:sleep(DelayMs),
        cleanup_task(TaskId)
    end).

%% @private Clean up task data from persistent_term
cleanup_task(TaskId) ->
    case get_task(TaskId) of
        {ok, #{mon_ref := MonRef}} ->
            %% Demonitor if still valid
            erlang:demonitor(MonRef, [flush]);
        _ ->
            ok
    end,
    %% Erase the task
    catch persistent_term:erase({vdb_import_task, TaskId}),
    ok.
