%%%-------------------------------------------------------------------
%%% @doc Posting List Writer for barrel_docdb
%%%
%%% Handles concurrent posting list updates by serializing writes
%%% through a dedicated process per database. This allows:
%%% - Efficient multi_get for reads (no transactions needed)
%%% - Atomic batch updates for writes
%%% - Concurrent write safety via message passing
%%%
%%% Each database has its own posting writer to avoid cross-db contention.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_posting_writer).
-behaviour(gen_server).

-include("barrel_docdb.hrl").

%% API
-export([start_link/2, stop/1]).
-export([add/5, remove/5, batch_update/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    db_name :: db_name(),
    store_ref :: barrel_store_rocksdb:db_ref()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a posting writer for a database
-spec start_link(db_name(), barrel_store_rocksdb:db_ref()) -> {ok, pid()} | {error, term()}.
start_link(DbName, StoreRef) ->
    gen_server:start_link(?MODULE, [DbName, StoreRef], []).

%% @doc Stop the posting writer
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc Add a document ID to a posting list
-spec add(pid(), db_name(), barrel_path_dict:path_id(), term(), docid()) ->
    ok | {error, term()}.
add(Pid, DbName, PathId, Value, DocId) ->
    gen_server:call(Pid, {update, DbName, PathId, Value, DocId, add}).

%% @doc Remove a document ID from a posting list
-spec remove(pid(), db_name(), barrel_path_dict:path_id(), term(), docid()) ->
    ok | {error, term()}.
remove(Pid, DbName, PathId, Value, DocId) ->
    gen_server:call(Pid, {update, DbName, PathId, Value, DocId, remove}).

%% @doc Apply a batch of posting updates atomically
%% Updates is a list of {PathId, Value, DocId, add|remove}
-spec batch_update(pid(), [{barrel_path_dict:path_id(), term(), docid(), add | remove}]) ->
    ok | {error, term()}.
batch_update(Pid, Updates) ->
    gen_server:call(Pid, {batch_update, Updates}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([DbName, StoreRef]) ->
    {ok, #state{db_name = DbName, store_ref = StoreRef}}.

handle_call({update, DbName, PathId, Value, DocId, Op}, _From,
            #state{store_ref = StoreRef} = State) ->
    Result = do_update(StoreRef, DbName, PathId, Value, DocId, Op),
    {reply, Result, State};

handle_call({batch_update, Updates}, _From,
            #state{db_name = DbName, store_ref = StoreRef} = State) ->
    Result = do_batch_update(StoreRef, DbName, Updates),
    {reply, Result, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Apply a single posting update
do_update(StoreRef, DbName, PathId, Value, DocId, Op) ->
    Key = barrel_posting:posting_key(DbName, PathId, Value),
    %% Read current posting list
    Current = case barrel_store_rocksdb:posting_get(StoreRef, Key) of
        {ok, Bin} -> barrel_posting:decode_posting(Bin);
        not_found -> []
    end,
    %% Apply operation
    Updated = case Op of
        add -> ordsets:add_element(DocId, Current);
        remove -> ordsets:del_element(DocId, Current)
    end,
    %% Write back
    write_posting(StoreRef, Key, Updated).

%% @private Apply a batch of posting updates
%% Groups updates by key for efficiency
do_batch_update(StoreRef, DbName, Updates) ->
    %% Group updates by key
    Grouped = group_updates(DbName, Updates),
    %% Process each key
    maps:fold(fun(Key, Ops, AccResult) ->
        case AccResult of
            ok ->
                %% Read current posting list
                Current = case barrel_store_rocksdb:posting_get(StoreRef, Key) of
                    {ok, Bin} -> barrel_posting:decode_posting(Bin);
                    not_found -> []
                end,
                %% Apply all operations for this key
                Updated = lists:foldl(fun({DocId, Op}, Acc) ->
                    case Op of
                        add -> ordsets:add_element(DocId, Acc);
                        remove -> ordsets:del_element(DocId, Acc)
                    end
                end, Current, Ops),
                %% Write back
                write_posting(StoreRef, Key, Updated);
            {error, _} = Err ->
                Err
        end
    end, ok, Grouped).

%% @private Group updates by posting key
group_updates(DbName, Updates) ->
    lists:foldl(fun({PathId, Value, DocId, Op}, Acc) ->
        Key = barrel_posting:posting_key(DbName, PathId, Value),
        Existing = maps:get(Key, Acc, []),
        maps:put(Key, [{DocId, Op} | Existing], Acc)
    end, #{}, Updates).

%% @private Write posting list back to store
write_posting(StoreRef, Key, []) ->
    %% Delete empty posting lists to save space
    barrel_store_rocksdb:posting_delete(StoreRef, Key);
write_posting(StoreRef, Key, Updated) ->
    barrel_store_rocksdb:posting_put(StoreRef, Key, barrel_posting:encode_posting(Updated)).
