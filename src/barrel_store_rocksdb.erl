%%%-------------------------------------------------------------------
%%% @doc RocksDB storage backend for barrel_docdb
%%%
%%% Implements the barrel_store behaviour using RocksDB 2.0.0.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_store_rocksdb).
-behaviour(barrel_store).

%% barrel_store callbacks
-export([open/2, close/1]).
-export([put/3, put/4, get/2, delete/2]).
-export([write_batch/2]).
-export([fold/4, fold_range/5]).

%% Additional utilities
-export([snapshot/1, release_snapshot/1]).
-export([get_with_snapshot/3]).

%%====================================================================
%% Types
%%====================================================================

-type db_ref() :: #{
    ref := rocksdb:db_handle(),
    path := string()
}.

-type snapshot() :: rocksdb:snapshot_handle().

-export_type([db_ref/0, snapshot/0]).

%%====================================================================
%% barrel_store callbacks
%%====================================================================

%% @doc Open a RocksDB database
-spec open(string(), map()) -> {ok, db_ref()} | {error, term()}.
open(Path, Options) ->
    ok = filelib:ensure_dir(Path ++ "/"),
    DbOpts = build_db_options(Options),
    case rocksdb:open(Path, DbOpts) of
        {ok, Ref} ->
            {ok, #{ref => Ref, path => Path}};
        {error, Reason} ->
            {error, {db_open_failed, Reason}}
    end.

%% @doc Close the database
-spec close(db_ref()) -> ok.
close(#{ref := Ref}) ->
    rocksdb:close(Ref).

%% @doc Put a key-value pair
-spec put(db_ref(), binary(), binary()) -> ok | {error, term()}.
put(DbRef, Key, Value) ->
    put(DbRef, Key, Value, []).

%% @doc Put a key-value pair with options
-spec put(db_ref(), binary(), binary(), list()) -> ok | {error, term()}.
put(#{ref := Ref}, Key, Value, Opts) ->
    rocksdb:put(Ref, Key, Value, Opts).

%% @doc Get a value by key
-spec get(db_ref(), binary()) -> {ok, binary()} | not_found | {error, term()}.
get(#{ref := Ref}, Key) ->
    rocksdb:get(Ref, Key, []).

%% @doc Delete a key
-spec delete(db_ref(), binary()) -> ok | {error, term()}.
delete(#{ref := Ref}, Key) ->
    rocksdb:delete(Ref, Key, []).

%% @doc Execute a batch of operations atomically
-spec write_batch(db_ref(), list()) -> ok | {error, term()}.
write_batch(#{ref := Ref}, Operations) ->
    {ok, Batch} = rocksdb:batch(),
    try
        lists:foreach(
            fun({put, Key, Value}) ->
                ok = rocksdb:batch_put(Batch, Key, Value);
               ({delete, Key}) ->
                ok = rocksdb:batch_delete(Batch, Key)
            end,
            Operations
        ),
        Result = rocksdb:write_batch(Ref, Batch, [{sync, true}]),
        Result
    after
        rocksdb:release_batch(Batch)
    end.

%% @doc Fold over all keys with a given prefix
-spec fold(db_ref(), binary(), fun(), term()) -> term().
fold(#{ref := Ref}, Prefix, Fun, Acc) ->
    PrefixEnd = prefix_end(Prefix),
    ReadOpts = [
        {iterate_lower_bound, Prefix},
        {iterate_upper_bound, PrefixEnd}
    ],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        fold_loop(rocksdb:iterator_move(Itr, first), Itr, Fun, Acc)
    after
        rocksdb:iterator_close(Itr)
    end.

%% @doc Fold over a key range
-spec fold_range(db_ref(), binary(), binary(), fun(), term()) -> term().
fold_range(#{ref := Ref}, StartKey, EndKey, Fun, Acc) ->
    ReadOpts = [
        {iterate_lower_bound, StartKey},
        {iterate_upper_bound, EndKey}
    ],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        fold_loop(rocksdb:iterator_move(Itr, first), Itr, Fun, Acc)
    after
        rocksdb:iterator_close(Itr)
    end.

%%====================================================================
%% Snapshot Operations
%%====================================================================

%% @doc Create a snapshot for consistent reads
-spec snapshot(db_ref()) -> {ok, snapshot()} | {error, term()}.
snapshot(#{ref := Ref}) ->
    rocksdb:snapshot(Ref).

%% @doc Release a snapshot
-spec release_snapshot(snapshot()) -> ok.
release_snapshot(Snapshot) ->
    rocksdb:release_snapshot(Snapshot).

%% @doc Get with a snapshot for consistent reads
-spec get_with_snapshot(db_ref(), binary(), snapshot()) ->
    {ok, binary()} | not_found | {error, term()}.
get_with_snapshot(#{ref := Ref}, Key, Snapshot) ->
    rocksdb:get(Ref, Key, [{snapshot, Snapshot}]).

%%====================================================================
%% Internal Functions
%%====================================================================

%% Build RocksDB options from config
build_db_options(Options) ->
    WriteBufferSize = maps:get(write_buffer_size, Options, 64 * 1024 * 1024),
    [
        {create_if_missing, true},
        {max_open_files, maps:get(max_open_files, Options, 1000)},
        {write_buffer_size, WriteBufferSize},
        {compression, maps:get(compression, Options, snappy)},
        {allow_concurrent_memtable_write, true},
        {enable_write_thread_adaptive_yield, true},
        %% Use counter merge operator for atomic counters
        {merge_operator, counter_merge_operator},
        {total_threads, erlang:max(4, erlang:system_info(schedulers))}
    ].

%% Compute the end of a prefix range (for iteration bounds)
prefix_end(Prefix) ->
    case Prefix of
        <<>> ->
            <<16#FF>>;
        _ ->
            Len = byte_size(Prefix),
            LastByte = binary:last(Prefix),
            if
                LastByte < 16#FF ->
                    Init = binary:part(Prefix, 0, Len - 1),
                    <<Init/binary, (LastByte + 1)>>;
                true ->
                    <<Prefix/binary, 16#FF>>
            end
    end.

%% Iterator fold loop
fold_loop({ok, Key, Value}, Itr, Fun, Acc) ->
    case Fun(Key, Value, Acc) of
        {ok, Acc1} ->
            fold_loop(rocksdb:iterator_move(Itr, next), Itr, Fun, Acc1);
        {stop, Acc1} ->
            Acc1;
        stop ->
            Acc
    end;
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc) ->
    Acc;
fold_loop({error, _Reason}, _Itr, _Fun, Acc) ->
    Acc.
