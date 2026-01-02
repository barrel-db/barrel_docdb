%%%-------------------------------------------------------------------
%%% @doc RocksDB storage backend for barrel_docdb
%%%
%%% Implements the barrel_store behaviour using RocksDB 2.2.0.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_store_rocksdb).
-behaviour(barrel_store).

%% barrel_store callbacks
-export([open/2, close/1]).
-export([put/3, put/4, get/2, multi_get/2, delete/2]).
-export([merge/3]).
-export([write_batch/2, write_batch/3]).
-export([fold/4, fold_range/5, fold_range_reverse/5]).

%% Additional utilities
-export([snapshot/1, release_snapshot/1]).
-export([get_with_snapshot/3]).

%% Bitmap operations
-export([bitmap_set/3, bitmap_unset/3, bitmap_get/2, multi_get_bitmap/2]).

%% Posting list operations
-export([posting_get/2, posting_multi_get/2]).
-export([posting_put/3, posting_delete/2]).

%%====================================================================
%% Types
%%====================================================================

-type db_ref() :: #{
    ref := rocksdb:db_handle(),
    path := string(),
    default_cf := rocksdb:cf_handle(),
    bitmap_cf := rocksdb:cf_handle(),
    posting_cf := rocksdb:cf_handle()
}.

-type snapshot() :: rocksdb:snapshot_handle().

-export_type([db_ref/0, snapshot/0]).

%% Column family names
-define(BITMAP_CF_NAME, "bitmap").
-define(POSTING_CF_NAME, "posting").

%%====================================================================
%% barrel_store callbacks
%%====================================================================

%% @doc Open a RocksDB database with column families
-spec open(string(), map()) -> {ok, db_ref()} | {error, term()}.
open(Path, Options) ->
    ok = filelib:ensure_dir(Path ++ "/"),
    DbOpts = build_db_options(Options),
    BitmapSize = maps:get(bitmap_size, Options, 1048576),  %% 1M bits default

    %% Column family descriptors
    DefaultCFOpts = [{merge_operator, counter_merge_operator}],
    BitmapCFOpts = [{merge_operator, {bitset_merge_operator, BitmapSize}}],
    PostingCFOpts = [],  %% No merge operator - writes serialized by posting_writer

    CFDescriptors = [
        {"default", DefaultCFOpts},
        {?BITMAP_CF_NAME, BitmapCFOpts},
        {?POSTING_CF_NAME, PostingCFOpts}
    ],

    case rocksdb:open(Path, DbOpts, CFDescriptors) of
        {ok, Ref, [DefaultCF, BitmapCF, PostingCF]} ->
            {ok, #{ref => Ref, path => Path,
                   default_cf => DefaultCF, bitmap_cf => BitmapCF,
                   posting_cf => PostingCF}};
        {error, {db_open, _Msg}} ->
            %% Database might not have all CFs yet, try to create them
            open_and_create_cfs(Path, DbOpts, BitmapCFOpts, PostingCFOpts);
        {error, Reason} ->
            {error, {db_open_failed, Reason}}
    end.

%% @private Open database and create missing column families
open_and_create_cfs(Path, DbOpts, BitmapCFOpts, PostingCFOpts) ->
    %% Try opening with just bitmap CF (for existing databases)
    case rocksdb:open(Path, DbOpts, [{"default", []}, {?BITMAP_CF_NAME, BitmapCFOpts}]) of
        {ok, Ref, [DefaultCF, BitmapCF]} ->
            %% Add posting CF
            case rocksdb:create_column_family(Ref, ?POSTING_CF_NAME, PostingCFOpts) of
                {ok, PostingCF} ->
                    {ok, #{ref => Ref, path => Path,
                           default_cf => DefaultCF, bitmap_cf => BitmapCF,
                           posting_cf => PostingCF}};
                {error, CFErr} ->
                    rocksdb:close(Ref),
                    {error, {cf_create_failed, CFErr}}
            end;
        {error, _} ->
            %% Try opening with no CFs (new database or very old)
            case rocksdb:open(Path, DbOpts) of
                {ok, Ref} ->
                    %% Create both bitmap and posting CFs
                    case rocksdb:create_column_family(Ref, ?BITMAP_CF_NAME, BitmapCFOpts) of
                        {ok, BitmapCF} ->
                            case rocksdb:create_column_family(Ref, ?POSTING_CF_NAME, PostingCFOpts) of
                                {ok, PostingCF} ->
                                    {ok, #{ref => Ref, path => Path,
                                           default_cf => default_column_family,
                                           bitmap_cf => BitmapCF,
                                           posting_cf => PostingCF}};
                                {error, CFErr} ->
                                    rocksdb:close(Ref),
                                    {error, {cf_create_failed, CFErr}}
                            end;
                        {error, CFErr} ->
                            rocksdb:close(Ref),
                            {error, {cf_create_failed, CFErr}}
                    end;
                {error, Reason} ->
                    {error, {db_open_failed, Reason}}
            end
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

%% @doc Get multiple values by keys (batch read)
-spec multi_get(db_ref(), [binary()]) -> [{ok, binary()} | not_found | {error, term()}].
multi_get(#{ref := Ref}, Keys) ->
    rocksdb:multi_get(Ref, Keys, []).

%% @doc Delete a key
-spec delete(db_ref(), binary()) -> ok | {error, term()}.
delete(#{ref := Ref}, Key) ->
    rocksdb:delete(Ref, Key, []).

%% @doc Merge a value with the counter merge operator
-spec merge(db_ref(), binary(), integer()) -> ok | {error, term()}.
merge(#{ref := Ref}, Key, Delta) ->
    rocksdb:merge(Ref, Key, integer_to_binary(Delta), []).

%% @doc Execute a batch of operations atomically (async by default)
-spec write_batch(db_ref(), list()) -> ok | {error, term()}.
write_batch(DbRef, Operations) ->
    write_batch(DbRef, Operations, #{}).

%% @doc Execute a batch of operations atomically with options
%% Options:
%%   - sync: boolean() - if true, sync to disk before returning (default: false)
%% Operations:
%%   - {put, Key, Value} - put to default CF
%%   - {delete, Key} - delete from default CF
%%   - {merge, Key, Delta} - merge counter in default CF
%%   - {bitmap_set, Key, Position} - set bit in bitmap CF
%%   - {bitmap_unset, Key, Position} - unset bit in bitmap CF
-spec write_batch(db_ref(), list(), map()) -> ok | {error, term()}.
write_batch(#{ref := Ref, bitmap_cf := BitmapCF}, Operations, Opts) ->
    Sync = maps:get(sync, Opts, false),
    {ok, Batch} = rocksdb:batch(),
    try
        lists:foreach(
            fun({put, Key, Value}) ->
                ok = rocksdb:batch_put(Batch, Key, Value);
               ({delete, Key}) ->
                ok = rocksdb:batch_delete(Batch, Key);
               ({merge, Key, Delta}) when is_integer(Delta) ->
                ok = rocksdb:batch_merge(Batch, Key, integer_to_binary(Delta));
               ({bitmap_set, Key, Position}) when is_integer(Position) ->
                ok = rocksdb:batch_merge(Batch, BitmapCF, Key,
                                         <<"+", (integer_to_binary(Position))/binary>>);
               ({bitmap_unset, Key, Position}) when is_integer(Position) ->
                ok = rocksdb:batch_merge(Batch, BitmapCF, Key,
                                         <<"-", (integer_to_binary(Position))/binary>>)
            end,
            Operations
        ),
        Result = rocksdb:write_batch(Ref, Batch, [{sync, Sync}]),
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

%% @doc Fold over a key range in reverse order (last to first)
%% Useful for building sorted lists with prepend: [Item | Acc]
-spec fold_range_reverse(db_ref(), binary(), binary(), fun(), term()) -> term().
fold_range_reverse(#{ref := Ref}, StartKey, EndKey, Fun, Acc) ->
    ReadOpts = [
        {iterate_lower_bound, StartKey},
        {iterate_upper_bound, EndKey}
    ],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        fold_loop_reverse(rocksdb:iterator_move(Itr, last), Itr, Fun, Acc)
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
    Schedulers = erlang:system_info(schedulers),

    %% Block-based table options with shared cache and bloom filters
    BlockOpts = barrel_cache:get_block_opts(#{
        bloom_bits => maps:get(bloom_bits, Options, 10),
        block_size => maps:get(block_size, Options, 4096)
    }),

    BaseOpts = [
        {create_if_missing, true},
        {max_open_files, maps:get(max_open_files, Options, 1000)},

        %% Write buffer configuration
        {write_buffer_size, WriteBufferSize},
        {max_write_buffer_number, maps:get(max_write_buffer_number, Options, 3)},
        {min_write_buffer_number_to_merge, maps:get(min_write_buffer_number_to_merge, Options, 1)},

        %% Compression - snappy for all levels (zstd optional if available)
        {compression, maps:get(compression, Options, snappy)},
        {bottommost_compression, maps:get(bottommost_compression, Options, snappy)},

        %% Concurrency
        {allow_concurrent_memtable_write, true},
        {enable_write_thread_adaptive_yield, true},
        {enable_pipelined_write, true},

        %% Compaction tuning
        {level0_file_num_compaction_trigger, maps:get(l0_compaction_trigger, Options, 4)},
        {level0_slowdown_writes_trigger, maps:get(l0_slowdown_trigger, Options, 20)},
        {level0_stop_writes_trigger, maps:get(l0_stop_trigger, Options, 36)},
        {max_background_jobs, maps:get(max_background_jobs, Options, Schedulers)},
        {max_subcompactions, maps:get(max_subcompactions, Options, 4)},

        %% Prefix extractor for prefix bloom filters
        %% Enables O(1) existence check for key prefixes (up to 64 bytes)
        {prefix_extractor, {capped_prefix_transform, 64}},

        %% Use counter merge operator for atomic counters
        {merge_operator, counter_merge_operator},
        {total_threads, erlang:max(4, Schedulers)},

        %% Block-based table with bloom filters and shared cache
        {block_based_table_options, BlockOpts}
    ],

    %% Optional rate limiter for production workloads
    case maps:get(rate_limit_bytes_per_sec, Options, 0) of
        0 -> BaseOpts;
        RateLimit ->
            case rocksdb:new_rate_limiter(RateLimit) of
                {ok, Limiter} -> [{rate_limiter, Limiter} | BaseOpts];
                _ -> BaseOpts
            end
    end.

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

%% Fold loop for reverse iteration (uses prev instead of next)
fold_loop_reverse({ok, Key, Value}, Itr, Fun, Acc) ->
    case Fun(Key, Value, Acc) of
        {ok, Acc1} ->
            fold_loop_reverse(rocksdb:iterator_move(Itr, prev), Itr, Fun, Acc1);
        {stop, Acc1} ->
            Acc1;
        stop ->
            Acc
    end;
fold_loop_reverse({error, invalid_iterator}, _Itr, _Fun, Acc) ->
    Acc;
fold_loop_reverse({error, _Reason}, _Itr, _Fun, Acc) ->
    Acc.

%%====================================================================
%% Bitmap Operations
%%====================================================================

%% @doc Set a bit in a bitmap using merge operator
%% The bitset_merge_operator uses format: <<"+", Position/binary>>
-spec bitmap_set(db_ref(), binary(), non_neg_integer()) -> ok | {error, term()}.
bitmap_set(#{ref := Ref, bitmap_cf := BitmapCF}, Key, Position) ->
    rocksdb:merge(Ref, BitmapCF, Key, <<"+", (integer_to_binary(Position))/binary>>, []).

%% @doc Unset a bit in a bitmap using merge operator
%% The bitset_merge_operator uses format: <<"-", Position/binary>>
-spec bitmap_unset(db_ref(), binary(), non_neg_integer()) -> ok | {error, term()}.
bitmap_unset(#{ref := Ref, bitmap_cf := BitmapCF}, Key, Position) ->
    rocksdb:merge(Ref, BitmapCF, Key, <<"-", (integer_to_binary(Position))/binary>>, []).

%% @doc Get a bitmap from the bitmap column family
-spec bitmap_get(db_ref(), binary()) -> {ok, binary()} | not_found | {error, term()}.
bitmap_get(#{ref := Ref, bitmap_cf := BitmapCF}, Key) ->
    rocksdb:get(Ref, BitmapCF, Key, []).

%% @doc Get multiple bitmaps from the bitmap column family (batch read)
-spec multi_get_bitmap(db_ref(), [binary()]) -> [{ok, binary()} | not_found | {error, term()}].
multi_get_bitmap(#{ref := Ref, bitmap_cf := BitmapCF}, Keys) ->
    rocksdb:multi_get(Ref, BitmapCF, Keys, []).

%%====================================================================
%% Posting List Operations
%%====================================================================

%% @doc Get a posting list from the posting column family
-spec posting_get(db_ref(), binary()) -> {ok, binary()} | not_found | {error, term()}.
posting_get(#{ref := Ref, posting_cf := PostingCF}, Key) ->
    rocksdb:get(Ref, PostingCF, Key, []).

%% @doc Get multiple posting lists from the posting column family (batch read)
-spec posting_multi_get(db_ref(), [binary()]) -> [{ok, binary()} | not_found | {error, term()}].
posting_multi_get(#{ref := Ref, posting_cf := PostingCF}, Keys) ->
    rocksdb:multi_get(Ref, PostingCF, Keys, []).

%% @doc Put a posting list to the posting column family
%% Note: Writes should be serialized through barrel_posting_writer
-spec posting_put(db_ref(), binary(), binary()) -> ok | {error, term()}.
posting_put(#{ref := Ref, posting_cf := PostingCF}, Key, Value) ->
    rocksdb:put(Ref, PostingCF, Key, Value, []).

%% @doc Delete a posting list from the posting column family
-spec posting_delete(db_ref(), binary()) -> ok | {error, term()}.
posting_delete(#{ref := Ref, posting_cf := PostingCF}, Key) ->
    rocksdb:delete(Ref, PostingCF, Key, []).
