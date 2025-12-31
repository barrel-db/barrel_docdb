%%%-------------------------------------------------------------------
%%% @doc BlobDB storage backend for attachments
%%%
%%% Uses a separate RocksDB instance with BlobDB enabled for storing
%%% attachment binary data. This avoids compaction issues from mixing
%%% small documents with large blobs.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_att_store).

-include("barrel_docdb.hrl").

%% API
-export([open/2, close/1]).
-export([put/5, get/4, delete/4]).
-export([delete_all/3]).
-export([fold/5]).

%%====================================================================
%% Types
%%====================================================================

-type att_ref() :: #{
    ref := rocksdb:db_handle(),
    path := string()
}.

-export_type([att_ref/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Open an attachment store with BlobDB enabled
-spec open(string(), map()) -> {ok, att_ref()} | {error, term()}.
open(Path, Options) ->
    ok = filelib:ensure_dir(Path ++ "/"),
    DbOpts = build_blob_options(Options),
    case rocksdb:open(Path, DbOpts) of
        {ok, Ref} ->
            {ok, #{ref => Ref, path => Path}};
        {error, Reason} ->
            {error, {att_store_open_failed, Reason}}
    end.

%% @doc Close the attachment store
-spec close(att_ref()) -> ok.
close(#{ref := Ref}) ->
    rocksdb:close(Ref).

%% @doc Store an attachment
-spec put(att_ref(), db_name(), docid(), binary(), binary()) ->
    {ok, att_info()} | {error, term()}.
put(#{ref := Ref}, DbName, DocId, AttName, Data) when is_binary(Data) ->
    Key = make_key(DbName, DocId, AttName),
    Digest = compute_digest(Data),
    ContentType = mimerl:filename(AttName),
    case rocksdb:put(Ref, Key, Data, [{sync, true}]) of
        ok ->
            AttInfo = #{
                name => AttName,
                content_type => ContentType,
                length => byte_size(Data),
                digest => Digest
            },
            {ok, AttInfo};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Retrieve an attachment
-spec get(att_ref(), db_name(), docid(), binary()) ->
    {ok, binary()} | not_found | {error, term()}.
get(#{ref := Ref}, DbName, DocId, AttName) ->
    Key = make_key(DbName, DocId, AttName),
    rocksdb:get(Ref, Key, []).

%% @doc Delete an attachment
-spec delete(att_ref(), db_name(), docid(), binary()) -> ok | {error, term()}.
delete(#{ref := Ref}, DbName, DocId, AttName) ->
    Key = make_key(DbName, DocId, AttName),
    rocksdb:delete(Ref, Key, []).

%% @doc Delete all attachments for a document
-spec delete_all(att_ref(), db_name(), docid()) -> ok | {error, term()}.
delete_all(AttRef, DbName, DocId) ->
    Keys = fold(AttRef, DbName, DocId,
        fun(Name, _Data, Acc) -> {ok, [Name | Acc]} end,
        []),
    lists:foreach(
        fun(AttName) ->
            delete(AttRef, DbName, DocId, AttName)
        end,
        Keys
    ),
    ok.

%% @doc Fold over all attachments for a document
-spec fold(att_ref(), db_name(), docid(), fun(), term()) -> term().
fold(#{ref := Ref}, DbName, DocId, Fun, Acc) ->
    Prefix = make_prefix(DbName, DocId),
    PrefixEnd = prefix_end(Prefix),
    ReadOpts = [
        {iterate_lower_bound, Prefix},
        {iterate_upper_bound, PrefixEnd}
    ],
    {ok, Itr} = rocksdb:iterator(Ref, ReadOpts),
    try
        fold_loop(rocksdb:iterator_move(Itr, first), Itr, Prefix, Fun, Acc)
    after
        rocksdb:iterator_close(Itr)
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Build RocksDB options with BlobDB enabled
build_blob_options(Options) ->
    BlobFileSize = maps:get(blob_file_size, Options, 256 * 1024 * 1024),  % 256MB
    Schedulers = erlang:system_info(schedulers),

    %% Block-based table options with shared cache (for metadata lookups)
    BlockOpts = barrel_cache:get_block_opts(#{
        bloom_bits => maps:get(bloom_bits, Options, 10),
        block_size => maps:get(block_size, Options, 4096)
    }),

    [
        {create_if_missing, true},
        {max_open_files, maps:get(max_open_files, Options, 256)},
        {compression, maps:get(compression, Options, snappy)},

        %% BlobDB settings for large attachments
        {enable_blob_files, true},
        {min_blob_size, maps:get(min_blob_size, Options, 4096)},  % 4KB threshold
        {blob_file_size, BlobFileSize},
        {blob_compression_type, maps:get(blob_compression, Options, snappy)},

        %% Blob garbage collection
        {enable_blob_garbage_collection, true},
        {blob_garbage_collection_age_cutoff,
            maps:get(blob_gc_age_cutoff, Options, 0.25)},
        {blob_garbage_collection_force_threshold,
            maps:get(blob_gc_force_threshold, Options, 0.5)},

        %% Background jobs for compaction/GC
        {max_background_jobs, maps:get(max_background_jobs, Options, erlang:max(2, Schedulers div 2))},

        %% Block-based table with bloom filters and shared cache
        {block_based_table_options, BlockOpts}
    ].

%% Create key for attachment: prefix + att_name
make_key(DbName, DocId, AttName) ->
    Prefix = make_prefix(DbName, DocId),
    <<Prefix/binary, AttName/binary>>.

%% Create prefix for all attachments of a document
make_prefix(DbName, DocId) ->
    DbNameLen = byte_size(DbName),
    DocIdLen = byte_size(DocId),
    <<DbNameLen:16, DbName/binary, DocIdLen:16, DocId/binary, $:>>.

%% Compute the end of a prefix range
prefix_end(Prefix) ->
    Len = byte_size(Prefix),
    LastByte = binary:last(Prefix),
    if
        LastByte < 16#FF ->
            Init = binary:part(Prefix, 0, Len - 1),
            <<Init/binary, (LastByte + 1)>>;
        true ->
            <<Prefix/binary, 16#FF>>
    end.

%% Extract attachment name from key
extract_att_name(Key, Prefix) ->
    PrefixLen = byte_size(Prefix),
    <<_:PrefixLen/binary, AttName/binary>> = Key,
    AttName.

%% Compute SHA-256 digest of data
compute_digest(Data) ->
    Digest = crypto:hash(sha256, Data),
    <<"sha256-", (to_hex(Digest))/binary>>.

%% Convert binary to hex string
to_hex(Bin) ->
    << <<(hex_char(N))>> || <<N:4>> <= Bin >>.

hex_char(N) when N < 10 -> $0 + N;
hex_char(N) -> $a + N - 10.

%% Iterator fold loop
fold_loop({ok, Key, Value}, Itr, Prefix, Fun, Acc) ->
    AttName = extract_att_name(Key, Prefix),
    case Fun(AttName, Value, Acc) of
        {ok, Acc1} ->
            fold_loop(rocksdb:iterator_move(Itr, next), Itr, Prefix, Fun, Acc1);
        {stop, Acc1} ->
            Acc1;
        stop ->
            Acc
    end;
fold_loop({error, invalid_iterator}, _Itr, _Prefix, _Fun, Acc) ->
    Acc;
fold_loop({error, _Reason}, _Itr, _Prefix, _Fun, Acc) ->
    Acc.
