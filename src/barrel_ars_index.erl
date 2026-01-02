%%%-------------------------------------------------------------------
%%% @doc Path index storage for automatic document indexing
%%%
%%% Provides storage operations for the path index:
%%% - Index all paths extracted from a document
%%% - Update paths when a document changes
%%% - Remove all paths when a document is deleted
%%% - Query paths by prefix
%%%
%%% Uses barrel_ars for path extraction and barrel_store_keys for encoding.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_ars_index).

-export([
    index_doc/4,
    update_doc/5,
    remove_doc/3,
    fold_path/5,
    fold_path_reverse/5,
    fold_path_range/6,
    fold_path_values/5,
    fold_path_values_reverse/5,
    fold_prefix/6
]).

%% Operations-only variants (for batching with other ops)
-export([
    index_doc_ops/3,
    update_doc_ops/4,
    remove_doc_ops/3
]).

%% Utility for reading stored paths
-export([
    get_doc_paths/3,
    get_path_cardinality/3
]).

%% Bitmap utilities
-export([
    get_path_bitmap/3,
    bitmap_size_for_path/1,
    bitmap_intersect/1,
    bitmap_test_position/2,
    doc_position/2
]).

-include("barrel_docdb.hrl").

-type store_ref() :: barrel_store_rocksdb:db_ref().

%% Bitmap sizes based on path depth
%% Top-level paths (depth 1-2): 1M bits - many docs likely match
%% Mid-level paths (depth 3-4): 256K bits - fewer docs match
%% Deep paths (depth 5+): 64K bits - very selective
-define(BITMAP_SIZE_SHALLOW, 1048576).   %% 1M bits for depth 1-2
-define(BITMAP_SIZE_MEDIUM, 262144).     %% 256K bits for depth 3-4
-define(BITMAP_SIZE_DEEP, 65536).        %% 64K bits for depth 5+

%%====================================================================
%% API
%%====================================================================

%% @doc Index all paths from a document.
%% Extracts paths using barrel_ars:analyze/1 and stores them.
%% Also stores the reverse index (doc_id -> paths) for later updates.
%% Updates posting lists for O(1) equality query lookups.
-spec index_doc(store_ref(), db_name(), docid(), doc()) ->
    ok | {error, term()}.
index_doc(StoreRef, DbName, DocId, Doc) ->
    Paths = barrel_ars:analyze(Doc),
    Operations = make_index_ops(DbName, DocId, Paths),
    barrel_store_rocksdb:write_batch(StoreRef, Operations).

%% @doc Return batch operations to index all paths from a document.
%% Use this to combine with other operations in a single write_batch.
-spec index_doc_ops(db_name(), docid(), doc()) -> [{put, binary(), binary()}].
index_doc_ops(DbName, DocId, Doc) ->
    Paths = barrel_ars:analyze(Doc),
    make_index_ops(DbName, DocId, Paths).

%% @doc Update paths when a document changes.
%% Computes the diff between old and new paths and applies changes.
-spec update_doc(store_ref(), db_name(), docid(), doc(), doc()) ->
    ok | {error, term()}.
update_doc(StoreRef, DbName, DocId, OldDoc, NewDoc) ->
    OldPaths = barrel_ars:analyze(OldDoc),
    NewPaths = barrel_ars:analyze(NewDoc),
    {Added, Removed} = barrel_ars:diff(OldPaths, NewPaths),

    case {Added, Removed} of
        {[], []} ->
            ok;
        _ ->
            Operations = make_update_ops(DbName, DocId, Added, Removed, NewPaths),
            barrel_store_rocksdb:write_batch(StoreRef, Operations)
    end.

%% @doc Return batch operations to update paths when a document changes.
%% Use this to combine with other operations in a single write_batch.
%% Note: Does not include posting list updates - use update_doc for full updates.
-spec update_doc_ops(db_name(), docid(), doc(), doc()) ->
    [{put | delete, binary()} | {put, binary(), binary()}].
update_doc_ops(DbName, DocId, OldDoc, NewDoc) ->
    OldPaths = barrel_ars:analyze(OldDoc),
    NewPaths = barrel_ars:analyze(NewDoc),
    {Added, Removed} = barrel_ars:diff(OldPaths, NewPaths),

    case {Added, Removed} of
        {[], []} ->
            [];
        _ ->
            make_update_ops(DbName, DocId, Added, Removed, NewPaths)
    end.

%% @doc Remove all paths for a document.
%% Reads the reverse index to find all paths and deletes them.
-spec remove_doc(store_ref(), db_name(), docid()) ->
    ok | {error, term()}.
remove_doc(StoreRef, DbName, DocId) ->
    %% Get stored paths from reverse index
    ReverseKey = barrel_store_keys:doc_paths_key(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, ReverseKey) of
        {ok, PathsBin} ->
            Paths = binary_to_term(PathsBin),
            Operations = remove_doc_ops(DbName, DocId, Paths),
            barrel_store_rocksdb:write_batch(StoreRef, Operations);
        not_found ->
            %% No paths indexed
            ok;
        {error, _} = Error ->
            Error
    end.

%% @doc Return batch operations to remove all paths for a document.
%% Takes the stored paths (from reverse index) as parameter.
%% Use this to combine with other operations in a single write_batch.
-spec remove_doc_ops(db_name(), docid(), [{[term()], term()}]) ->
    [{delete, binary()} | {merge, binary(), integer()}].
remove_doc_ops(DbName, DocId, Paths) ->
    %% Delete all path index entries
    DeleteOps = [{delete, barrel_store_keys:path_index_key(DbName, Path, DocId)}
                 || {Path, _} <- Paths],
    %% Decrement counters for each path
    DecrOps = [{merge, barrel_store_keys:path_stats_key(DbName, Path), -1}
               || {Path, _} <- Paths],
    %% Delete reverse index
    ReverseKey = barrel_store_keys:doc_paths_key(DbName, DocId),
    DeleteOps ++ DecrOps ++ [{delete, ReverseKey}].

%% @doc Get stored paths for a document from the reverse index.
%% Returns {ok, Paths} or not_found.
-spec get_doc_paths(store_ref(), db_name(), docid()) ->
    {ok, [{[term()], term()}]} | not_found | {error, term()}.
get_doc_paths(StoreRef, DbName, DocId) ->
    ReverseKey = barrel_store_keys:doc_paths_key(DbName, DocId),
    case barrel_store_rocksdb:get(StoreRef, ReverseKey) of
        {ok, PathsBin} ->
            {ok, binary_to_term(PathsBin)};
        not_found ->
            not_found;
        {error, _} = Error ->
            Error
    end.

%% @doc Get the cardinality (document count) for a path+value.
%% Returns 0 if the path has never been indexed.
-spec get_path_cardinality(store_ref(), db_name(), [term()]) ->
    {ok, non_neg_integer()} | {error, term()}.
get_path_cardinality(StoreRef, DbName, Path) ->
    Key = barrel_store_keys:path_stats_key(DbName, Path),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, CountBin} ->
            Count = binary_to_integer(CountBin),
            {ok, max(0, Count)};
        not_found ->
            {ok, 0};
        {error, _} = Error ->
            Error
    end.

%% @doc Get the bitmap for a path+value.
%% Returns the raw bitmap binary from the bitmap column family.
-spec get_path_bitmap(store_ref(), db_name(), [term()]) ->
    {ok, binary()} | not_found | {error, term()}.
get_path_bitmap(StoreRef, DbName, Path) ->
    Key = barrel_store_keys:path_bitmap_key(DbName, Path),
    barrel_store_rocksdb:bitmap_get(StoreRef, Key).

%% @doc Fold over path index entries matching a path prefix.
%% The callback receives {Path, DocId} for each match.
-spec fold_path(store_ref(), db_name(), [term()], fun(), term()) -> term().
fold_path(StoreRef, DbName, PathPrefix, Fun, Acc0) ->
    Prefix = barrel_store_keys:path_index_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_index_end(DbName, PathPrefix),
    fold_path_range(StoreRef, DbName, Prefix, EndKey, Fun, Acc0).

%% @doc Fold over path index entries in reverse order.
%% Iterates from last to first; useful for building sorted lists with prepend.
-spec fold_path_reverse(store_ref(), db_name(), [term()], fun(), term()) -> term().
fold_path_reverse(StoreRef, DbName, PathPrefix, Fun, Acc0) ->
    Prefix = barrel_store_keys:path_index_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_index_end(DbName, PathPrefix),
    FoldFun = fun(Key, _Value, Acc) ->
        {ok, {Path, DocId}} = decode_path_index_key(Key),
        Fun({Path, DocId}, Acc)
    end,
    barrel_store_rocksdb:fold_range_reverse(StoreRef, Prefix, EndKey, FoldFun, Acc0).

%% @doc Fold over path index entries in a key range.
%% Lower-level function for range queries.
-spec fold_path_range(store_ref(), db_name(), binary(), binary(), fun(), term()) ->
    term().
fold_path_range(StoreRef, _DbName, StartKey, EndKey, Fun, Acc0) ->
    FoldFun = fun(Key, _Value, Acc) ->
        %% Extract DocId from the end of the key
        %% Key format: prefix + db_name + encoded_path + docid
        %% We need to extract the docid from the end
        {ok, {Path, DocId}} = decode_path_index_key(Key),
        Fun({Path, DocId}, Acc)
    end,
    barrel_store_rocksdb:fold_range(StoreRef, StartKey, EndKey, FoldFun, Acc0).

%% @doc Fold over all values for a path in ascending order.
%% Useful for ORDER BY path ASC with early termination.
%% The callback receives {FullPath, DocId} where FullPath includes the value.
-spec fold_path_values(store_ref(), db_name(), [term()], fun(), term()) -> term().
fold_path_values(StoreRef, DbName, PathPrefix, Fun, Acc0) ->
    Prefix = barrel_store_keys:path_index_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_index_end(DbName, PathPrefix),
    FoldFun = fun(Key, _Value, Acc) ->
        {ok, {Path, DocId}} = decode_path_index_key(Key),
        Fun({Path, DocId}, Acc)
    end,
    barrel_store_rocksdb:fold_range(StoreRef, Prefix, EndKey, FoldFun, Acc0).

%% @doc Fold over all values for a path in descending order.
%% Useful for ORDER BY path DESC with early termination.
%% Iterates from highest value to lowest.
-spec fold_path_values_reverse(store_ref(), db_name(), [term()], fun(), term()) -> term().
fold_path_values_reverse(StoreRef, DbName, PathPrefix, Fun, Acc0) ->
    Prefix = barrel_store_keys:path_index_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_index_end(DbName, PathPrefix),
    FoldFun = fun(Key, _Value, Acc) ->
        {ok, {Path, DocId}} = decode_path_index_key(Key),
        Fun({Path, DocId}, Acc)
    end,
    barrel_store_rocksdb:fold_range_reverse(StoreRef, Prefix, EndKey, FoldFun, Acc0).

%% @doc Fold over path index entries matching a value prefix.
%% Uses interval scan: [path, prefix] to [path, prefix ++ 0xFF]
%% Much faster than collecting all values and filtering.
%% Example: fold_prefix(S, Db, [<<"name">>], <<"John">>, Fun, Acc)
%%   matches: John, Johnny, Johnson, etc.
-spec fold_prefix(store_ref(), db_name(), [term()], binary(), fun(), term()) -> term().
fold_prefix(StoreRef, DbName, Path, Prefix, Fun, Acc0) when is_binary(Prefix) ->
    %% Start key: path + prefix value
    StartPath = Path ++ [Prefix],
    StartKey = barrel_store_keys:path_index_prefix(DbName, StartPath),
    %% End key: path + prefix + 0xFF (exclusive upper bound)
    EndPrefix = <<Prefix/binary, 16#FF>>,
    EndPath = Path ++ [EndPrefix],
    EndKey = barrel_store_keys:path_index_prefix(DbName, EndPath),
    FoldFun = fun(Key, _Value, Acc) ->
        {ok, {FullPath, DocId}} = decode_path_index_key(Key),
        Fun({FullPath, DocId}, Acc)
    end,
    barrel_store_rocksdb:fold_range(StoreRef, StartKey, EndKey, FoldFun, Acc0).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Create batch operations for indexing paths
make_index_ops(DbName, DocId, Paths) ->
    %% Create batch operations for all path entries
    PathOps = [{put, barrel_store_keys:path_index_key(DbName, Path, DocId), <<>>}
               || {Path, _} <- Paths],

    %% Increment counters for each path
    StatsOps = [{merge, barrel_store_keys:path_stats_key(DbName, Path), 1}
                || {Path, _} <- Paths],

    %% Set bitmap bits for each path (position varies by path depth)
    BitmapOps = [{bitmap_set,
                  barrel_store_keys:path_bitmap_key(DbName, Path),
                  doc_to_position(DocId, Path)}
                 || {Path, _} <- Paths],

    %% Store reverse index (doc_id -> paths) for later updates/deletes
    ReverseOp = {put,
                 barrel_store_keys:doc_paths_key(DbName, DocId),
                 term_to_binary(Paths)},

    PathOps ++ StatsOps ++ BitmapOps ++ [ReverseOp].

%% @private Convert DocId to a bitmap position using deterministic hash
%% Uses path depth to determine bitmap size for better space efficiency
doc_to_position(DocId, Path) ->
    BitmapSize = bitmap_size_for_path(Path),
    erlang:phash2(DocId, BitmapSize).

%% @doc Get bitmap size based on path depth.
%% Deeper paths are more selective, so we can use smaller bitmaps.
%% This is used for consistent position calculation across read/write.
-spec bitmap_size_for_path([term()]) -> pos_integer().
bitmap_size_for_path(Path) when is_list(Path) ->
    %% Path includes the value at the end, so actual depth = length - 1
    Depth = length(Path) - 1,
    if
        Depth =< 2 -> ?BITMAP_SIZE_SHALLOW;
        Depth =< 4 -> ?BITMAP_SIZE_MEDIUM;
        true -> ?BITMAP_SIZE_DEEP
    end.

%% @doc Get the bitmap position for a document ID and path.
%% This is the public version of doc_to_position/2.
-spec doc_position(docid(), [term()]) -> non_neg_integer().
doc_position(DocId, Path) ->
    doc_to_position(DocId, Path).

%% @doc Intersect multiple bitmaps (binary AND operation).
%% Returns a bitmap with only bits set that are in ALL input bitmaps.
-spec bitmap_intersect([binary()]) -> binary().
bitmap_intersect([]) -> <<>>;
bitmap_intersect([Bitmap]) -> Bitmap;
bitmap_intersect([First | Rest]) ->
    lists:foldl(fun bitmap_and/2, First, Rest).

%% @doc Test if a position is set in a bitmap.
%% Note: bitset_merge_operator uses big-endian bit ordering within bytes
-spec bitmap_test_position(binary(), non_neg_integer()) -> boolean().
bitmap_test_position(Bitmap, Position) when is_binary(Bitmap) ->
    ByteIndex = Position div 8,
    %% Big-endian bit ordering: bit 0 is MSB (leftmost), bit 7 is LSB
    BitIndex = 7 - (Position rem 8),
    case ByteIndex < byte_size(Bitmap) of
        true ->
            Byte = binary:at(Bitmap, ByteIndex),
            (Byte band (1 bsl BitIndex)) =/= 0;
        false ->
            false
    end.

%% @private Binary AND of two bitmaps
bitmap_and(A, B) ->
    MinLen = min(byte_size(A), byte_size(B)),
    bitmap_and_loop(A, B, MinLen, 0, <<>>).

bitmap_and_loop(_A, _B, Len, Idx, Acc) when Idx >= Len ->
    Acc;
bitmap_and_loop(A, B, Len, Idx, Acc) ->
    ByteA = binary:at(A, Idx),
    ByteB = binary:at(B, Idx),
    Result = ByteA band ByteB,
    bitmap_and_loop(A, B, Len, Idx + 1, <<Acc/binary, Result>>).

%% @private Decode a path index key to extract path and docid
%% Key format: 0x0B + len:16 + dbname + encoded_path + docid
decode_path_index_key(Key) ->
    %% Skip prefix byte (0x0B) and extract db name length
    <<16#0B, DbNameLen:16, _DbName:DbNameLen/binary, Rest/binary>> = Key,
    %% The rest is encoded_path + docid
    %% We need to parse the path components until we reach the docid
    decode_path_and_docid(Rest).

%% @private Parse encoded path components and extract trailing docid
decode_path_and_docid(Bin) ->
    {Path, DocId} = decode_path_components(Bin, []),
    {ok, {Path, DocId}}.

%% @private Decode path components, treating the remaining bytes as docid
decode_path_components(Bin, Acc) ->
    case Bin of
        <<16#01, Rest/binary>> ->  %% null
            decode_path_components(Rest, [null | Acc]);
        <<16#02, Rest/binary>> ->  %% false
            decode_path_components(Rest, [false | Acc]);
        <<16#03, Rest/binary>> ->  %% true
            decode_path_components(Rest, [true | Acc]);
        <<16#20, Rest/binary>> ->  %% zero
            decode_path_components(Rest, [0 | Acc]);
        <<16#30, Len:8, IntBin:Len/binary, Rest/binary>> ->  %% positive int
            N = binary_to_integer(IntBin),
            decode_path_components(Rest, [N | Acc]);
        <<16#10, InvLen:8, Rest/binary>> ->  %% negative int
            Len = 255 - InvLen,
            <<InvBytes:Len/binary, Rest2/binary>> = Rest,
            Bin2 = << <<(255 - B)>> || <<B>> <= InvBytes >>,
            N = -binary_to_integer(Bin2),
            decode_path_components(Rest2, [N | Acc]);
        <<16#40, _Encoded:8/binary, Rest/binary>> ->  %% float
            %% We'll use the existing decode function
            <<16#40, Encoded:8/binary, _/binary>> = <<16#40, _Encoded/binary>>,
            F = decode_float(Encoded),
            decode_path_components(Rest, [F | Acc]);
        <<16#50, Rest/binary>> ->  %% binary (null-escaped)
            {BinVal, Rest2} = unescape_binary(Rest),
            decode_path_components(Rest2, [BinVal | Acc]);
        DocId when is_binary(DocId), byte_size(DocId) > 0 ->
            %% Remaining bytes are the docid
            {lists:reverse(Acc), DocId};
        <<>> ->
            %% Empty remaining means no docid (shouldn't happen in practice)
            {lists:reverse(Acc), <<>>}
    end.

%% @private Unescape binary (same as in barrel_store_keys)
unescape_binary(Bin) ->
    unescape_binary(Bin, <<>>).

unescape_binary(<<0, 0, Rest/binary>>, Acc) ->
    {Acc, Rest};
unescape_binary(<<0, 16#FF, Rest/binary>>, Acc) ->
    unescape_binary(Rest, <<Acc/binary, 0>>);
unescape_binary(<<B, Rest/binary>>, Acc) ->
    unescape_binary(Rest, <<Acc/binary, B>>);
unescape_binary(<<>>, Acc) ->
    {Acc, <<>>}.

%% @private Decode float (same as in barrel_store_keys)
decode_float(<<Encoded:64/big-unsigned>>) ->
    Bits = case Encoded band 16#8000000000000000 of
        0 -> bnot Encoded;
        _ -> Encoded bxor 16#8000000000000000
    end,
    <<F:64/float>> = <<Bits:64/big-unsigned>>,
    F.

%% @private Create batch operations for updating paths (add/remove)
make_update_ops(DbName, DocId, Added, Removed, NewPaths) ->
    %% Build batch operations
    RemoveOps = [{delete, barrel_store_keys:path_index_key(DbName, Path, DocId)}
                 || {Path, _} <- Removed],
    AddOps = [{put, barrel_store_keys:path_index_key(DbName, Path, DocId), <<>>}
              || {Path, _} <- Added],

    %% Update counters: decrement removed, increment added
    DecrOps = [{merge, barrel_store_keys:path_stats_key(DbName, Path), -1}
               || {Path, _} <- Removed],
    IncrOps = [{merge, barrel_store_keys:path_stats_key(DbName, Path), 1}
               || {Path, _} <- Added],

    %% Update bitmap for added paths (position varies by path depth)
    %% Note: We don't unset on remove because other docs may share the position
    BitmapSetOps = [{bitmap_set,
                     barrel_store_keys:path_bitmap_key(DbName, Path),
                     doc_to_position(DocId, Path)}
                    || {Path, _} <- Added],

    %% Update reverse index with new paths
    ReverseOp = {put,
                 barrel_store_keys:doc_paths_key(DbName, DocId),
                 term_to_binary(NewPaths)},

    RemoveOps ++ AddOps ++ DecrOps ++ IncrOps ++ BitmapSetOps ++ [ReverseOp].
