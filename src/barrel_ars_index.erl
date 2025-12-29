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
    fold_path_range/6
]).

%% Operations-only variants (for batching with other ops)
-export([
    index_doc_ops/3,
    update_doc_ops/4,
    remove_doc_ops/3
]).

%% Utility for reading stored paths
-export([
    get_doc_paths/3
]).

-include("barrel_docdb.hrl").

-type store_ref() :: barrel_store_rocksdb:db_ref().

%%====================================================================
%% API
%%====================================================================

%% @doc Index all paths from a document.
%% Extracts paths using barrel_ars:analyze/1 and stores them.
%% Also stores the reverse index (doc_id -> paths) for later updates.
-spec index_doc(store_ref(), db_name(), docid(), doc()) ->
    ok | {error, term()}.
index_doc(StoreRef, DbName, DocId, Doc) ->
    Operations = index_doc_ops(DbName, DocId, Doc),
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
    case update_doc_ops(DbName, DocId, OldDoc, NewDoc) of
        [] ->
            ok;
        Operations ->
            barrel_store_rocksdb:write_batch(StoreRef, Operations)
    end.

%% @doc Return batch operations to update paths when a document changes.
%% Use this to combine with other operations in a single write_batch.
-spec update_doc_ops(db_name(), docid(), doc(), doc()) ->
    [{put | delete, binary()} | {put, binary(), binary()}].
update_doc_ops(DbName, DocId, OldDoc, NewDoc) ->
    OldPaths = barrel_ars:analyze(OldDoc),
    NewPaths = barrel_ars:analyze(NewDoc),
    {Added, Removed} = barrel_ars:diff(OldPaths, NewPaths),

    case {Added, Removed} of
        {[], []} ->
            %% No changes
            [];
        _ ->
            %% Build batch operations
            RemoveOps = [{delete, barrel_store_keys:path_index_key(DbName, Path, DocId)}
                         || {Path, _} <- Removed],
            AddOps = [{put, barrel_store_keys:path_index_key(DbName, Path, DocId), <<>>}
                      || {Path, _} <- Added],

            %% Update reverse index with new paths
            ReverseOp = {put,
                         barrel_store_keys:doc_paths_key(DbName, DocId),
                         term_to_binary(NewPaths)},

            RemoveOps ++ AddOps ++ [ReverseOp]
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
-spec remove_doc_ops(db_name(), docid(), [{[term()], term()}]) -> [{delete, binary()}].
remove_doc_ops(DbName, DocId, Paths) ->
    %% Delete all path index entries
    DeleteOps = [{delete, barrel_store_keys:path_index_key(DbName, Path, DocId)}
                 || {Path, _} <- Paths],
    %% Delete reverse index
    ReverseKey = barrel_store_keys:doc_paths_key(DbName, DocId),
    DeleteOps ++ [{delete, ReverseKey}].

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

%% @doc Fold over path index entries matching a path prefix.
%% The callback receives {Path, DocId} for each match.
-spec fold_path(store_ref(), db_name(), [term()], fun(), term()) -> term().
fold_path(StoreRef, DbName, PathPrefix, Fun, Acc0) ->
    Prefix = barrel_store_keys:path_index_prefix(DbName, PathPrefix),
    EndKey = barrel_store_keys:path_index_end(DbName, PathPrefix),
    fold_path_range(StoreRef, DbName, Prefix, EndKey, Fun, Acc0).

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

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Create batch operations for indexing paths
make_index_ops(DbName, DocId, Paths) ->
    %% Create batch operations for all path entries
    PathOps = [{put, barrel_store_keys:path_index_key(DbName, Path, DocId), <<>>}
               || {Path, _} <- Paths],

    %% Store reverse index (doc_id -> paths) for later updates/deletes
    ReverseOp = {put,
                 barrel_store_keys:doc_paths_key(DbName, DocId),
                 term_to_binary(Paths)},

    PathOps ++ [ReverseOp].

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
