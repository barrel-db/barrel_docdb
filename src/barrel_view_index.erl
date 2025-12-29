%%%-------------------------------------------------------------------
%%% @doc View index storage for barrel_docdb
%%%
%%% Handles the low-level storage of view index entries in RocksDB.
%%% Each view entry consists of:
%%% - Key: encoded view key (term)
%%% - Value: {DocId, EmittedValue}
%%%
%%% Also maintains a reverse index (view_by_docid) to track which
%%% entries belong to each document for efficient updates/deletes.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_view_index).

-include("barrel_docdb.hrl").

%% API
-export([
    get_indexed_seq/3,
    set_indexed_seq/4,
    update_doc_entries/5,
    delete_doc_entries/4,
    fold_index/5,
    query_range/5,
    clear_all/3
]).

%% View metadata
-export([
    get_view_meta/3,
    set_view_meta/4,
    delete_view_meta/3,
    list_views/2
]).

%%====================================================================
%% Types
%%====================================================================

-type view_entry() :: #{
    key := term(),
    value := term(),
    id := docid()
}.

-type query_opts() :: #{
    start_key => term(),
    end_key => term(),
    inclusive_start => boolean(),
    inclusive_end => boolean(),
    descending => boolean(),
    limit => pos_integer(),
    skip => non_neg_integer()
}.

-export_type([view_entry/0, query_opts/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Get the last indexed HLC for a view
-spec get_indexed_seq(barrel_store_rocksdb:db_ref(), db_name(), binary()) ->
    barrel_hlc:timestamp() | first.
get_indexed_seq(StoreRef, DbName, ViewId) ->
    Key = barrel_store_keys:view_seq(DbName, ViewId),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, Bin} ->
            barrel_hlc:decode(Bin);
        not_found ->
            first
    end.

%% @doc Set the indexed HLC for a view
-spec set_indexed_seq(barrel_store_rocksdb:db_ref(), db_name(), binary(), barrel_hlc:timestamp()) -> ok.
set_indexed_seq(StoreRef, DbName, ViewId, Hlc) ->
    Key = barrel_store_keys:view_seq(DbName, ViewId),
    Value = barrel_hlc:encode(Hlc),
    barrel_store_rocksdb:put(StoreRef, Key, Value).

%% @doc Update view entries for a document
%% First deletes old entries, then inserts new ones
-spec update_doc_entries(barrel_store_rocksdb:db_ref(), db_name(), binary(),
                        docid(), [{term(), term()}]) -> ok.
update_doc_entries(StoreRef, DbName, ViewId, DocId, NewEntries) ->
    %% Get old entry keys for this doc
    OldKeys = get_doc_entry_keys(StoreRef, DbName, ViewId, DocId),

    %% Build batch operations
    DeleteOps = [
        {delete, barrel_store_keys:view_index(DbName, ViewId, encode_index_key(K, DocId))}
        || K <- OldKeys
    ],

    %% Delete old by_docid entry
    ByDocIdKey = barrel_store_keys:view_by_docid(DbName, ViewId, DocId),
    DeleteByDocIdOp = {delete, ByDocIdKey},

    %% Build new entry operations
    InsertOps = lists:map(
        fun({Key, Value}) ->
            IndexKey = encode_index_key(Key, DocId),
            FullKey = barrel_store_keys:view_index(DbName, ViewId, IndexKey),
            EntryValue = term_to_binary({DocId, Value}),
            {put, FullKey, EntryValue}
        end,
        NewEntries
    ),

    %% Build new by_docid entry (stores list of keys for this doc)
    NewKeys = [K || {K, _V} <- NewEntries],
    ByDocIdOp = case NewKeys of
        [] ->
            [];
        _ ->
            [{put, ByDocIdKey, term_to_binary(NewKeys)}]
    end,

    %% Execute batch
    AllOps = DeleteOps ++ [DeleteByDocIdOp] ++ InsertOps ++ ByDocIdOp,
    case AllOps of
        [] -> ok;
        _ -> barrel_store_rocksdb:write_batch(StoreRef, AllOps)
    end.

%% @doc Delete all view entries for a document
-spec delete_doc_entries(barrel_store_rocksdb:db_ref(), db_name(), binary(), docid()) -> ok.
delete_doc_entries(StoreRef, DbName, ViewId, DocId) ->
    %% Get old entry keys
    OldKeys = get_doc_entry_keys(StoreRef, DbName, ViewId, DocId),

    %% Build delete operations
    DeleteOps = [
        {delete, barrel_store_keys:view_index(DbName, ViewId, encode_index_key(K, DocId))}
        || K <- OldKeys
    ],

    %% Delete by_docid entry
    ByDocIdKey = barrel_store_keys:view_by_docid(DbName, ViewId, DocId),
    ByDocIdOp = {delete, ByDocIdKey},

    AllOps = [ByDocIdOp | DeleteOps],
    barrel_store_rocksdb:write_batch(StoreRef, AllOps).

%% @doc Fold over all index entries for a view
-spec fold_index(barrel_store_rocksdb:db_ref(), db_name(), binary(),
                fun((view_entry(), Acc) -> {ok, Acc} | {stop, Acc}), Acc) ->
    Acc when Acc :: term().
fold_index(StoreRef, DbName, ViewId, Fun, Acc) ->
    StartKey = barrel_store_keys:view_index_prefix(DbName, ViewId),
    EndKey = barrel_store_keys:view_index_end(DbName, ViewId),

    barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey,
        fun(Key, Value, AccIn) ->
            Entry = decode_entry(DbName, ViewId, Key, Value),
            Fun(Entry, AccIn)
        end,
        Acc
    ).

%% @doc Query view entries within a key range
-spec query_range(barrel_store_rocksdb:db_ref(), db_name(), binary(),
                 query_opts(), fun((view_entry(), Acc) -> {ok, Acc} | {stop, Acc})) ->
    {ok, Acc, non_neg_integer()} when Acc :: term().
query_range(StoreRef, DbName, ViewId, Opts, Fun) ->
    Limit = maps:get(limit, Opts, infinity),
    Skip = maps:get(skip, Opts, 0),
    Descending = maps:get(descending, Opts, false),

    %% Determine key range
    {StartKey, EndKey} = compute_range_keys(DbName, ViewId, Opts),

    %% Fold with skip/limit handling
    FoldFun = fun(Key, Value, {Skipped, Count, Results}) ->
        if
            Skipped < Skip ->
                {ok, {Skipped + 1, Count, Results}};
            Limit =/= infinity andalso Count >= Limit ->
                {stop, {Skipped, Count, Results}};
            true ->
                Entry = decode_entry(DbName, ViewId, Key, Value),
                case Fun(Entry, Results) of
                    {ok, NewResults} ->
                        {ok, {Skipped, Count + 1, NewResults}};
                    {stop, NewResults} ->
                        {stop, {Skipped, Count + 1, NewResults}}
                end
        end
    end,

    {_Skipped, TotalCount, FinalAcc} = barrel_store_rocksdb:fold_range(
        StoreRef, StartKey, EndKey, FoldFun, {0, 0, []}
    ),

    %% Handle descending order
    Result = case Descending of
        true -> lists:reverse(FinalAcc);
        false -> FinalAcc
    end,

    {ok, Result, TotalCount}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Get the list of index keys for a document
-spec get_doc_entry_keys(barrel_store_rocksdb:db_ref(), db_name(), binary(), docid()) ->
    [term()].
get_doc_entry_keys(StoreRef, DbName, ViewId, DocId) ->
    Key = barrel_store_keys:view_by_docid(DbName, ViewId, DocId),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, Bin} ->
            binary_to_term(Bin);
        not_found ->
            []
    end.

%% @doc Encode an index key with docid for uniqueness
%% Key format: {EncodedKey, DocId} to ensure each doc's entries are unique
encode_index_key(Key, DocId) ->
    EncodedKey = barrel_store_keys:encode_view_key(Key),
    <<EncodedKey/binary, DocId/binary>>.

%% @doc Decode an entry from storage
decode_entry(DbName, ViewId, FullKey, Value) ->
    %% Extract the encoded key part (remove prefix)
    Prefix = barrel_store_keys:view_index_prefix(DbName, ViewId),
    PrefixLen = byte_size(Prefix),
    <<Prefix:PrefixLen/binary, IndexKeyWithDocId/binary>> = FullKey,

    %% Parse value
    {DocId, EmittedValue} = binary_to_term(Value),

    %% Extract the original key (IndexKey minus the DocId suffix)
    DocIdLen = byte_size(DocId),
    EncodedKeyLen = byte_size(IndexKeyWithDocId) - DocIdLen,
    <<EncodedKey:EncodedKeyLen/binary, _DocId/binary>> = IndexKeyWithDocId,
    Key = barrel_store_keys:decode_view_key(EncodedKey),

    #{key => Key, value => EmittedValue, id => DocId}.

%% @doc Compute start and end keys for a range query
compute_range_keys(DbName, ViewId, Opts) ->
    Prefix = barrel_store_keys:view_index_prefix(DbName, ViewId),
    PrefixEnd = barrel_store_keys:view_index_end(DbName, ViewId),

    StartKey = case maps:get(start_key, Opts, undefined) of
        undefined -> Prefix;
        SK ->
            EncodedSK = barrel_store_keys:encode_view_key(SK),
            barrel_store_keys:view_index(DbName, ViewId, EncodedSK)
    end,

    EndKey = case maps:get(end_key, Opts, undefined) of
        undefined -> PrefixEnd;
        EK ->
            EncodedEK = barrel_store_keys:encode_view_key(EK),
            %% For inclusive end, we need to include all entries with this key
            <<(barrel_store_keys:view_index(DbName, ViewId, EncodedEK))/binary, 16#FF>>
    end,

    {StartKey, EndKey}.

%%====================================================================
%% View Metadata Functions
%%====================================================================

%% @doc Get view metadata (definition)
-spec get_view_meta(barrel_store_rocksdb:db_ref(), db_name(), binary()) ->
    {ok, map()} | not_found.
get_view_meta(StoreRef, DbName, ViewId) ->
    Key = barrel_store_keys:view_meta(DbName, ViewId),
    case barrel_store_rocksdb:get(StoreRef, Key) of
        {ok, Bin} ->
            {ok, binary_to_term(Bin)};
        not_found ->
            not_found
    end.

%% @doc Set view metadata (definition)
-spec set_view_meta(barrel_store_rocksdb:db_ref(), db_name(), binary(), map()) -> ok.
set_view_meta(StoreRef, DbName, ViewId, Meta) ->
    Key = barrel_store_keys:view_meta(DbName, ViewId),
    Value = term_to_binary(Meta),
    barrel_store_rocksdb:put(StoreRef, Key, Value).

%% @doc Delete view metadata
-spec delete_view_meta(barrel_store_rocksdb:db_ref(), db_name(), binary()) -> ok.
delete_view_meta(StoreRef, DbName, ViewId) ->
    Key = barrel_store_keys:view_meta(DbName, ViewId),
    barrel_store_rocksdb:delete(StoreRef, Key).

%% @doc List all registered views for a database
-spec list_views(barrel_store_rocksdb:db_ref(), db_name()) -> [map()].
list_views(StoreRef, DbName) ->
    %% Scan all view_meta keys for this database
    Prefix = barrel_store_keys:view_meta(DbName, <<>>),
    PrefixEnd = <<Prefix/binary, 16#FF>>,

    barrel_store_rocksdb:fold_range(
        StoreRef, Prefix, PrefixEnd,
        fun(_Key, Value, Acc) ->
            Meta = binary_to_term(Value),
            {ok, [Meta | Acc]}
        end,
        []
    ).

%%====================================================================
%% Clear Functions
%%====================================================================

%% @doc Clear all index entries for a view (for rebuild)
-spec clear_all(barrel_store_rocksdb:db_ref(), db_name(), binary()) -> ok.
clear_all(StoreRef, DbName, ViewId) ->
    %% Collect all keys to delete
    IndexPrefix = barrel_store_keys:view_index_prefix(DbName, ViewId),
    IndexEnd = barrel_store_keys:view_index_end(DbName, ViewId),

    %% Collect index keys
    IndexKeys = barrel_store_rocksdb:fold_range(
        StoreRef, IndexPrefix, IndexEnd,
        fun(Key, _Value, Acc) -> {ok, [Key | Acc]} end,
        []
    ),

    %% Collect by_docid keys (we need to scan view_by_docid prefix)
    %% Since view_by_docid keys include the ViewId, we can scan them
    ByDocIdPrefix = barrel_store_keys:view_by_docid(DbName, ViewId, <<>>),
    ByDocIdEnd = <<ByDocIdPrefix/binary, 16#FF>>,

    ByDocIdKeys = barrel_store_rocksdb:fold_range(
        StoreRef, ByDocIdPrefix, ByDocIdEnd,
        fun(Key, _Value, Acc) -> {ok, [Key | Acc]} end,
        []
    ),

    %% Also delete the sequence key
    SeqKey = barrel_store_keys:view_seq(DbName, ViewId),

    %% Build delete operations
    AllKeys = [SeqKey | IndexKeys ++ ByDocIdKeys],
    DeleteOps = [{delete, K} || K <- AllKeys],

    case DeleteOps of
        [] -> ok;
        _ -> barrel_store_rocksdb:write_batch(StoreRef, DeleteOps)
    end.
