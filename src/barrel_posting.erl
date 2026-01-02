%%%-------------------------------------------------------------------
%%% @doc Posting List Storage for barrel_docdb
%%%
%%% Stores sorted document ID lists per (path_id, value) for O(1)
%%% equality query lookups. Replaces per-document index entries with
%%% inverted index posting lists.
%%%
%%% Posting list format: sorted list of DocIds encoded as
%%% length-prefixed binaries for efficient binary search.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_posting).

-include("barrel_docdb.hrl").

%% API
-export([get/4, get_multi/3]).
-export([add/5, remove/5]).
-export([posting_key/3]).
-export([encode_posting/1, decode_posting/1]).
-export([intersect/2, union/2]).
-export([member/2]).

%% Key prefix for posting lists
-define(PREFIX_POSTING, 16#13).

%%====================================================================
%% Types
%%====================================================================

-type path_id() :: barrel_path_dict:path_id().
-type posting_list() :: [docid()].

-export_type([posting_list/0]).

%%====================================================================
%% API - Read Operations
%%====================================================================

%% @doc Get posting list for a path_id and value.
%% Returns sorted list of document IDs.
-spec get(barrel_store_rocksdb:db_ref(), db_name(), path_id(), term()) ->
    posting_list().
get(StoreRef, DbName, PathId, Value) ->
    Key = posting_key(DbName, PathId, Value),
    case barrel_store_rocksdb:posting_get(StoreRef, Key) of
        {ok, Bin} -> decode_posting(Bin);
        not_found -> []
    end.

%% @doc Get multiple posting lists in a single batch read.
%% Returns list of posting lists in same order as input.
-spec get_multi(barrel_store_rocksdb:db_ref(), db_name(),
                [{path_id(), term()}]) -> [posting_list()].
get_multi(StoreRef, DbName, PathValues) ->
    Keys = [posting_key(DbName, PathId, Value) || {PathId, Value} <- PathValues],
    Results = barrel_store_rocksdb:posting_multi_get(StoreRef, Keys),
    [case R of
        {ok, Bin} -> decode_posting(Bin);
        not_found -> []
    end || R <- Results].

%%====================================================================
%% API - Write Operations
%%====================================================================

%% @doc Add a document ID to a posting list.
%% Note: Callers should serialize writes through barrel_posting_writer
%% for concurrent safety in production use.
-spec add(barrel_store_rocksdb:db_ref(), db_name(), path_id(), term(), docid()) ->
    ok | {error, term()}.
add(StoreRef, DbName, PathId, Value, DocId) ->
    Key = posting_key(DbName, PathId, Value),
    %% Read-modify-write
    Current = case barrel_store_rocksdb:posting_get(StoreRef, Key) of
        {ok, Bin} -> decode_posting(Bin);
        not_found -> []
    end,
    Updated = ordsets:add_element(DocId, Current),
    barrel_store_rocksdb:posting_put(StoreRef, Key, encode_posting(Updated)).

%% @doc Remove a document ID from a posting list.
%% Note: Callers should serialize writes through barrel_posting_writer
%% for concurrent safety in production use.
-spec remove(barrel_store_rocksdb:db_ref(), db_name(), path_id(), term(), docid()) ->
    ok | {error, term()}.
remove(StoreRef, DbName, PathId, Value, DocId) ->
    Key = posting_key(DbName, PathId, Value),
    %% Read-modify-write
    Current = case barrel_store_rocksdb:posting_get(StoreRef, Key) of
        {ok, Bin} -> decode_posting(Bin);
        not_found -> []
    end,
    Updated = ordsets:del_element(DocId, Current),
    case Updated of
        [] -> barrel_store_rocksdb:posting_delete(StoreRef, Key);
        _ -> barrel_store_rocksdb:posting_put(StoreRef, Key, encode_posting(Updated))
    end.

%%====================================================================
%% API - Set Operations
%%====================================================================

%% @doc Intersect two posting lists (AND operation).
%% Both lists must be sorted. Returns sorted result.
-spec intersect(posting_list(), posting_list()) -> posting_list().
intersect([], _) -> [];
intersect(_, []) -> [];
intersect([H|T1], [H|T2]) -> [H | intersect(T1, T2)];
intersect([H1|T1], [H2|_]=L2) when H1 < H2 -> intersect(T1, L2);
intersect(L1, [_|T2]) -> intersect(L1, T2).

%% @doc Union two posting lists (OR operation).
%% Both lists must be sorted. Returns sorted result.
-spec union(posting_list(), posting_list()) -> posting_list().
union([], L2) -> L2;
union(L1, []) -> L1;
union([H|T1], [H|T2]) -> [H | union(T1, T2)];
union([H1|T1], [H2|_]=L2) when H1 < H2 -> [H1 | union(T1, L2)];
union(L1, [H2|T2]) -> [H2 | union(L1, T2)].

%% @doc Check if a DocId is in a posting list.
%% Uses binary search for efficiency.
-spec member(docid(), posting_list()) -> boolean().
member(_DocId, []) -> false;
member(DocId, List) ->
    %% For small lists, linear search is faster
    case length(List) < 16 of
        true -> lists:member(DocId, List);
        false -> binary_search(DocId, list_to_tuple(List))
    end.

%%====================================================================
%% Key Encoding
%%====================================================================

%% @doc Create posting list key.
%% Format: PREFIX + DbName (length-prefixed) + PathId:32 + EncodedValue
-spec posting_key(db_name(), path_id(), term()) -> binary().
posting_key(DbName, PathId, Value) ->
    EncodedValue = encode_value(Value),
    <<?PREFIX_POSTING, (encode_name(DbName))/binary,
      PathId:32/big-unsigned, EncodedValue/binary>>.

%%====================================================================
%% Posting List Encoding
%%====================================================================

%% @doc Encode a posting list to binary.
%% Uses length-prefixed encoding for variable-length DocIds.
%% DocIds are stored sorted for efficient binary search.
-spec encode_posting(posting_list()) -> binary().
encode_posting([]) ->
    <<>>;
encode_posting(DocIds) ->
    Sorted = lists:usort(DocIds),
    iolist_to_binary([encode_docid(Id) || Id <- Sorted]).

%% @doc Decode a posting list from binary.
-spec decode_posting(binary()) -> posting_list().
decode_posting(<<>>) ->
    [];
decode_posting(Bin) ->
    decode_docids(Bin, []).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private Encode database name with length prefix
encode_name(Name) when is_binary(Name) ->
    Len = byte_size(Name),
    <<Len:16, Name/binary>>.

%% @private Encode a value for the posting key
%% Uses barrel_store_keys encoding for consistent ordering
encode_value(Value) ->
    barrel_store_keys:encode_path([Value]).

%% @private Encode a single DocId with length prefix
encode_docid(DocId) when is_binary(DocId) ->
    Len = byte_size(DocId),
    <<Len:16, DocId/binary>>.

%% @private Decode DocIds from binary
decode_docids(<<>>, Acc) ->
    lists:reverse(Acc);
decode_docids(<<Len:16, DocId:Len/binary, Rest/binary>>, Acc) ->
    decode_docids(Rest, [DocId | Acc]).

%% @private Binary search in a tuple
binary_search(Target, Tuple) ->
    binary_search(Target, Tuple, 1, tuple_size(Tuple)).

binary_search(_Target, _Tuple, Low, High) when Low > High ->
    false;
binary_search(Target, Tuple, Low, High) ->
    Mid = (Low + High) div 2,
    MidVal = element(Mid, Tuple),
    if
        Target < MidVal -> binary_search(Target, Tuple, Low, Mid - 1);
        Target > MidVal -> binary_search(Target, Tuple, Mid + 1, High);
        true -> true
    end.
