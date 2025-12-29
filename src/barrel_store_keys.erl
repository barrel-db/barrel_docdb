%%%-------------------------------------------------------------------
%%% @doc Key encoding for barrel_docdb storage
%%%
%%% Provides functions to encode and decode keys for RocksDB storage.
%%% Keys are prefixed to enable efficient range scans.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_store_keys).

-include("barrel_docdb.hrl").

%% Database metadata keys
-export([db_meta/2, db_uid/1, db_docs_count/1, db_del_count/1]).

%% Document keys
-export([doc_info/2, doc_info_prefix/1, doc_info_end/1]).
-export([doc_rev/3, doc_rev_prefix/2]).
-export([doc_seq/2, doc_seq_prefix/1, doc_seq_end/1]).

%% Local document keys
-export([local_doc/2]).

%% View keys
-export([view_meta/2, view_seq/2, view_index/3, view_index_prefix/2, view_index_end/2]).
-export([view_by_docid/3, view_by_docid_prefix/3, view_by_docid_end/3]).
-export([encode_view_key/1, decode_view_key/1]).

%% Path index keys
-export([path_index_key/3, path_index_prefix/2, path_index_end/2]).
-export([doc_paths_key/2, doc_paths_prefix/1]).
-export([encode_path/1, decode_path/1]).

%% Attachment keys
-export([att_data/3, att_data_prefix/2]).

%% Sequence encoding/decoding
-export([encode_seq/1, decode_seq/1]).

%% Key decoding
-export([decode_doc_id/2, decode_seq_key/2]).

%%====================================================================
%% Key Prefixes - single byte for efficiency
%%====================================================================

%% Key type prefixes
-define(PREFIX_DB_META, 16#01).
-define(PREFIX_DOC_INFO, 16#02).
-define(PREFIX_DOC_REV, 16#03).
-define(PREFIX_DOC_SEQ, 16#04).
-define(PREFIX_LOCAL_DOC, 16#05).
-define(PREFIX_VIEW_META, 16#06).
-define(PREFIX_VIEW_SEQ, 16#07).
-define(PREFIX_VIEW_INDEX, 16#08).
-define(PREFIX_VIEW_BY_DOCID, 16#09).
-define(PREFIX_ATT, 16#0A).
-define(PREFIX_PATH_INDEX, 16#0B).
-define(PREFIX_DOC_PATHS, 16#0C).

%% Path component type tags (for ordered encoding)
-define(PATH_TYPE_NULL, 16#01).
-define(PATH_TYPE_FALSE, 16#02).
-define(PATH_TYPE_TRUE, 16#03).
-define(PATH_TYPE_NEG_INT, 16#10).  %% Negative integers
-define(PATH_TYPE_ZERO, 16#20).     %% Zero
-define(PATH_TYPE_POS_INT, 16#30).  %% Positive integers
-define(PATH_TYPE_FLOAT, 16#40).    %% Floats
-define(PATH_TYPE_BINARY, 16#50).   %% Binary strings

%% Meta key suffixes
-define(META_UID, <<"uid">>).
-define(META_DOCS_COUNT, <<"docs_count">>).
-define(META_DEL_COUNT, <<"del_count">>).

%%====================================================================
%% Database Metadata Keys
%%====================================================================

%% @doc General database metadata key
-spec db_meta(db_name(), binary()) -> binary().
db_meta(DbName, MetaKey) ->
    <<?PREFIX_DB_META, (encode_name(DbName))/binary, $:, MetaKey/binary>>.

%% @doc Database UID key
-spec db_uid(db_name()) -> binary().
db_uid(DbName) ->
    db_meta(DbName, ?META_UID).

%% @doc Documents count key
-spec db_docs_count(db_name()) -> binary().
db_docs_count(DbName) ->
    db_meta(DbName, ?META_DOCS_COUNT).

%% @doc Deleted documents count key
-spec db_del_count(db_name()) -> binary().
db_del_count(DbName) ->
    db_meta(DbName, ?META_DEL_COUNT).

%%====================================================================
%% Document Keys
%%====================================================================

%% @doc Document info key (stores doc_info record)
-spec doc_info(db_name(), docid()) -> binary().
doc_info(DbName, DocId) ->
    <<?PREFIX_DOC_INFO, (encode_name(DbName))/binary, DocId/binary>>.

%% @doc Prefix for all doc_info keys in a database
-spec doc_info_prefix(db_name()) -> binary().
doc_info_prefix(DbName) ->
    <<?PREFIX_DOC_INFO, (encode_name(DbName))/binary>>.

%% @doc End marker for doc_info range scan
-spec doc_info_end(db_name()) -> binary().
doc_info_end(DbName) ->
    <<?PREFIX_DOC_INFO, (encode_name(DbName))/binary, 16#FF>>.

%% @doc Document revision key (stores document body)
-spec doc_rev(db_name(), docid(), revid()) -> binary().
doc_rev(DbName, DocId, RevId) ->
    <<?PREFIX_DOC_REV, (encode_name(DbName))/binary, DocId/binary, $:, RevId/binary>>.

%% @doc Prefix for all revisions of a document
-spec doc_rev_prefix(db_name(), docid()) -> binary().
doc_rev_prefix(DbName, DocId) ->
    <<?PREFIX_DOC_REV, (encode_name(DbName))/binary, DocId/binary, $:>>.

%% @doc Document sequence key (for changes feed)
-spec doc_seq(db_name(), seq()) -> binary().
doc_seq(DbName, Seq) ->
    <<?PREFIX_DOC_SEQ, (encode_name(DbName))/binary, (encode_seq(Seq))/binary>>.

%% @doc Prefix for all sequence keys
-spec doc_seq_prefix(db_name()) -> binary().
doc_seq_prefix(DbName) ->
    <<?PREFIX_DOC_SEQ, (encode_name(DbName))/binary>>.

%% @doc End marker for sequence range scan
-spec doc_seq_end(db_name()) -> binary().
doc_seq_end(DbName) ->
    <<?PREFIX_DOC_SEQ, (encode_name(DbName))/binary, 16#FF, 16#FF, 16#FF, 16#FF,
      16#FF, 16#FF, 16#FF, 16#FF>>.

%%====================================================================
%% Local Document Keys
%%====================================================================

%% @doc Local document key (not replicated)
-spec local_doc(db_name(), docid()) -> binary().
local_doc(DbName, DocId) ->
    <<?PREFIX_LOCAL_DOC, (encode_name(DbName))/binary, DocId/binary>>.

%%====================================================================
%% View Keys
%%====================================================================

%% @doc View metadata key
-spec view_meta(db_name(), binary()) -> binary().
view_meta(DbName, ViewId) ->
    <<?PREFIX_VIEW_META, (encode_name(DbName))/binary, ViewId/binary>>.

%% @doc View indexed sequence key
-spec view_seq(db_name(), binary()) -> binary().
view_seq(DbName, ViewId) ->
    <<?PREFIX_VIEW_SEQ, (encode_name(DbName))/binary, ViewId/binary>>.

%% @doc View index entry key
-spec view_index(db_name(), binary(), binary()) -> binary().
view_index(DbName, ViewId, IndexKey) ->
    <<?PREFIX_VIEW_INDEX, (encode_name(DbName))/binary, ViewId/binary, $:, IndexKey/binary>>.

%% @doc Prefix for view index entries
-spec view_index_prefix(db_name(), binary()) -> binary().
view_index_prefix(DbName, ViewId) ->
    <<?PREFIX_VIEW_INDEX, (encode_name(DbName))/binary, ViewId/binary, $:>>.

%% @doc End marker for view index range scan
-spec view_index_end(db_name(), binary()) -> binary().
view_index_end(DbName, ViewId) ->
    <<?PREFIX_VIEW_INDEX, (encode_name(DbName))/binary, ViewId/binary, $:, 16#FF>>.

%% @doc View by docid key (tracks which index entries belong to each doc)
-spec view_by_docid(db_name(), binary(), docid()) -> binary().
view_by_docid(DbName, ViewId, DocId) ->
    <<?PREFIX_VIEW_BY_DOCID, (encode_name(DbName))/binary, ViewId/binary, $:, DocId/binary>>.

%% @doc Prefix for view by docid entries
-spec view_by_docid_prefix(db_name(), binary(), docid()) -> binary().
view_by_docid_prefix(DbName, ViewId, DocId) ->
    <<?PREFIX_VIEW_BY_DOCID, (encode_name(DbName))/binary, ViewId/binary, $:, DocId/binary>>.

%% @doc End marker for view by docid range scan
-spec view_by_docid_end(db_name(), binary(), docid()) -> binary().
view_by_docid_end(DbName, ViewId, DocId) ->
    <<?PREFIX_VIEW_BY_DOCID, (encode_name(DbName))/binary, ViewId/binary, $:, DocId/binary, 16#FF>>.

%% @doc Encode a view key for sorted storage
%% Uses term_to_binary with ordered encoding to preserve Erlang term ordering
-spec encode_view_key(term()) -> binary().
encode_view_key(Key) ->
    term_to_binary(Key, [{minor_version, 2}]).

%% @doc Decode a view key
-spec decode_view_key(binary()) -> term().
decode_view_key(Bin) ->
    binary_to_term(Bin).

%%====================================================================
%% Attachment Keys
%%====================================================================

%% @doc Attachment data key
-spec att_data(db_name(), docid(), binary()) -> binary().
att_data(DbName, DocId, AttName) ->
    <<?PREFIX_ATT, (encode_name(DbName))/binary, DocId/binary, $:, AttName/binary>>.

%% @doc Prefix for document attachments
-spec att_data_prefix(db_name(), docid()) -> binary().
att_data_prefix(DbName, DocId) ->
    <<?PREFIX_ATT, (encode_name(DbName))/binary, DocId/binary, $:>>.

%%====================================================================
%% Encoding/Decoding Helpers
%%====================================================================

%% @doc Encode database name with length prefix
-spec encode_name(db_name()) -> binary().
encode_name(Name) when is_binary(Name) ->
    Len = byte_size(Name),
    <<Len:16, Name/binary>>.

%% @doc Encode sequence number (epoch:32, counter:32 big-endian for sort order)
-spec encode_seq(seq()) -> binary().
encode_seq({Epoch, Counter}) when is_integer(Epoch), is_integer(Counter) ->
    <<Epoch:32/big-unsigned, Counter:32/big-unsigned>>.

%% @doc Decode sequence number
-spec decode_seq(binary()) -> seq().
decode_seq(<<Epoch:32/big-unsigned, Counter:32/big-unsigned>>) ->
    {Epoch, Counter}.

%% @doc Extract doc_id from a doc_info key
-spec decode_doc_id(db_name(), binary()) -> docid().
decode_doc_id(DbName, Key) ->
    Prefix = doc_info_prefix(DbName),
    PrefixLen = byte_size(Prefix),
    <<Prefix:PrefixLen/binary, DocId/binary>> = Key,
    DocId.

%% @doc Extract sequence from a seq key
-spec decode_seq_key(db_name(), binary()) -> seq().
decode_seq_key(DbName, Key) ->
    Prefix = doc_seq_prefix(DbName),
    PrefixLen = byte_size(Prefix),
    <<Prefix:PrefixLen/binary, SeqBin/binary>> = Key,
    decode_seq(SeqBin).

%%====================================================================
%% Path Index Keys
%%====================================================================

%% @doc Path index key for a document path.
%% Key format: prefix | db_name | encoded_path | docid
%% Path includes the value at the end: [field1, field2, value]
-spec path_index_key(db_name(), [term()], docid()) -> binary().
path_index_key(DbName, Path, DocId) ->
    EncodedPath = encode_path(Path),
    <<?PREFIX_PATH_INDEX, (encode_name(DbName))/binary, EncodedPath/binary, DocId/binary>>.

%% @doc Prefix for scanning path index entries.
%% Can be used with partial paths for prefix scans.
-spec path_index_prefix(db_name(), [term()]) -> binary().
path_index_prefix(DbName, Path) ->
    EncodedPath = encode_path(Path),
    <<?PREFIX_PATH_INDEX, (encode_name(DbName))/binary, EncodedPath/binary>>.

%% @doc End marker for path index range scan.
-spec path_index_end(db_name(), [term()]) -> binary().
path_index_end(DbName, Path) ->
    EncodedPath = encode_path(Path),
    <<?PREFIX_PATH_INDEX, (encode_name(DbName))/binary, EncodedPath/binary, 16#FF>>.

%% @doc Reverse index key: doc_id -> list of indexed paths.
%% Used to remove old paths when updating a document.
-spec doc_paths_key(db_name(), docid()) -> binary().
doc_paths_key(DbName, DocId) ->
    <<?PREFIX_DOC_PATHS, (encode_name(DbName))/binary, DocId/binary>>.

%% @doc Prefix for doc_paths keys.
-spec doc_paths_prefix(db_name()) -> binary().
doc_paths_prefix(DbName) ->
    <<?PREFIX_DOC_PATHS, (encode_name(DbName))/binary>>.

%% @doc Encode a path for lexicographic ordering.
%% Path components are encoded with length prefix and type tags.
%% This ensures correct sort order across different types.
-spec encode_path([term()]) -> binary().
encode_path(Path) when is_list(Path) ->
    iolist_to_binary([encode_path_component(C) || C <- Path]).

%% @doc Decode a path from binary.
-spec decode_path(binary()) -> [term()].
decode_path(Bin) ->
    decode_path_components(Bin, []).

%%====================================================================
%% Path Component Encoding
%%====================================================================

%% @private Encode a single path component with type tag for ordering
encode_path_component(null) ->
    <<?PATH_TYPE_NULL>>;
encode_path_component(false) ->
    <<?PATH_TYPE_FALSE>>;
encode_path_component(true) ->
    <<?PATH_TYPE_TRUE>>;
encode_path_component(0) ->
    <<?PATH_TYPE_ZERO>>;
encode_path_component(N) when is_integer(N), N > 0 ->
    %% Positive integers: encode with length prefix for proper ordering
    Bin = integer_to_binary(N),
    Len = byte_size(Bin),
    <<?PATH_TYPE_POS_INT, Len:8, Bin/binary>>;
encode_path_component(N) when is_integer(N), N < 0 ->
    %% Negative integers: invert and encode for proper ordering
    %% -1 should sort after -1000, so we use complement
    Abs = abs(N),
    Bin = integer_to_binary(Abs),
    Len = byte_size(Bin),
    %% Invert the length so larger negative numbers sort first
    InvLen = 255 - Len,
    %% Invert each byte so -1 > -2
    InvBin = << <<(255 - B)>> || <<B>> <= Bin >>,
    <<?PATH_TYPE_NEG_INT, InvLen:8, InvBin/binary>>;
encode_path_component(F) when is_float(F) ->
    %% Floats: use IEEE 754 encoding with sign adjustment
    <<?PATH_TYPE_FLOAT, (encode_float(F))/binary>>;
encode_path_component(Bin) when is_binary(Bin) ->
    %% Binary: escape null bytes and terminate for lexicographic order
    %% 0x00 -> 0x00 0xFF (escape), end with 0x00 0x00
    Escaped = escape_binary(Bin),
    <<?PATH_TYPE_BINARY, Escaped/binary, 0, 0>>.

%% @private Encode float for lexicographic ordering
encode_float(F) when F >= 0 ->
    <<Bits:64/big-unsigned>> = <<F:64/float>>,
    <<(Bits bxor 16#8000000000000000):64/big-unsigned>>;
encode_float(F) ->
    <<Bits:64/big-unsigned>> = <<F:64/float>>,
    <<(bnot Bits):64/big-unsigned>>.

%% @private Decode float from lexicographic encoding
decode_float(<<Encoded:64/big-unsigned>>) ->
    %% Check if sign bit is set (was positive)
    Bits = case Encoded band 16#8000000000000000 of
        0 -> bnot Encoded;  %% Was negative
        _ -> Encoded bxor 16#8000000000000000  %% Was positive
    end,
    <<F:64/float>> = <<Bits:64/big-unsigned>>,
    F.

%% @private Escape null bytes in binary for lexicographic encoding
%% 0x00 -> 0x00 0xFF
escape_binary(Bin) ->
    escape_binary(Bin, <<>>).

escape_binary(<<>>, Acc) ->
    Acc;
escape_binary(<<0, Rest/binary>>, Acc) ->
    escape_binary(Rest, <<Acc/binary, 0, 16#FF>>);
escape_binary(<<B, Rest/binary>>, Acc) ->
    escape_binary(Rest, <<Acc/binary, B>>).

%% @private Unescape binary from lexicographic encoding
%% 0x00 0xFF -> 0x00, 0x00 0x00 = end
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

%% @private Decode path components from binary
decode_path_components(<<>>, Acc) ->
    lists:reverse(Acc);
decode_path_components(<<?PATH_TYPE_NULL, Rest/binary>>, Acc) ->
    decode_path_components(Rest, [null | Acc]);
decode_path_components(<<?PATH_TYPE_FALSE, Rest/binary>>, Acc) ->
    decode_path_components(Rest, [false | Acc]);
decode_path_components(<<?PATH_TYPE_TRUE, Rest/binary>>, Acc) ->
    decode_path_components(Rest, [true | Acc]);
decode_path_components(<<?PATH_TYPE_ZERO, Rest/binary>>, Acc) ->
    decode_path_components(Rest, [0 | Acc]);
decode_path_components(<<?PATH_TYPE_POS_INT, Len:8, Bin:Len/binary, Rest/binary>>, Acc) ->
    N = binary_to_integer(Bin),
    decode_path_components(Rest, [N | Acc]);
decode_path_components(<<?PATH_TYPE_NEG_INT, InvLen:8, InvBin/binary>>, Acc) ->
    Len = 255 - InvLen,
    <<InvBytes:Len/binary, Rest/binary>> = InvBin,
    Bin = << <<(255 - B)>> || <<B>> <= InvBytes >>,
    N = -binary_to_integer(Bin),
    decode_path_components(Rest, [N | Acc]);
decode_path_components(<<?PATH_TYPE_FLOAT, Encoded:8/binary, Rest/binary>>, Acc) ->
    F = decode_float(Encoded),
    decode_path_components(Rest, [F | Acc]);
decode_path_components(<<?PATH_TYPE_BINARY, Rest/binary>>, Acc) ->
    {Bin, Rest2} = unescape_binary(Rest),
    decode_path_components(Rest2, [Bin | Acc]).
