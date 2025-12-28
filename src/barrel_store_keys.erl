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
-export([view_meta/2, view_seq/2, view_index/3, view_index_prefix/2]).

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
-define(PREFIX_ATT, 16#09).

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
