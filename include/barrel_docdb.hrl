%%%-------------------------------------------------------------------
%%% @doc barrel_docdb header file
%%%
%%% Contains type definitions and common macros for barrel_docdb.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(BARREL_DOCDB_HRL).
-define(BARREL_DOCDB_HRL, true).

%%====================================================================
%% Type Definitions
%%====================================================================

%% Database types
-type db_name() :: binary().
%% Database name (unique identifier).

-type db_ref() :: pid() | atom().
%% Reference to a database (pid or registered name).

-type db_config() :: #{
    path => string(),
    store => module(),
    atom() => term()
}.
%% Database configuration options.

%% Document types
-type docid() :: binary().
%% Unique document identifier.

-type revid() :: binary().
%% Revision identifier in format "N-HASH" where N is generation.

-type doc() :: #{
    <<"id">> := docid(),
    <<"_rev">> => revid(),
    binary() => term()
}.
%% JSON document as an Erlang map.

-type doc_info() :: #{
    id := docid(),
    rev := revid(),
    deleted := boolean(),
    revtree := revtree()
}.
%% Internal document metadata.

%% Revision tree types
-type revtree() :: #{revid() => rev_info()}.
%% Revision tree mapping revisions to their info.

-type rev_info() :: #{
    id := revid(),
    parent := revid() | <<>>,
    deleted := boolean(),
    attachments => #{binary() => att_info()}
}.
%% Information about a single revision.

%% Attachment types
-type att_info() :: #{
    name := binary(),
    content_type := binary(),
    length := non_neg_integer(),
    digest := binary()
}.
%% Attachment metadata.

%% Sequence types
-type seq() :: {non_neg_integer(), non_neg_integer()}.
%% Sequence number as {Epoch, Counter} tuple.

-type seq_string() :: binary().
%% Sequence number as a string for external use.

%% Changes types
-type change() :: #{
    <<"id">> := docid(),
    <<"seq">> := seq_string(),
    <<"changes">> := [#{<<"rev">> := revid()}],
    <<"deleted">> => boolean(),
    <<"doc">> => doc()
}.
%% A single change entry.

%% View types
-type view_name() :: binary().
%% View name.

-type view_result() :: #{
    key := term(),
    value := term(),
    id := docid()
}.
%% A single view result row.

%% Replication types
-type endpoint() :: db_name() | {node(), db_name()} | {module(), term()}.
%% Replication endpoint - local db, remote Erlang node, or custom transport.

-type rep_options() :: #{
    continuous => boolean(),
    since => seq_string(),
    filter => fun((doc()) -> boolean()),
    atom() => term()
}.
%% Replication options.

%%====================================================================
%% Export type definitions
%%====================================================================

-export_type([
    db_name/0, db_ref/0, db_config/0,
    docid/0, revid/0, doc/0, doc_info/0,
    revtree/0, rev_info/0,
    att_info/0,
    seq/0, seq_string/0,
    change/0,
    view_name/0, view_result/0,
    endpoint/0, rep_options/0
]).

%%====================================================================
%% Macros
%%====================================================================

%% Default configuration values
-define(DEFAULT_DATA_DIR, "data/barrel_docdb").
-define(DEFAULT_STORE_MODULE, barrel_store_rocksdb).

%% Key prefixes for RocksDB storage
-define(PREFIX_DB_META, 16#01).
-define(PREFIX_DOC_INFO, 16#02).
-define(PREFIX_DOC_REV, 16#03).
-define(PREFIX_DOC_SEQ, 16#04).
-define(PREFIX_VIEW, 16#05).
-define(PREFIX_ATT, 16#06).
-define(PREFIX_LOCAL, 16#07).

-endif. %% BARREL_DOCDB_HRL
