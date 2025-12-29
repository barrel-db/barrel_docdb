%%%-------------------------------------------------------------------
%%% @doc barrel_rep_transport - Transport behaviour for replication
%%%
%%% This behaviour defines the interface for replication transports.
%%% Transports abstract the communication with databases, allowing
%%% replication to work between local databases or remote ones.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_transport).

-include("barrel_docdb.hrl").

%% Behaviour callbacks

%% @doc Get a document with options
%% Options may include: rev, history
-callback get_doc(Endpoint :: term(), DocId :: docid(), Opts :: map()) ->
    {ok, Doc :: map(), Meta :: map()} | {error, not_found} | {error, term()}.

%% @doc Put a document with explicit revision history (replication)
-callback put_rev(Endpoint :: term(), Doc :: map(), History :: [revid()], Deleted :: boolean()) ->
    {ok, DocId :: docid(), RevId :: revid()} | {error, term()}.

%% @doc Get revision differences
%% Returns {ok, MissingRevs, PossibleAncestors}
-callback revsdiff(Endpoint :: term(), DocId :: docid(), RevIds :: [revid()]) ->
    {ok, Missing :: [revid()], Ancestors :: [revid()]} | {error, term()}.

%% @doc Get changes since a sequence
-callback get_changes(Endpoint :: term(), Since :: seq() | first, Opts :: map()) ->
    {ok, Changes :: [map()], LastSeq :: seq()} | {error, term()}.

%% @doc Get a local document (for checkpoints)
-callback get_local_doc(Endpoint :: term(), DocId :: docid()) ->
    {ok, Doc :: map()} | {error, not_found} | {error, term()}.

%% @doc Put a local document (for checkpoints)
-callback put_local_doc(Endpoint :: term(), DocId :: docid(), Doc :: map()) ->
    ok | {error, term()}.

%% @doc Delete a local document
-callback delete_local_doc(Endpoint :: term(), DocId :: docid()) ->
    ok | {error, not_found} | {error, term()}.

%% @doc Get database info
-callback db_info(Endpoint :: term()) ->
    {ok, Info :: map()} | {error, term()}.
