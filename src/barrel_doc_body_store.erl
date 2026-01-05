%%%-------------------------------------------------------------------
%%% @doc Document body storage using body column family (BlobDB enabled)
%%%
%%% Document bodies are stored in the "bodies" column family which has
%%% BlobDB enabled. This keeps the main LSM tree lean (only indexes and
%%% metadata) and provides efficient batch fetching with multi_get.
%%%
%%% The store is accessed via persistent_term registry using the db name.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_doc_body_store).

-include("barrel_docdb.hrl").

%% API
-export([get_body/3, multi_get_bodies/3]).

%%====================================================================
%% API
%%====================================================================

%% @doc Get a document body by looking up the store from persistent_term.
%% This allows query code to fetch doc bodies without passing store_ref around.
-spec get_body(db_name(), docid(), revid()) ->
    {ok, binary()} | not_found | {error, term()}.
get_body(DbName, DocId, RevId) ->
    case persistent_term:get({barrel_store, DbName}, undefined) of
        undefined ->
            {error, store_not_found};
        StoreRef ->
            Key = barrel_store_keys:doc_body(DbName, DocId, RevId),
            barrel_store_rocksdb:body_get(StoreRef, Key)
    end.

%% @doc Batch get document bodies by looking up the store from persistent_term.
%% DocIdRevPairs is a list of {DocId, RevId} tuples.
%% Returns results in same order as input.
-spec multi_get_bodies(db_name(), [{docid(), revid()}], map()) ->
    [{ok, binary()} | not_found | {error, term()}].
multi_get_bodies(DbName, DocIdRevPairs, _Opts) ->
    case persistent_term:get({barrel_store, DbName}, undefined) of
        undefined ->
            [{error, store_not_found} || _ <- DocIdRevPairs];
        StoreRef ->
            Keys = [barrel_store_keys:doc_body(DbName, DocId, RevId)
                    || {DocId, RevId} <- DocIdRevPairs],
            barrel_store_rocksdb:body_multi_get(StoreRef, Keys)
    end.
