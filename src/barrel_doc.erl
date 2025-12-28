%%%-------------------------------------------------------------------
%%% @doc Document utilities for barrel_docdb
%%%
%%% Provides functions for document manipulation, revision handling,
%%% and document hashing.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_doc).

-include("barrel_docdb.hrl").

%% Document accessors
-export([id/1, rev/1, id_rev/1, deleted/1]).

%% Revision operations
-export([
    parse_revision/1,
    make_revision/2,
    revision_hash/3,
    compare_revisions/2
]).

%% Revision history
-export([
    encode_revisions/1,
    parse_revisions/1,
    trim_history/3
]).

%% Document processing
-export([
    make_doc_record/1,
    doc_without_meta/1
]).

%% Unique ID generation
-export([generate_docid/0]).

%%====================================================================
%% Document Accessors
%%====================================================================

%% @doc Get document ID
-spec id(doc()) -> docid() | undefined.
id(#{<<"id">> := Id}) -> Id;
id(#{}) -> undefined;
id(_) -> erlang:error(bad_doc).

%% @doc Get document revision
-spec rev(doc()) -> revid().
rev(#{<<"_rev">> := Rev}) -> Rev;
rev(#{}) -> <<>>;
rev(_) -> error(bad_doc).

%% @doc Get document ID and revision
-spec id_rev(doc()) -> {docid() | undefined, revid()}.
id_rev(#{<<"id">> := Id, <<"_rev">> := Rev}) -> {Id, Rev};
id_rev(#{<<"id">> := Id}) -> {Id, <<>>};
id_rev(#{<<"_rev">> := _Rev}) -> erlang:error(bad_doc);
id_rev(#{}) -> {undefined, <<>>};
id_rev(_) -> erlang:error(bad_doc).

%% @doc Check if document is deleted
-spec deleted(doc()) -> boolean().
deleted(#{<<"_deleted">> := Del}) when is_boolean(Del) -> Del;
deleted(_) -> false.

%%====================================================================
%% Revision Operations
%%====================================================================

%% @doc Parse a revision ID into {Generation, Hash}
-spec parse_revision(revid()) -> {non_neg_integer(), binary()}.
parse_revision(<<>>) -> {0, <<>>};
parse_revision(Rev) when is_binary(Rev) ->
    case binary:split(Rev, <<"-">>) of
        [BinPos, Hash] -> {binary_to_integer(BinPos), Hash};
        _ -> exit({bad_rev, bad_format})
    end;
parse_revision(Rev) when is_list(Rev) ->
    parse_revision(list_to_binary(Rev));
parse_revision(_Rev) ->
    exit({bad_rev, bad_format}).

%% @doc Create a revision ID from generation and hash
-spec make_revision(non_neg_integer(), binary()) -> revid().
make_revision(Gen, Hash) when is_integer(Gen), is_binary(Hash) ->
    <<(integer_to_binary(Gen))/binary, "-", Hash/binary>>.

%% @doc Generate a revision hash for a document
-spec revision_hash(doc(), revid(), boolean()) -> binary().
revision_hash(Doc, Rev, Deleted) ->
    %% Use SHA-256 for content hashing
    Data = term_to_binary({Doc, Rev, Deleted}),
    Digest = crypto:hash(sha256, Data),
    to_hex(Digest).

%% @doc Compare two revisions
%% Returns 1 if RevA > RevB, -1 if RevA < RevB, 0 if equal
-spec compare_revisions(revid(), revid()) -> -1 | 0 | 1.
compare_revisions(RevA, RevB) ->
    TupleA = parse_revision(RevA),
    TupleB = parse_revision(RevB),
    if
        TupleA > TupleB -> 1;
        TupleA < TupleB -> -1;
        true -> 0
    end.

%%====================================================================
%% Revision History
%%====================================================================

%% @doc Encode revision list to compact format
-spec encode_revisions([revid()]) -> map().
encode_revisions([]) ->
    #{<<"start">> => 0, <<"ids">> => []};
encode_revisions(Revs) ->
    [Oldest | _] = Revs,
    {Start, _} = parse_revision(Oldest),
    Digests = lists:foldl(
        fun(Rev, Acc) ->
            {_, Digest} = parse_revision(Rev),
            [Digest | Acc]
        end,
        [],
        Revs
    ),
    #{<<"start">> => Start, <<"ids">> => lists:reverse(Digests)}.

%% @doc Parse revision history from document
-spec parse_revisions(doc()) -> [revid()].
parse_revisions(#{<<"revisions">> := Revisions}) ->
    case Revisions of
        #{<<"start">> := Start, <<"ids">> := Ids} ->
            {Revs, _} = lists:foldl(
                fun(Id, {Acc, I}) ->
                    Rev = <<(integer_to_binary(I))/binary, "-", Id/binary>>,
                    {[Rev | Acc], I - 1}
                end,
                {[], Start},
                Ids
            ),
            lists:reverse(Revs);
        _ ->
            []
    end;
parse_revisions(#{<<"_rev">> := Rev}) ->
    [Rev];
parse_revisions(_) ->
    [].

%% @doc Trim revision history based on ancestors and limit
-spec trim_history(map(), [revid()], non_neg_integer()) -> map().
trim_history(EncodedRevs, Ancestors, Limit) ->
    #{<<"start">> := Start, <<"ids">> := Digests} = EncodedRevs,
    ADigests = array:from_list(Digests),
    {_, Limit2} = lists:foldl(
        fun(Ancestor, {Matched, Unmatched}) ->
            {Gen, Digest} = parse_revision(Ancestor),
            Idx = Start - Gen,
            IsDigest = array:get(Idx, ADigests) =:= Digest,
            if
                Idx >= 0, Idx < Matched, IsDigest =:= true ->
                    {Idx, Idx + 1};
                true ->
                    {Matched, Unmatched}
            end
        end,
        {length(Digests), Limit},
        Ancestors
    ),
    EncodedRevs#{<<"ids">> => lists:sublist(Digests, Limit2)}.

%%====================================================================
%% Document Processing
%%====================================================================

%% @doc Remove metadata fields from document
-spec doc_without_meta(doc()) -> doc().
doc_without_meta(Doc) ->
    maps:filter(
        fun
            (<<"_attachments">>, _) -> false;
            (<<"_", _/binary>>, _) -> false;
            (_, _) -> true
        end,
        Doc
    ).

%% @doc Create internal document record from user document
-spec make_doc_record(doc()) -> map().
make_doc_record(#{<<"id">> := Id, <<"doc">> := Doc0, <<"history">> := History}) ->
    %% Bulk format with explicit history
    Deleted = maps:get(<<"deleted">>, Doc0, false),
    Atts = maps:get(<<"_attachments">>, Doc0, #{}),
    Doc1 = doc_without_meta(Doc0),
    #{
        id => Id,
        ref => erlang:make_ref(),
        revs => History,
        deleted => Deleted,
        attachments => Atts,
        doc => Doc1
    };
make_doc_record(Doc0) ->
    %% Regular document format
    Deleted = maps:get(<<"_deleted">>, Doc0, false),
    Rev = maps:get(<<"_rev">>, Doc0, <<>>),
    Atts = maps:get(<<"_attachments">>, Doc0, #{}),
    Id = case maps:find(<<"id">>, Doc0) of
        {ok, DocId} -> DocId;
        error -> generate_docid()
    end,
    Doc1 = doc_without_meta(Doc0),
    Hash = revision_hash(Doc1, Rev, Deleted),
    Revs = case Rev of
        <<>> ->
            [<<"1-", Hash/binary>>];
        _ ->
            {Gen, _} = parse_revision(Rev),
            NewRev = <<(integer_to_binary(Gen + 1))/binary, "-", Hash/binary>>,
            [NewRev, Rev]
    end,
    #{
        id => Id,
        ref => erlang:make_ref(),
        revs => Revs,
        deleted => Deleted,
        hash => Hash,
        attachments => Atts,
        doc => Doc1
    }.

%%====================================================================
%% ID Generation
%%====================================================================

%% @doc Generate a unique document ID
-spec generate_docid() -> docid().
generate_docid() ->
    %% Use a combination of timestamp and random bytes
    Now = erlang:system_time(microsecond),
    Random = crypto:strong_rand_bytes(8),
    Data = <<Now:64, Random/binary>>,
    to_hex(crypto:hash(md5, Data)).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @doc Convert binary to lowercase hex string
-spec to_hex(binary()) -> binary().
to_hex(Bin) ->
    << <<(hex_char(N))>> || <<N:4>> <= Bin >>.

hex_char(N) when N < 10 -> $0 + N;
hex_char(N) -> $a + N - 10.
