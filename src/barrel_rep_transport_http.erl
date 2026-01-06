%%%-------------------------------------------------------------------
%%% @doc barrel_rep_transport_http - HTTP transport for replication
%%%
%%% Transport implementation for replicating between remote databases
%%% over HTTP. The endpoint is a URL like `http://host:port/db/dbname'.
%%%
%%% Supports both JSON and CBOR content types via Accept/Content-Type
%%% headers. CBOR is used by default for efficiency.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_transport_http).

-behaviour(barrel_rep_transport).

-include("barrel_docdb.hrl").

%% barrel_rep_transport callbacks
-export([
    get_doc/3,
    put_rev/4,
    revsdiff/3,
    get_changes/3,
    get_local_doc/2,
    put_local_doc/3,
    delete_local_doc/2,
    db_info/1,
    sync_hlc/2
]).

%% API
-export([parse_endpoint/1]).

%% Content types
-define(CT_JSON, <<"application/json">>).
-define(CT_CBOR, <<"application/cbor">>).

%% Default timeout for HTTP requests (30 seconds)
-define(DEFAULT_TIMEOUT, 30000).

%%====================================================================
%% Types
%%====================================================================

-type http_endpoint() :: #{
    url := binary(),
    content_type => json | cbor,
    timeout => pos_integer(),
    %% Authentication options
    bearer_token => binary(),           % Bearer token for Authorization header
    basic_auth => {binary(), binary()}, % {Username, Password} for Basic auth
    %% SSL/TLS options
    ssl_options => [ssl:tls_client_option()], % Custom SSL options
    insecure => boolean()               % Skip certificate verification (NOT recommended)
}.

-export_type([http_endpoint/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Parse a URL into an endpoint configuration
%% Can be a simple URL binary or a map with options
-spec parse_endpoint(binary() | map()) -> http_endpoint().
parse_endpoint(Url) when is_binary(Url) ->
    #{url => Url, content_type => cbor};
parse_endpoint(#{url := _} = Endpoint) ->
    maps:merge(#{content_type => cbor}, Endpoint).

%%====================================================================
%% barrel_rep_transport callbacks
%%====================================================================

%% @doc Get a document with options
-spec get_doc(http_endpoint() | binary(), docid(), map()) ->
    {ok, map(), map()} | {error, not_found} | {error, term()}.
get_doc(Endpoint, DocId, Opts) ->
    #{url := BaseUrl} = E = parse_endpoint(Endpoint),
    Url = build_doc_url(BaseUrl, DocId, Opts),
    case http_get(E, Url) of
        {ok, 200, _Headers, Body} ->
            Doc = decode_body(E, Body),
            Rev = maps:get(<<"_rev">>, Doc, undefined),
            Meta = #{
                <<"rev">> => Rev,
                <<"deleted">> => maps:get(<<"_deleted">>, Doc, false)
            },
            %% Include revisions if requested and present
            Meta2 = case maps:get(<<"_revisions">>, Doc, undefined) of
                undefined -> Meta;
                Revisions -> Meta#{<<"revisions">> => Revisions}
            end,
            {ok, Doc, Meta2};
        {ok, 404, _Headers, _Body} ->
            {error, not_found};
        {ok, Status, _Headers, Body} ->
            {error, {http_error, Status, decode_error(E, Body)}};
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%% @doc Put a document with explicit revision history
-spec put_rev(http_endpoint() | binary(), map(), [revid()], boolean()) ->
    {ok, docid(), revid()} | {error, term()}.
put_rev(Endpoint, Doc, History, Deleted) ->
    #{url := BaseUrl} = E = parse_endpoint(Endpoint),
    Url = <<BaseUrl/binary, "/_put_rev">>,
    ReqBody = #{
        <<"doc">> => Doc,
        <<"history">> => History,
        <<"deleted">> => Deleted
    },
    case http_post(E, Url, ReqBody) of
        {ok, 201, _Headers, Body} ->
            #{<<"id">> := DocId, <<"rev">> := RevId} = decode_body(E, Body),
            {ok, DocId, RevId};
        {ok, Status, _Headers, Body} ->
            {error, {http_error, Status, decode_error(E, Body)}};
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%% @doc Get revision differences
-spec revsdiff(http_endpoint() | binary(), docid(), [revid()]) ->
    {ok, [revid()], [revid()]} | {error, term()}.
revsdiff(Endpoint, DocId, RevIds) ->
    #{url := BaseUrl} = E = parse_endpoint(Endpoint),
    Url = <<BaseUrl/binary, "/_revsdiff">>,
    ReqBody = #{<<"id">> => DocId, <<"revs">> => RevIds},
    case http_post(E, Url, ReqBody) of
        {ok, 200, _Headers, Body} ->
            #{<<"missing">> := Missing, <<"possible_ancestors">> := Ancestors} =
                decode_body(E, Body),
            {ok, Missing, Ancestors};
        {ok, Status, _Headers, Body} ->
            {error, {http_error, Status, decode_error(E, Body)}};
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%% @doc Get changes since a sequence
-spec get_changes(http_endpoint() | binary(), seq() | first, map()) ->
    {ok, [map()], seq()} | {error, term()}.
get_changes(Endpoint, Since, Opts) ->
    #{url := BaseUrl} = E = parse_endpoint(Endpoint),
    Url = build_changes_url(BaseUrl, Since, Opts),
    case http_get(E, Url) of
        {ok, 200, _Headers, Body} ->
            #{<<"results">> := Results, <<"last_seq">> := LastSeqBin} =
                decode_body(E, Body),
            %% Parse the HLC from string representation
            LastSeq = parse_hlc(LastSeqBin),
            {ok, Results, LastSeq};
        {ok, Status, _Headers, Body} ->
            {error, {http_error, Status, decode_error(E, Body)}};
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%% @doc Get a local document
-spec get_local_doc(http_endpoint() | binary(), docid()) ->
    {ok, map()} | {error, not_found} | {error, term()}.
get_local_doc(Endpoint, DocId) ->
    #{url := BaseUrl} = E = parse_endpoint(Endpoint),
    Url = <<BaseUrl/binary, "/_local/", DocId/binary>>,
    case http_get(E, Url) of
        {ok, 200, _Headers, Body} ->
            {ok, decode_body(E, Body)};
        {ok, 404, _Headers, _Body} ->
            {error, not_found};
        {ok, Status, _Headers, Body} ->
            {error, {http_error, Status, decode_error(E, Body)}};
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%% @doc Put a local document
-spec put_local_doc(http_endpoint() | binary(), docid(), map()) -> ok | {error, term()}.
put_local_doc(Endpoint, DocId, Doc) ->
    #{url := BaseUrl} = E = parse_endpoint(Endpoint),
    Url = <<BaseUrl/binary, "/_local/", DocId/binary>>,
    case http_put(E, Url, Doc) of
        {ok, 201, _Headers, _Body} ->
            ok;
        {ok, Status, _Headers, Body} ->
            {error, {http_error, Status, decode_error(E, Body)}};
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%% @doc Delete a local document
-spec delete_local_doc(http_endpoint() | binary(), docid()) -> ok | {error, not_found} | {error, term()}.
delete_local_doc(Endpoint, DocId) ->
    #{url := BaseUrl} = E = parse_endpoint(Endpoint),
    Url = <<BaseUrl/binary, "/_local/", DocId/binary>>,
    case http_delete(E, Url) of
        {ok, 200, _Headers, _Body} ->
            ok;
        {ok, 404, _Headers, _Body} ->
            {error, not_found};
        {ok, Status, _Headers, Body} ->
            {error, {http_error, Status, decode_error(E, Body)}};
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%% @doc Get database info
-spec db_info(http_endpoint() | binary()) -> {ok, map()} | {error, term()}.
db_info(Endpoint) ->
    #{url := Url} = E = parse_endpoint(Endpoint),
    case http_get(E, Url) of
        {ok, 200, _Headers, Body} ->
            {ok, decode_body(E, Body)};
        {ok, 404, _Headers, _Body} ->
            {error, not_found};
        {ok, Status, _Headers, Body} ->
            {error, {http_error, Status, decode_error(E, Body)}};
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%% @doc Synchronize HLC with remote timestamp
-spec sync_hlc(http_endpoint() | binary(), barrel_hlc:timestamp()) ->
    {ok, barrel_hlc:timestamp()} | {error, term()}.
sync_hlc(Endpoint, LocalHlc) ->
    #{url := BaseUrl} = E = parse_endpoint(Endpoint),
    Url = <<BaseUrl/binary, "/_sync_hlc">>,
    ReqBody = #{<<"hlc">> => format_hlc(LocalHlc)},
    case http_post(E, Url, ReqBody) of
        {ok, 200, _Headers, Body} ->
            #{<<"hlc">> := RemoteHlcBin} = decode_body(E, Body),
            {ok, parse_hlc(RemoteHlcBin)};
        {ok, Status, _Headers, Body} ->
            {error, {http_error, Status, decode_error(E, Body)}};
        {error, Reason} ->
            {error, {connection_error, Reason}}
    end.

%%====================================================================
%% Internal - HTTP helpers
%%====================================================================

http_get(Endpoint, Url) ->
    Headers = request_headers(Endpoint),
    Opts = request_options(Endpoint),
    case hackney:get(Url, Headers, <<>>, Opts) of
        {ok, Status, RespHeaders, Ref} ->
            {ok, Body} = hackney:body(Ref),
            {ok, Status, RespHeaders, Body};
        {error, _} = Error ->
            Error
    end.

http_post(Endpoint, Url, ReqBody) ->
    Headers = request_headers(Endpoint),
    Body = encode_body(Endpoint, ReqBody),
    Opts = request_options(Endpoint),
    case hackney:post(Url, Headers, Body, Opts) of
        {ok, Status, RespHeaders, Ref} ->
            {ok, RespBody} = hackney:body(Ref),
            {ok, Status, RespHeaders, RespBody};
        {error, _} = Error ->
            Error
    end.

http_put(Endpoint, Url, ReqBody) ->
    Headers = request_headers(Endpoint),
    Body = encode_body(Endpoint, ReqBody),
    Opts = request_options(Endpoint),
    case hackney:put(Url, Headers, Body, Opts) of
        {ok, Status, RespHeaders, Ref} ->
            {ok, RespBody} = hackney:body(Ref),
            {ok, Status, RespHeaders, RespBody};
        {error, _} = Error ->
            Error
    end.

http_delete(Endpoint, Url) ->
    Headers = request_headers(Endpoint),
    Opts = request_options(Endpoint),
    case hackney:delete(Url, Headers, <<>>, Opts) of
        {ok, Status, RespHeaders, Ref} ->
            {ok, Body} = hackney:body(Ref),
            {ok, Status, RespHeaders, Body};
        {error, _} = Error ->
            Error
    end.

%% Build request headers including authentication
request_headers(Endpoint) ->
    ContentTypeHeaders = content_type_headers(Endpoint),
    AuthHeaders = auth_headers(Endpoint),
    ContentTypeHeaders ++ AuthHeaders.

content_type_headers(#{content_type := cbor}) ->
    [{<<"Content-Type">>, ?CT_CBOR}, {<<"Accept">>, ?CT_CBOR}];
content_type_headers(_) ->
    [{<<"Content-Type">>, ?CT_JSON}, {<<"Accept">>, ?CT_JSON}].

%% Build authentication headers
auth_headers(#{bearer_token := Token}) when is_binary(Token) ->
    [{<<"Authorization">>, <<"Bearer ", Token/binary>>}];
auth_headers(#{basic_auth := {User, Pass}}) ->
    Credentials = base64:encode(<<User/binary, ":", Pass/binary>>),
    [{<<"Authorization">>, <<"Basic ", Credentials/binary>>}];
auth_headers(_) ->
    [].

%% Build request options including SSL and timeout
request_options(Endpoint) ->
    Timeout = maps:get(timeout, Endpoint, ?DEFAULT_TIMEOUT),
    BaseOpts = [{recv_timeout, Timeout}],
    ssl_options(Endpoint, BaseOpts).

%% Add SSL options if needed
ssl_options(#{ssl_options := SslOpts}, BaseOpts) when is_list(SslOpts) ->
    [{ssl_options, SslOpts} | BaseOpts];
ssl_options(#{url := Url} = Endpoint, BaseOpts) ->
    %% Auto-detect SSL from URL scheme
    case binary:match(Url, <<"https://">>) of
        {0, _} ->
            %% HTTPS URL - add default SSL options if none provided
            DefaultSslOpts = default_ssl_options(Endpoint),
            [{ssl_options, DefaultSslOpts} | BaseOpts];
        _ ->
            BaseOpts
    end;
ssl_options(_, BaseOpts) ->
    BaseOpts.

%% Default SSL options - verify peer by default
default_ssl_options(#{insecure := true}) ->
    [{verify, verify_none}];
default_ssl_options(_) ->
    %% Secure defaults: verify peer certificate
    [{verify, verify_peer},
     {cacerts, public_key:cacerts_get()},
     {customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}].

%%====================================================================
%% Internal - Encoding/Decoding
%%====================================================================

encode_body(#{content_type := cbor}, Data) ->
    barrel_docdb_codec_cbor:encode_cbor(Data);
encode_body(_, Data) ->
    iolist_to_binary(json:encode(Data)).

decode_body(#{content_type := cbor}, Body) ->
    barrel_docdb_codec_cbor:decode_cbor(Body);
decode_body(_, Body) ->
    json:decode(Body).

decode_error(Endpoint, Body) ->
    try
        #{<<"error">> := Msg} = decode_body(Endpoint, Body),
        Msg
    catch
        _:_ -> Body
    end.

%%====================================================================
%% Internal - URL building
%%====================================================================

build_doc_url(BaseUrl, DocId, Opts) ->
    Base = <<BaseUrl/binary, "/", DocId/binary>>,
    QsParams = build_doc_qs(Opts),
    append_qs(Base, QsParams).

build_doc_qs(Opts) ->
    lists:filtermap(
        fun({rev, Rev}) -> {true, {<<"rev">>, Rev}};
           ({revs, true}) -> {true, {<<"revs">>, <<"true">>}};
           ({revs_info, true}) -> {true, {<<"revs_info">>, <<"true">>}};
           ({conflicts, true}) -> {true, {<<"conflicts">>, <<"true">>}};
           (_) -> false
        end,
        maps:to_list(Opts)
    ).

build_changes_url(BaseUrl, Since, Opts) ->
    Base = <<BaseUrl/binary, "/_changes">>,
    SinceParam = case Since of
        first -> {<<"since">>, <<"first">>};
        Hlc -> {<<"since">>, format_hlc(Hlc)}
    end,
    OptParams = lists:filtermap(
        fun({limit, N}) -> {true, {<<"limit">>, integer_to_binary(N)}};
           ({include_docs, true}) -> {true, {<<"include_docs">>, <<"true">>}};
           ({descending, true}) -> {true, {<<"descending">>, <<"true">>}};
           (_) -> false
        end,
        maps:to_list(Opts)
    ),
    append_qs(Base, [SinceParam | OptParams]).

append_qs(Url, []) ->
    Url;
append_qs(Url, Params) ->
    Qs = lists:join(<<"&">>, [<<K/binary, "=", V/binary>> || {K, V} <- Params]),
    <<Url/binary, "?", (iolist_to_binary(Qs))/binary>>.

%%====================================================================
%% Internal - HLC formatting
%%====================================================================

format_hlc(Hlc) when is_tuple(Hlc) ->
    iolist_to_binary(io_lib:format("~p", [Hlc]));
format_hlc(Hlc) ->
    Hlc.

parse_hlc(<<"first">>) ->
    first;
parse_hlc(HlcBin) when is_binary(HlcBin) ->
    try
        {ok, Tokens, _} = erl_scan:string(binary_to_list(HlcBin) ++ "."),
        {ok, Term} = erl_parse:parse_term(Tokens),
        Term
    catch
        _:_ -> first
    end.
