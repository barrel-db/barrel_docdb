%%%-------------------------------------------------------------------
%%% @doc HTTP Handler for barrel_docdb P2P replication
%%%
%%% Handles HTTP requests for document operations, changes feed,
%%% and replication endpoints.
%%%
%%% Content types supported:
%%% - application/json
%%% - application/cbor
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_http_handler).

-export([init/2]).

%% Content types
-define(CT_JSON, <<"application/json">>).
-define(CT_CBOR, <<"application/cbor">>).

%%====================================================================
%% Cowboy Handler
%%====================================================================

init(Req0, State) ->
    #{action := Action} = State,
    Method = cowboy_req:method(Req0),
    try
        {Status, Headers, Body, Req1} = handle_action(Action, Method, Req0),
        Req2 = cowboy_req:reply(Status, Headers, Body, Req1),
        {ok, Req2, State}
    catch
        throw:{error, ErrStatus, Message} ->
            ErrorBody = encode_error(Message, Req0),
            ErrHeaders = response_headers(Req0),
            ErrReq = cowboy_req:reply(ErrStatus, ErrHeaders, ErrorBody, Req0),
            {ok, ErrReq, State};
        Class:Reason:Stack ->
            logger:error("HTTP handler error: ~p:~p~n~p", [Class, Reason, Stack]),
            ErrorBody = encode_error(<<"Internal server error">>, Req0),
            ErrHeaders = response_headers(Req0),
            ErrReq = cowboy_req:reply(500, ErrHeaders, ErrorBody, Req0),
            {ok, ErrReq, State}
    end.

%%====================================================================
%% Action Handlers
%%====================================================================

%% Health check
handle_action(health, <<"GET">>, Req) ->
    Body = encode_response(#{<<"status">> => <<"ok">>}, Req),
    {200, response_headers(Req), Body, Req};

%% Database info
handle_action(db_info, <<"GET">>, Req) ->
    DbName = cowboy_req:binding(db, Req),
    case barrel_docdb:db_info(DbName) of
        {ok, Info} ->
            Body = encode_response(Info, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Database not found">>})
    end;

%% Document operations
handle_action(doc, <<"GET">>, Req) ->
    handle_get_doc(Req);
handle_action(doc, <<"PUT">>, Req) ->
    handle_put_doc(Req);
handle_action(doc, <<"DELETE">>, Req) ->
    handle_delete_doc(Req);

%% Changes feed
handle_action(changes, <<"GET">>, Req) ->
    handle_get_changes(Req);
handle_action(changes, <<"POST">>, Req) ->
    handle_get_changes(Req);

%% Bulk docs
handle_action(bulk_docs, <<"POST">>, Req) ->
    handle_bulk_docs(Req);

%% Method not allowed
handle_action(_Action, _Method, _Req) ->
    throw({error, 405, <<"Method not allowed">>}).

%%====================================================================
%% Document Handlers
%%====================================================================

handle_get_doc(Req) ->
    DbName = cowboy_req:binding(db, Req),
    DocId = cowboy_req:binding(doc_id, Req),
    Qs = cowboy_req:parse_qs(Req),
    Opts = parse_doc_opts(Qs),
    case barrel_docdb:get_doc(DbName, DocId, Opts) of
        {ok, Doc} ->
            Body = encode_doc_response(Doc, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Document not found">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_put_doc(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    DocId = cowboy_req:binding(doc_id, Req0),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    Doc0 = decode_request_body(ReqBody, Req1),
    %% Ensure doc has the ID from URL
    Doc = Doc0#{<<"id">> => DocId},
    case barrel_docdb:put_doc(DbName, Doc) of
        {ok, Result} ->
            Body = encode_response(Result, Req1),
            {201, response_headers(Req1), Body, Req1};
        {error, conflict} ->
            throw({error, 409, <<"Document update conflict">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_delete_doc(Req) ->
    DbName = cowboy_req:binding(db, Req),
    DocId = cowboy_req:binding(doc_id, Req),
    Qs = cowboy_req:parse_qs(Req),
    Rev = proplists:get_value(<<"rev">>, Qs, <<>>),
    case barrel_docdb:delete_doc(DbName, DocId, #{rev => Rev}) of
        {ok, Result} ->
            Body = encode_response(Result, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Document not found">>});
        {error, conflict} ->
            throw({error, 409, <<"Document update conflict">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

%%====================================================================
%% Changes Handler
%%====================================================================

handle_get_changes(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    {Since, Opts} = parse_changes_opts(Req0),
    case barrel_docdb:get_changes(DbName, Since, Opts) of
        {ok, Changes, LastHlc} ->
            %% Format changes for JSON serialization
            FormattedChanges = lists:map(fun format_change/1, Changes),
            Response = #{
                <<"results">> => FormattedChanges,
                <<"last_seq">> => format_hlc(LastHlc)
            },
            Body = encode_response(Response, Req0),
            {200, response_headers(Req0), Body, Req0};
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

%% Format a change for JSON serialization
format_change(Change) when is_map(Change) ->
    maps:map(
        fun(hlc, V) -> format_hlc(V);
           (<<"hlc">>, V) -> format_hlc(V);
           (_, V) -> V
        end,
        Change
    ).

%%====================================================================
%% Bulk Docs Handler
%%====================================================================

handle_bulk_docs(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    #{<<"docs">> := Docs} = decode_request_body(ReqBody, Req1),
    Results = lists:map(
        fun(Doc) ->
            case barrel_docdb:put_doc(DbName, Doc) of
                {ok, Result} ->
                    Result#{<<"ok">> => true};
                {error, Reason} ->
                    #{<<"error">> => format_error(Reason),
                      <<"id">> => maps:get(<<"id">>, Doc, null)}
            end
        end,
        Docs
    ),
    Body = encode_response(Results, Req1),
    {201, response_headers(Req1), Body, Req1}.

%%====================================================================
%% Content Negotiation
%%====================================================================

%% Determine response content type based on Accept header
response_content_type(Req) ->
    case cowboy_req:header(<<"accept">>, Req) of
        undefined ->
            ?CT_JSON;
        Accept ->
            case binary:match(Accept, <<"application/cbor">>) of
                nomatch -> ?CT_JSON;
                _ -> ?CT_CBOR
            end
    end.

%% Get request content type
request_content_type(Req) ->
    case cowboy_req:header(<<"content-type">>, Req) of
        ?CT_CBOR -> cbor;
        _ -> json
    end.

%% Response headers with content type
response_headers(Req) ->
    #{<<"content-type">> => response_content_type(Req)}.

%%====================================================================
%% Encoding/Decoding
%%====================================================================

%% Encode response based on Accept header
encode_response(Data, Req) ->
    case response_content_type(Req) of
        ?CT_JSON -> iolist_to_binary(json:encode(Data));
        ?CT_CBOR -> barrel_docdb_codec_cbor:encode_cbor(Data)
    end.

%% Encode document (handles both map and indexed binary)
encode_doc_response(Doc, Req) ->
    case response_content_type(Req) of
        ?CT_JSON -> barrel_doc:to_json(Doc);
        ?CT_CBOR -> barrel_doc:to_cbor(Doc)
    end.

%% Decode request body based on Content-Type
decode_request_body(Body, Req) ->
    case request_content_type(Req) of
        json -> json:decode(Body);
        cbor -> barrel_docdb_codec_cbor:decode_cbor(Body)
    end.

%% Encode error response
encode_error(Message, Req) ->
    encode_response(#{<<"error">> => Message}, Req).

%%====================================================================
%% Option Parsing
%%====================================================================

parse_doc_opts(Qs) ->
    lists:foldl(
        fun({<<"rev">>, Rev}, Acc) ->
                Acc#{rev => Rev};
           ({<<"revs">>, <<"true">>}, Acc) ->
                Acc#{revs => true};
           ({<<"revs_info">>, <<"true">>}, Acc) ->
                Acc#{revs_info => true};
           ({<<"conflicts">>, <<"true">>}, Acc) ->
                Acc#{conflicts => true};
           (_, Acc) ->
                Acc
        end,
        #{},
        Qs
    ).

parse_changes_opts(Req) ->
    Qs = cowboy_req:parse_qs(Req),
    Since = case proplists:get_value(<<"since">>, Qs, <<"0">>) of
        <<"0">> -> first;
        <<"first">> -> first;
        HlcBin -> parse_hlc(HlcBin)
    end,
    Opts = lists:foldl(
        fun({<<"limit">>, LimitBin}, Acc) ->
                Acc#{limit => binary_to_integer(LimitBin)};
           ({<<"include_docs">>, <<"true">>}, Acc) ->
                Acc#{include_docs => true};
           ({<<"descending">>, <<"true">>}, Acc) ->
                Acc#{descending => true};
           (_, Acc) ->
                Acc
        end,
        #{},
        Qs
    ),
    {Since, Opts}.

%%====================================================================
%% HLC Formatting
%%====================================================================

format_hlc(Hlc) when is_tuple(Hlc) ->
    %% Convert HLC timestamp to string representation
    iolist_to_binary(io_lib:format("~p", [Hlc]));
format_hlc(Hlc) ->
    Hlc.

parse_hlc(HlcBin) when is_binary(HlcBin) ->
    %% Try to parse as Erlang term
    try
        {ok, Tokens, _} = erl_scan:string(binary_to_list(HlcBin) ++ "."),
        {ok, Term} = erl_parse:parse_term(Tokens),
        Term
    catch
        _:_ -> first
    end.

%%====================================================================
%% Error Formatting
%%====================================================================

format_error(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason);
format_error(Reason) when is_binary(Reason) ->
    Reason;
format_error(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).
