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
        %% Authenticate request (unless it's a health check)
        ok = maybe_authenticate(Action, Req0),
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
%% Authentication
%%====================================================================

%% @doc Check authentication for request
%% Health endpoint is always public.
%% Other endpoints require valid bearer token when API keys are configured.
maybe_authenticate(health, _Req) ->
    %% Health check is always public
    ok;
maybe_authenticate(Action, Req) when Action =:= keys; Action =:= key ->
    %% Key management requires admin authentication
    authenticate_admin(Req);
maybe_authenticate(Action, Req) ->
    %% Check if any API keys are configured
    case barrel_http_api_keys:has_any_keys() of
        false ->
            %% No keys configured - allow all requests
            ok;
        true ->
            %% Keys exist - require authentication
            %% Get database name for per-database auth (if applicable)
            DbName = get_db_from_action(Action, Req),
            authenticate(Req, DbName)
    end.

%% @doc Get database name from action and request
get_db_from_action(health, _Req) -> undefined;
get_db_from_action(_Action, Req) ->
    cowboy_req:binding(db, Req, undefined).

%% @doc Authenticate request via bearer token
authenticate(Req, DbName) ->
    case extract_bearer_token(Req) of
        undefined ->
            throw({error, 401, <<"Authorization required">>});
        Token ->
            Result = case DbName of
                undefined ->
                    %% No database context - global validation
                    barrel_http_api_keys:validate_key(Token);
                _ ->
                    %% Database-specific validation
                    barrel_http_api_keys:validate_key(Token, DbName)
            end,
            case Result of
                {ok, _KeyInfo} ->
                    ok;
                {error, invalid_key} ->
                    throw({error, 401, <<"Invalid API key">>});
                {error, access_denied} ->
                    throw({error, 403, <<"Access denied to this database">>})
            end
    end.

%% @doc Authenticate request requiring admin privileges
authenticate_admin(Req) ->
    case extract_bearer_token(Req) of
        undefined ->
            throw({error, 401, <<"Authorization required">>});
        Token ->
            case barrel_http_api_keys:validate_key(Token) of
                {ok, #{is_admin := true}} ->
                    ok;
                {ok, _} ->
                    throw({error, 403, <<"Admin access required">>});
                {error, invalid_key} ->
                    throw({error, 401, <<"Invalid API key">>})
            end
    end.

%% @doc Extract bearer token from Authorization header
extract_bearer_token(Req) ->
    case cowboy_req:header(<<"authorization">>, Req) of
        undefined ->
            undefined;
        <<"Bearer ", Token/binary>> ->
            Token;
        <<"bearer ", Token/binary>> ->
            Token;
        _ ->
            undefined
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
            %% Sanitize info for JSON/CBOR encoding (remove pid, format atom keys)
            SafeInfo = sanitize_db_info(Info),
            Body = encode_response(SafeInfo, Req),
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

%% Replication: revsdiff
handle_action(revsdiff, <<"POST">>, Req) ->
    handle_revsdiff(Req);

%% Replication: put_rev
handle_action(put_rev, <<"POST">>, Req) ->
    handle_put_rev(Req);

%% Replication: sync_hlc
handle_action(sync_hlc, <<"POST">>, Req) ->
    handle_sync_hlc(Req);

%% Local documents
handle_action(local_doc, <<"GET">>, Req) ->
    handle_get_local_doc(Req);
handle_action(local_doc, <<"PUT">>, Req) ->
    handle_put_local_doc(Req);
handle_action(local_doc, <<"DELETE">>, Req) ->
    handle_delete_local_doc(Req);

%% API Key management (admin only)
handle_action(keys, <<"GET">>, Req) ->
    handle_list_keys(Req);
handle_action(keys, <<"POST">>, Req) ->
    handle_create_key(Req);
handle_action(key, <<"GET">>, Req) ->
    handle_get_key(Req);
handle_action(key, <<"DELETE">>, Req) ->
    handle_delete_key(Req);

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
%% Replication Handlers
%%====================================================================

handle_revsdiff(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    #{<<"id">> := DocId, <<"revs">> := RevIds} = decode_request_body(ReqBody, Req1),
    case barrel_docdb:revsdiff(DbName, DocId, RevIds) of
        {ok, Missing, Ancestors} ->
            Response = #{
                <<"missing">> => Missing,
                <<"possible_ancestors">> => Ancestors
            },
            Body = encode_response(Response, Req1),
            {200, response_headers(Req1), Body, Req1};
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_put_rev(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    #{<<"doc">> := Doc, <<"history">> := History} = Body0 = decode_request_body(ReqBody, Req1),
    Deleted = maps:get(<<"deleted">>, Body0, false),
    case barrel_docdb:put_rev(DbName, Doc, History, Deleted) of
        {ok, DocId, RevId} ->
            Response = #{<<"ok">> => true, <<"id">> => DocId, <<"rev">> => RevId},
            RespBody = encode_response(Response, Req1),
            {201, response_headers(Req1), RespBody, Req1};
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_sync_hlc(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    #{<<"hlc">> := HlcBin} = decode_request_body(ReqBody, Req1),
    RemoteHlc = parse_hlc(HlcBin),
    case barrel_docdb:sync_hlc(RemoteHlc) of
        {ok, LocalHlc} ->
            Response = #{<<"hlc">> => format_hlc(LocalHlc), <<"db">> => DbName},
            Body = encode_response(Response, Req1),
            {200, response_headers(Req1), Body, Req1};
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

%%====================================================================
%% Local Document Handlers
%%====================================================================

handle_get_local_doc(Req) ->
    DbName = cowboy_req:binding(db, Req),
    DocId = cowboy_req:binding(doc_id, Req),
    case barrel_docdb:get_local_doc(DbName, DocId) of
        {ok, Doc} ->
            Body = encode_response(Doc, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Local document not found">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_put_local_doc(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    DocId = cowboy_req:binding(doc_id, Req0),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    Doc = decode_request_body(ReqBody, Req1),
    case barrel_docdb:put_local_doc(DbName, DocId, Doc) of
        ok ->
            Response = #{<<"ok">> => true, <<"id">> => DocId},
            Body = encode_response(Response, Req1),
            {201, response_headers(Req1), Body, Req1};
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_delete_local_doc(Req) ->
    DbName = cowboy_req:binding(db, Req),
    DocId = cowboy_req:binding(doc_id, Req),
    case barrel_docdb:delete_local_doc(DbName, DocId) of
        ok ->
            Response = #{<<"ok">> => true},
            Body = encode_response(Response, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Local document not found">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

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
%% API Key Management Handlers
%%====================================================================

handle_list_keys(Req) ->
    {ok, Keys} = barrel_http_api_keys:list_keys(),
    Body = encode_response(Keys, Req),
    {200, response_headers(Req), Body, Req}.

handle_create_key(Req0) ->
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    Opts0 = decode_request_body(ReqBody, Req1),
    %% Convert binary keys to atom keys for internal use
    Opts = maps:fold(
        fun(<<"name">>, V, Acc) -> Acc#{name => V};
           (<<"permissions">>, V, Acc) -> Acc#{permissions => V};
           (<<"databases">>, <<"all">>, Acc) -> Acc#{databases => all};
           (<<"databases">>, V, Acc) when is_list(V) -> Acc#{databases => V};
           (<<"is_admin">>, V, Acc) -> Acc#{is_admin => V};
           (_, _, Acc) -> Acc
        end,
        #{},
        Opts0
    ),
    case barrel_http_api_keys:create_key(Opts) of
        {ok, Key, KeyInfo} ->
            %% Return the full key only on creation
            Response = KeyInfo#{<<"key">> => Key},
            Body = encode_response(Response, Req1),
            {201, response_headers(Req1), Body, Req1};
        {error, Reason} ->
            throw({error, 400, format_error(Reason)})
    end.

handle_get_key(Req) ->
    KeyPrefix = cowboy_req:binding(key_prefix, Req),
    {ok, Keys} = barrel_http_api_keys:list_keys(),
    case lists:filter(
        fun(#{key_prefix := P}) -> P =:= KeyPrefix end,
        Keys
    ) of
        [KeyInfo] ->
            Body = encode_response(KeyInfo, Req),
            {200, response_headers(Req), Body, Req};
        [] ->
            throw({error, 404, <<"Key not found">>})
    end.

handle_delete_key(Req) ->
    KeyPrefix = cowboy_req:binding(key_prefix, Req),
    case barrel_http_api_keys:delete_key(KeyPrefix) of
        ok ->
            Body = encode_response(#{<<"ok">> => true}, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Key not found">>});
        {error, cannot_delete_last_admin_key} ->
            throw({error, 400, <<"Cannot delete the last admin key">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
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

%%====================================================================
%% Database Info Sanitization
%%====================================================================

%% @doc Sanitize database info for JSON/CBOR encoding
%% Removes pids and converts atom keys to binary
sanitize_db_info(Info) when is_map(Info) ->
    maps:fold(
        fun(pid, _V, Acc) ->
                %% Skip pid - not serializable
                Acc;
           (K, V, Acc) when is_atom(K) ->
                maps:put(atom_to_binary(K), sanitize_value(V), Acc);
           (K, V, Acc) when is_binary(K) ->
                maps:put(K, sanitize_value(V), Acc)
        end,
        #{},
        Info
    ).

sanitize_value(V) when is_pid(V) ->
    %% Convert pid to string for debugging
    iolist_to_binary(pid_to_list(V));
sanitize_value(V) when is_atom(V) ->
    atom_to_binary(V);
sanitize_value(V) when is_map(V) ->
    sanitize_db_info(V);
sanitize_value(V) when is_list(V) ->
    lists:map(fun sanitize_value/1, V);
sanitize_value(V) ->
    V.
