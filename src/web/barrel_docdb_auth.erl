%%%-------------------------------------------------------------------
%%% @doc Authentication and authorisation middleware.
%%%
%%% Service-level middleware: runs ahead of every routed handler.
%%% Classifies the request by method + path:
%%%
%%% - `health' / `metrics' / `/.well-known/barrel'  → public, pass-through.
%%% - `/keys' / `/keys/:p' / `/admin/*'              → admin (`is_admin = true').
%%% - Anything else under `/db/...'                  → `ak_*' API key
%%%   scoped to the database (if applicable) with a per-(verb, path)
%%%   `read' / `write' permission check.
%%%
%%% On success the validated key map is attached to the request as
%%% `livery_req:set_meta(auth_ctx, _, _)' so handlers can inspect
%%% the caller.
%%%
%%% Domain logic lives in `barrel_http_api_keys' (DETS-backed key
%%% store) and `barrel_peer_auth' (replication-internal Ed25519
%%% signing); this module is the gate.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_auth).
-behaviour(livery_middleware).

-export([call/3]).

%% Run the auth pipeline. State is unused.
%%
%% This is a service-level middleware: it runs before livery's
%% router matches the request, so path bindings (`livery_req:binding/2,3')
%% are NOT yet populated. The db name (and per-(path, method)
%% permission) is parsed from the raw path here instead.
-spec call(livery_req:req(), livery_middleware:next(), term()) ->
    livery_resp:resp().
call(Req, Next, _State) ->
    try
        Class   = classify(livery_req:path(Req), livery_req:method(Req)),
        AuthCtx = authenticate(Class, Req),
        ok      = authorize(Class, AuthCtx),
        Req1    = livery_req:set_meta(auth_ctx, AuthCtx, Req),
        Next(Req1)
    catch
        throw:{error, Status, Message} ->
            error_response(Status, Message)
    end.

%%====================================================================
%% Classification
%%
%% Turn an inbound (path, method) into an access descriptor:
%%   public                - no auth required
%%   admin                 - is_admin = true required
%%   {data, read | write}  - data-plane perm required
%%====================================================================

-type class() ::
    public
  | admin
  | {data, DbName :: binary(), Perm :: binary()}.

-spec classify(binary(), binary()) -> class().
classify(<<"/health">>,             _)        -> public;
classify(<<"/metrics">>,            _)        -> public;
classify(<<"/.well-known/barrel">>, _)        -> public;
classify(<<"/openapi.json">>,       _)        -> public;
classify(<<"/docs">>,               _)        -> public;
classify(<<"/keys", _/binary>>,     _)        -> admin;
classify(<<"/admin/", _/binary>>,   _)        -> admin;
classify(<<"/db/", Rest/binary>>,   Method)   -> db_class(Rest, Method);
classify(_Path,                     _)        -> throw({error, 404, <<"Not found">>}).

%% Parse /db/<dbname><tail> into the data-class descriptor: db name +
%% per-(path tail, method) permission.
db_class(Rest, Method) ->
    {DbName, Tail} = case binary:split(Rest, <<"/">>) of
        [Db]           -> {Db, <<>>};
        [Db, Trailing] -> {Db, <<"/", Trailing/binary>>}
    end,
    {data, DbName, perm_tail(Tail, Method)}.

%% GET-ish tails read, PUT/POST/DELETE-ish tails write. The
%% replication-receiving endpoints (`_put_rev', `_revsdiff',
%% `_sync_hlc', `_replicate', `_bulk_docs') are all writes.
%% `_revsdiff' is technically a read but it sits next to the
%% writers and benefits from the same permission grouping.
perm_tail(<<>>,                     <<"GET">>) -> <<"read">>;
perm_tail(<<>>,                     _)          -> <<"write">>;
perm_tail(<<"/_find">>,             _)          -> <<"read">>;
perm_tail(<<"/_revsdiff">>,         _)          -> <<"read">>;
perm_tail(<<"/_changes">>,          _)          -> <<"read">>;
perm_tail(<<"/_changes/stream">>,   _)          -> <<"read">>;
perm_tail(<<"/_bulk_docs">>,        _)          -> <<"write">>;
perm_tail(<<"/_replicate">>,        _)          -> <<"write">>;
perm_tail(<<"/_put_rev">>,          _)          -> <<"write">>;
perm_tail(<<"/_sync_hlc">>,         _)          -> <<"write">>;
perm_tail(<<"/_local/", _/binary>>, <<"GET">>)  -> <<"read">>;
perm_tail(<<"/_local/", _/binary>>, _)          -> <<"write">>;
perm_tail(_Tail,                    <<"GET">>)  -> <<"read">>;
perm_tail(_Tail,                    _)          -> <<"write">>.

%%====================================================================
%% Authentication
%%====================================================================

authenticate(public, _Req) ->
    public;
authenticate(admin,  Req) ->
    authenticate_admin(Req),
    admin;
authenticate({data, DbName, _Perm}, Req) ->
    case barrel_http_api_keys:has_any_keys() of
        false ->
            unconfigured;
        true ->
            authenticate_bearer(Req, DbName)
    end.

authenticate_bearer(Req, DbName) ->
    case bearer_token(Req) of
        undefined ->
            throw({error, 401, <<"Authorization required">>});
        Token ->
            case validate_token(Token, DbName) of
                {ok, KeyMap} ->
                    KeyMap;
                {error, invalid_key} ->
                    throw({error, 401, <<"Invalid API key">>});
                {error, access_denied} ->
                    throw({error, 403, <<"Access denied to this database">>})
            end
    end.

authenticate_admin(Req) ->
    case bearer_token(Req) of
        undefined ->
            throw({error, 401, <<"Authorization required">>});
        Token ->
            case validate_token_admin(Token) of
                {ok, #{is_admin := true}} ->
                    ok;
                {ok, _} ->
                    throw({error, 403, <<"Admin access required">>});
                {error, invalid_key} ->
                    throw({error, 401, <<"Invalid API key">>})
            end
    end.

validate_token(<<"ak_", _/binary>> = Token, DbName) when is_binary(DbName) ->
    barrel_http_api_keys:validate_key(Token, DbName);
validate_token(_, _) ->
    {error, invalid_key}.

validate_token_admin(<<"ak_", _/binary>> = Token) ->
    barrel_http_api_keys:validate_key(Token);
validate_token_admin(_) ->
    {error, invalid_key}.

bearer_token(Req) ->
    case livery_req:header(<<"authorization">>, Req) of
        undefined                  -> undefined;
        <<"Bearer ", Token/binary>> -> Token;
        <<"bearer ", Token/binary>> -> Token;
        _                          -> undefined
    end.

%%====================================================================
%% Authorization
%%====================================================================

authorize(public,                  _AuthCtx)               -> ok;
authorize(admin,                   _AuthCtx)               -> ok;
authorize({data, _Db, _Perm},       public)                -> ok;
authorize({data, _Db, _Perm},       unconfigured)          -> ok;
authorize({data, _Db, _Perm},      #{is_admin := true})    -> ok;
authorize({data, _Db, Required},   #{permissions := Perms}) ->
    case lists:member(Required, Perms) of
        true ->
            ok;
        false ->
            throw({error, 403,
                   <<"Permission denied: requires ", Required/binary>>})
    end;
authorize({data, _Db, _Perm},      _Other) ->
    throw({error, 403, <<"Permission denied">>}).

%%====================================================================
%% Error responses
%%====================================================================

error_response(Status, Message) ->
    Body = iolist_to_binary(json:encode(#{<<"error">> => Message})),
    livery_resp:new(
        Status,
        [{<<"content-type">>, <<"application/json">>}],
        {full, Body}).
