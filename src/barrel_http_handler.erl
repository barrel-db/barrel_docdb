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
        case handle_action(Action, Method, Req0) of
            {Status, Headers, Body, Req1} ->
                %% Normal response
                Req2 = cowboy_req:reply(Status, Headers, Body, Req1),
                {ok, Req2, State};
            {stream, Status, Headers, StreamFun, Req1} ->
                %% Streaming response - StreamFun sends the body
                Req2 = cowboy_req:stream_reply(Status, Headers, Req1),
                StreamFun(Req2),
                {ok, Req2, State}
        end
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
maybe_authenticate(metrics, _Req) ->
    %% Metrics endpoint is always public (for Prometheus scraping)
    ok;
maybe_authenticate(node_info, _Req) ->
    %% Node info is public (for discovery)
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

%% Health check - returns detailed system status
handle_action(health, <<"GET">>, Req) ->
    %% Collect health information
    Health = collect_health_info(),
    Body = encode_response(Health, Req),
    %% Return 503 if unhealthy, 200 otherwise
    Status = case maps:get(<<"status">>, Health) of
        <<"ok">> -> 200;
        _ -> 503
    end,
    {Status, response_headers(Req), Body, Req};

%% Prometheus metrics endpoint
handle_action(metrics, <<"GET">>, Req) ->
    MetricsText = barrel_metrics:export_text(),
    Headers = #{<<"content-type">> => <<"text/plain; version=0.0.4; charset=utf-8">>},
    {200, Headers, MetricsText, Req};

%% Node info (discovery endpoint)
handle_action(node_info, <<"GET">>, Req) ->
    case barrel_discovery:node_info() of
        {ok, Info} ->
            Body = encode_response(Info, Req),
            {200, response_headers(Req), Body, Req};
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end;

%% Peer management: list all peers
handle_action(peers, <<"GET">>, Req) ->
    {ok, Peers} = barrel_discovery:list_peers(),
    Body = encode_response(#{<<"peers">> => Peers}, Req),
    {200, response_headers(Req), Body, Req};

%% Peer management: add peer
handle_action(peers, <<"POST">>, Req) ->
    handle_add_peer(Req);

%% Peer management: get/delete peer
handle_action(peer, <<"GET">>, Req) ->
    PeerUrlEncoded = cowboy_req:binding(peer_url, Req),
    PeerUrl = uri_string:unquote(PeerUrlEncoded),
    case barrel_discovery:get_peer(PeerUrl) of
        {ok, PeerInfo} ->
            Body = encode_response(PeerInfo, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Peer not found">>})
    end;

handle_action(peer, <<"DELETE">>, Req) ->
    PeerUrlEncoded = cowboy_req:binding(peer_url, Req),
    PeerUrl = uri_string:unquote(PeerUrlEncoded),
    ok = barrel_discovery:remove_peer(PeerUrl),
    Body = encode_response(#{<<"ok">> => true}, Req),
    {200, response_headers(Req), Body, Req};

%% Federation: list all
handle_action(federations, <<"GET">>, Req) ->
    {ok, Feds} = barrel_federation:list(),
    Body = encode_response(#{<<"federations">> => Feds}, Req),
    {200, response_headers(Req), Body, Req};

%% Federation: create
handle_action(federations, <<"POST">>, Req) ->
    handle_create_federation(Req);

%% Federation: get/delete
handle_action(federation, <<"GET">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    case barrel_federation:get(Name) of
        {ok, Fed} ->
            Body = encode_response(Fed, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Federation not found">>})
    end;
handle_action(federation, <<"DELETE">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    case barrel_federation:delete(Name) of
        ok ->
            Body = encode_response(#{<<"ok">> => true}, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Federation not found">>})
    end;
handle_action(federation, <<"PUT">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req),
    Spec = decode_request_body(ReqBody, Req1),
    %% Update query if provided
    case maps:get(<<"query">>, Spec, undefined) of
        undefined ->
            throw({error, 400, <<"No query provided">>});
        QuerySpec ->
            Query = convert_query_spec(QuerySpec),
            case barrel_federation:set_query(Name, Query) of
                ok ->
                    Body = encode_response(#{<<"ok">> => true}, Req1),
                    {200, response_headers(Req1), Body, Req1};
                {error, not_found} ->
                    throw({error, 404, <<"Federation not found">>})
            end
    end;

%% Federation: add/remove member
handle_action(federation_member, <<"PUT">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    Member = cowboy_req:binding(member, Req),
    case barrel_federation:add_member(Name, Member) of
        ok ->
            Body = encode_response(#{<<"ok">> => true}, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Federation not found">>});
        {error, {member_not_found, _}} ->
            throw({error, 400, <<"Member database not found">>})
    end;
handle_action(federation_member, <<"DELETE">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    Member = cowboy_req:binding(member, Req),
    case barrel_federation:remove_member(Name, Member) of
        ok ->
            Body = encode_response(#{<<"ok">> => true}, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Federation not found">>})
    end;

%% Federation: query
handle_action(federation_find, <<"GET">>, Req) ->
    %% GET uses stored query (find/1)
    Name = cowboy_req:binding(name, Req),
    case barrel_federation:find(Name) of
        {ok, Results, Meta} ->
            FormattedMeta = format_federation_meta(Meta),
            Response = #{<<"results">> => Results, <<"meta">> => FormattedMeta},
            Body = encode_response(Response, Req),
            {200, response_headers(Req), Body, Req};
        {error, {federation_not_found, _}} ->
            throw({error, 404, <<"Federation not found">>});
        {error, Reason} ->
            throw({error, 400, format_error(Reason)})
    end;
handle_action(federation_find, <<"POST">>, Req) ->
    handle_federation_find(Req);

%% Replication policies: list all
handle_action(policies, <<"GET">>, Req) ->
    {ok, Policies} = barrel_rep_policy:list(),
    %% Convert policies to JSON-safe format
    SafePolicies = [policy_to_json(P) || P <- Policies],
    Body = encode_response(#{<<"policies">> => SafePolicies}, Req),
    {200, response_headers(Req), Body, Req};

%% Replication policies: create
handle_action(policies, <<"POST">>, Req) ->
    handle_create_policy(Req);

%% Replication policies: get
handle_action(policy, <<"GET">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    case barrel_rep_policy:get(Name) of
        {ok, Policy} ->
            Body = encode_response(policy_to_json(Policy), Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Policy not found">>})
    end;

%% Replication policies: update
handle_action(policy, <<"PUT">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req),
    Spec = decode_request_body(ReqBody, Req1),
    %% Delete and recreate with new config
    case barrel_rep_policy:get(Name) of
        {ok, OldPolicy} ->
            WasEnabled = maps:get(enabled, OldPolicy, false),
            ok = barrel_rep_policy:delete(Name),
            Config = json_to_policy_config(Spec),
            case barrel_rep_policy:create(Name, Config) of
                ok ->
                    %% Re-enable if it was enabled
                    case WasEnabled of
                        true -> barrel_rep_policy:enable(Name);
                        false -> ok
                    end,
                    Body = encode_response(#{<<"ok">> => true}, Req1),
                    {200, response_headers(Req1), Body, Req1};
                {error, Reason} ->
                    throw({error, 400, format_error(Reason)})
            end;
        {error, not_found} ->
            throw({error, 404, <<"Policy not found">>})
    end;

%% Replication policies: delete
handle_action(policy, <<"DELETE">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    case barrel_rep_policy:delete(Name) of
        ok ->
            Body = encode_response(#{<<"ok">> => true}, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Policy not found">>})
    end;

%% Replication policies: enable
handle_action(policy_enable, <<"POST">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    case barrel_rep_policy:enable(Name) of
        ok ->
            Body = encode_response(#{<<"ok">> => true}, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Policy not found">>});
        {error, Reason} ->
            throw({error, 400, format_error(Reason)})
    end;

%% Replication policies: disable
handle_action(policy_disable, <<"POST">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    case barrel_rep_policy:disable(Name) of
        ok ->
            Body = encode_response(#{<<"ok">> => true}, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Policy not found">>});
        {error, Reason} ->
            throw({error, 400, format_error(Reason)})
    end;

%% Replication policies: get status
handle_action(policy_status, <<"GET">>, Req) ->
    Name = cowboy_req:binding(name, Req),
    case barrel_rep_policy:status(Name) of
        {ok, Status} ->
            Body = encode_response(policy_status_to_json(Status), Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Policy not found">>})
    end;

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

%% Query
handle_action(find, <<"POST">>, Req) ->
    handle_find(Req);

%% Materialized Views
handle_action(views, <<"GET">>, Req) ->
    handle_list_views(Req);
handle_action(views, <<"POST">>, Req) ->
    handle_create_view(Req);
handle_action(view, <<"GET">>, Req) ->
    handle_get_view(Req);
handle_action(view, <<"DELETE">>, Req) ->
    handle_delete_view(Req);
handle_action(view_query, <<"GET">>, Req) ->
    handle_query_view(Req);
handle_action(view_query, <<"POST">>, Req) ->
    handle_query_view(Req);
handle_action(view_refresh, <<"POST">>, Req) ->
    handle_refresh_view(Req);

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

%% Attachments
handle_action(attachments, <<"GET">>, Req) ->
    handle_list_attachments(Req);
handle_action(attachment, <<"GET">>, Req) ->
    handle_get_attachment(Req);
handle_action(attachment, <<"PUT">>, Req) ->
    handle_put_attachment(Req);
handle_action(attachment, <<"DELETE">>, Req) ->
    handle_delete_attachment(Req);

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
    Opts0 = parse_doc_opts(Qs),
    %% For CBOR responses, request raw body for zero-copy
    {Opts, IsCbor} = case response_content_type(Req) of
        ?CT_CBOR -> {Opts0#{raw_body => true}, true};
        ?CT_JSON -> {Opts0, false}
    end,
    case barrel_docdb:get_doc(DbName, DocId, Opts) of
        {ok, CborBin, Meta} when IsCbor ->
            %% Zero-copy CBOR response: embed raw body with metadata
            Body = encode_doc_with_raw_body(CborBin, Meta),
            {200, response_headers(Req), Body, Req};
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
    {Since, Feed, FilterPattern, Opts} = parse_changes_opts(Req0),
    Timeout = maps:get(timeout, Opts, 60000),

    %% Create filter function if pattern provided
    FilterFun = create_filter_fun(FilterPattern),

    case Feed of
        longpoll ->
            handle_longpoll_changes(DbName, Since, FilterFun, Opts, Timeout, Req0);
        normal ->
            handle_normal_changes(DbName, Since, FilterFun, Opts, Req0)
    end.

%% Normal poll - return changes immediately
handle_normal_changes(DbName, Since, FilterFun, Opts, Req0) ->
    case barrel_docdb:get_changes(DbName, Since, Opts) of
        {ok, Changes, LastHlc} ->
            FilteredChanges = filter_changes(Changes, FilterFun),
            FormattedChanges = lists:map(fun format_change/1, FilteredChanges),
            Response = #{
                <<"results">> => FormattedChanges,
                <<"last_seq">> => format_hlc(LastHlc)
            },
            Body = encode_response(Response, Req0),
            {200, response_headers(Req0), Body, Req0};
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

%% Long poll - wait for changes or timeout
handle_longpoll_changes(DbName, Since, FilterFun, Opts, Timeout, Req0) ->
    StartTime = erlang:monotonic_time(millisecond),
    longpoll_loop(DbName, Since, FilterFun, Opts, Timeout, StartTime, Req0).

longpoll_loop(DbName, Since, FilterFun, Opts, Timeout, StartTime, Req0) ->
    Elapsed = erlang:monotonic_time(millisecond) - StartTime,
    Remaining = Timeout - Elapsed,

    case Remaining =< 0 of
        true ->
            %% Timeout - return empty results
            Response = #{
                <<"results">> => [],
                <<"last_seq">> => format_hlc(Since)
            },
            Body = encode_response(Response, Req0),
            {200, response_headers(Req0), Body, Req0};

        false ->
            case barrel_docdb:get_changes(DbName, Since, Opts) of
                {ok, [], _LastHlc} ->
                    %% No changes yet - wait and retry
                    timer:sleep(100),
                    longpoll_loop(DbName, Since, FilterFun, Opts, Timeout, StartTime, Req0);

                {ok, Changes, LastHlc} ->
                    FilteredChanges = filter_changes(Changes, FilterFun),
                    case FilteredChanges of
                        [] ->
                            %% Changes exist but filtered out - continue waiting
                            timer:sleep(100),
                            longpoll_loop(DbName, LastHlc, FilterFun, Opts, Timeout, StartTime, Req0);
                        _ ->
                            %% Have matching changes - return them
                            FormattedChanges = lists:map(fun format_change/1, FilteredChanges),
                            Response = #{
                                <<"results">> => FormattedChanges,
                                <<"last_seq">> => format_hlc(LastHlc)
                            },
                            Body = encode_response(Response, Req0),
                            {200, response_headers(Req0), Body, Req0}
                    end;

                {error, Reason} ->
                    throw({error, 500, format_error(Reason)})
            end
    end.

%% Create filter function from MQTT-style pattern
%% Supports:
%%   + : matches one segment
%%   # : matches zero or more segments (must be last)
create_filter_fun(undefined) -> undefined;
create_filter_fun(Pattern) when is_binary(Pattern) ->
    case match_trie:validate({filter, Pattern}) of
        true ->
            %% Pre-compute pattern words for efficiency
            PatternWords = pattern_to_words(Pattern),
            fun(Change) ->
                DocId = maps:get(<<"id">>, Change, maps:get(id, Change, <<>>)),
                DocWords = binary:split(DocId, <<"/">>, [global]),
                match_mqtt_pattern(PatternWords, DocWords)
            end;
        false ->
            undefined
    end.

%% Convert pattern to words, converting + and # to atoms
pattern_to_words(Pattern) ->
    [pattern_word(W) || W <- binary:split(Pattern, <<"/">>, [global])].

pattern_word(<<"+">>) -> '+';
pattern_word(<<"#">>) -> '#';
pattern_word(W) -> W.

%% Match document path against MQTT pattern words
match_mqtt_pattern([], []) -> true;
match_mqtt_pattern(['#'], _) -> true;  %% # at end matches anything
match_mqtt_pattern(['+' | PRest], [_ | DRest]) ->
    match_mqtt_pattern(PRest, DRest);
match_mqtt_pattern([P | PRest], [P | DRest]) ->
    match_mqtt_pattern(PRest, DRest);
match_mqtt_pattern(_, _) -> false.

%% Filter changes using filter function
filter_changes(Changes, undefined) -> Changes;
filter_changes(Changes, FilterFun) ->
    lists:filter(FilterFun, Changes).

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
%% Peer Discovery Handlers
%%====================================================================

handle_add_peer(Req0) ->
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    Spec = decode_request_body(ReqBody, Req1),
    Url = maps:get(<<"url">>, Spec),
    case barrel_discovery:add_peer(Url) of
        ok ->
            Response = #{<<"ok">> => true, <<"url">> => Url},
            Body = encode_response(Response, Req1),
            {201, response_headers(Req1), Body, Req1};
        {error, {invalid_remote_url, _}} ->
            throw({error, 400, <<"Invalid peer URL">>});
        {error, Reason} ->
            throw({error, 400, format_error(Reason)})
    end.

%%====================================================================
%% Federation Handlers
%%====================================================================

handle_create_federation(Req0) ->
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    Spec = decode_request_body(ReqBody, Req1),
    Name = maps:get(<<"name">>, Spec),
    Members = maps:get(<<"members">>, Spec, []),
    Options0 = maps:get(<<"options">>, Spec, #{}),
    %% Convert query spec if present
    Options = case maps:get(<<"query">>, Spec, undefined) of
        undefined -> Options0;
        QuerySpec -> Options0#{query => convert_query_spec(QuerySpec)}
    end,
    case barrel_federation:create(Name, Members, Options) of
        ok ->
            Response = #{<<"ok">> => true, <<"name">> => Name},
            Body = encode_response(Response, Req1),
            {201, response_headers(Req1), Body, Req1};
        {error, {member_not_found, Member}} ->
            throw({error, 400, <<"Member not found: ", Member/binary>>});
        {error, Reason} ->
            throw({error, 400, format_error(Reason)})
    end.

handle_federation_find(Req0) ->
    Name = cowboy_req:binding(name, Req0),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    QuerySpec0 = decode_request_body(ReqBody, Req1),
    QuerySpec = convert_query_spec(QuerySpec0),
    Opts = maps:get(<<"options">>, QuerySpec0, #{}),
    case barrel_federation:find(Name, QuerySpec, Opts) of
        {ok, Results, Meta} ->
            FormattedMeta = format_federation_meta(Meta),
            case response_content_type(Req1) of
                ?CT_CBOR ->
                    ConvertedResults = [doc_to_map(Doc) || Doc <- Results],
                    Response = #{
                        <<"results">> => ConvertedResults,
                        <<"meta">> => FormattedMeta
                    },
                    Body = barrel_docdb_codec_cbor:encode_cbor(Response),
                    {200, response_headers(Req1), Body, Req1};
                ?CT_JSON ->
                    JsonDocsStr = iolist_to_binary([
                        <<"[">>,
                        lists:join(<<",">>, [doc_to_json(Doc) || Doc <- Results]),
                        <<"]">>
                    ]),
                    JsonMetaStr = iolist_to_binary(json:encode(FormattedMeta)),
                    Body = iolist_to_binary([
                        <<"{\"results\":">>, JsonDocsStr,
                        <<",\"meta\":">>, JsonMetaStr,
                        <<"}">>
                    ]),
                    {200, response_headers(Req1), Body, Req1}
            end;
        {error, {federation_not_found, _}} ->
            throw({error, 404, <<"Federation not found">>});
        {error, Reason} ->
            throw({error, 400, format_error(Reason)})
    end.

%% Format federation query metadata for JSON/CBOR
format_federation_meta(Meta) ->
    #{
        <<"federation">> => maps:get(federation, Meta),
        <<"members_queried">> => maps:get(members_queried, Meta),
        <<"source_counts">> => maps:get(source_counts, Meta),
        <<"total_results">> => maps:get(total_results, Meta)
    }.

%%====================================================================
%% Replication Policy Handlers
%%====================================================================

handle_create_policy(Req0) ->
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    Spec = decode_request_body(ReqBody, Req1),
    Name = maps:get(<<"name">>, Spec),
    Config = json_to_policy_config(Spec),
    case barrel_rep_policy:create(Name, Config) of
        ok ->
            Response = #{<<"ok">> => true, <<"name">> => Name},
            Body = encode_response(Response, Req1),
            {201, response_headers(Req1), Body, Req1};
        {error, already_exists} ->
            throw({error, 409, <<"Policy already exists">>});
        {error, {invalid_config, Reason}} ->
            throw({error, 400, format_error({invalid_config, Reason})});
        {error, {unknown_pattern, Pattern}} ->
            throw({error, 400, <<"Unknown pattern: ", (atom_to_binary(Pattern))/binary>>});
        {error, Reason} ->
            throw({error, 400, format_error(Reason)})
    end.

%% Convert JSON policy spec to Erlang config
json_to_policy_config(Spec) ->
    Config0 = #{},
    Config1 = case maps:get(<<"pattern">>, Spec, undefined) of
        undefined -> Config0;
        PatternBin -> Config0#{pattern => binary_to_atom(PatternBin)}
    end,
    Config2 = case maps:get(<<"mode">>, Spec, undefined) of
        undefined -> Config1;
        ModeBin -> Config1#{mode => binary_to_atom(ModeBin)}
    end,
    Config3 = case maps:get(<<"enabled">>, Spec, undefined) of
        undefined -> Config2;
        Enabled -> Config2#{enabled => Enabled}
    end,
    %% Chain pattern options
    Config4 = case maps:get(<<"nodes">>, Spec, undefined) of
        undefined -> Config3;
        Nodes -> Config3#{nodes => Nodes}
    end,
    Config5 = case maps:get(<<"database">>, Spec, undefined) of
        undefined -> Config4;
        Database -> Config4#{database => Database}
    end,
    %% Group pattern options
    Config6 = case maps:get(<<"members">>, Spec, undefined) of
        undefined -> Config5;
        Members -> Config5#{members => Members}
    end,
    %% Fanout pattern options
    Config7 = case maps:get(<<"source">>, Spec, undefined) of
        undefined -> Config6;
        Source -> Config6#{source => Source}
    end,
    Config8 = case maps:get(<<"targets">>, Spec, undefined) of
        undefined -> Config7;
        Targets -> Config7#{targets => Targets}
    end,
    %% Tiered pattern options
    Config9 = case maps:get(<<"hot_db">>, Spec, undefined) of
        undefined -> Config8;
        HotDb -> Config8#{hot_db => HotDb}
    end,
    Config10 = case maps:get(<<"warm_db">>, Spec, undefined) of
        undefined -> Config9;
        WarmDb -> Config9#{warm_db => WarmDb}
    end,
    Config11 = case maps:get(<<"cold_db">>, Spec, undefined) of
        undefined -> Config10;
        ColdDb -> Config10#{cold_db => ColdDb}
    end,
    %% Filter
    case maps:get(<<"filter">>, Spec, undefined) of
        undefined -> Config11;
        Filter -> Config11#{filter => Filter}
    end.

%% Convert Erlang policy to JSON-safe map
policy_to_json(Policy) ->
    maps:fold(
        fun(pattern, V, Acc) -> Acc#{<<"pattern">> => atom_to_binary(V)};
           (mode, V, Acc) -> Acc#{<<"mode">> => atom_to_binary(V)};
           (enabled, V, Acc) -> Acc#{<<"enabled">> => V};
           (name, V, Acc) -> Acc#{<<"name">> => V};
           (nodes, V, Acc) -> Acc#{<<"nodes">> => V};
           (database, V, Acc) -> Acc#{<<"database">> => V};
           (members, V, Acc) -> Acc#{<<"members">> => V};
           (source, V, Acc) -> Acc#{<<"source">> => V};
           (targets, V, Acc) -> Acc#{<<"targets">> => V};
           (hot_db, V, Acc) -> Acc#{<<"hot_db">> => V};
           (warm_db, V, Acc) -> Acc#{<<"warm_db">> => V};
           (cold_db, V, Acc) -> Acc#{<<"cold_db">> => V};
           (filter, V, Acc) -> Acc#{<<"filter">> => V};
           (_, _, Acc) -> Acc
        end,
        #{},
        Policy
    ).

%% Convert policy status to JSON-safe map
policy_status_to_json(Status) ->
    #{
        <<"name">> => maps:get(name, Status),
        <<"pattern">> => atom_to_binary(maps:get(pattern, Status)),
        <<"enabled">> => maps:get(enabled, Status),
        <<"task_count">> => maps:get(task_count, Status),
        <<"tasks">> => [task_to_json(T) || T <- maps:get(tasks, Status, [])]
    }.

%% Convert task info to JSON
task_to_json(Task) when is_map(Task) ->
    maps:fold(
        fun(status, V, Acc) when is_atom(V) -> Acc#{<<"status">> => atom_to_binary(V)};
           (task_id, V, Acc) -> Acc#{<<"task_id">> => V};
           (pid, V, Acc) when is_pid(V) -> Acc#{<<"pid">> => list_to_binary(pid_to_list(V))};
           (K, V, Acc) when is_atom(K) -> Acc#{atom_to_binary(K) => V};
           (K, V, Acc) -> Acc#{K => V}
        end,
        #{},
        Task
    );
task_to_json(_) ->
    #{<<"status">> => <<"unknown">>}.

%%====================================================================
%% Query Handler
%%====================================================================

handle_find(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    QuerySpec0 = decode_request_body(ReqBody, Req1),
    %% Convert binary keys to atoms for internal API
    QuerySpec1 = convert_query_spec(QuerySpec0),
    %% For CBOR responses with include_docs, use doc_format => binary for efficiency
    IsCbor = response_content_type(Req1) =:= ?CT_CBOR,
    IncludeDocs = maps:get(include_docs, QuerySpec1, false),
    QuerySpec = case IsCbor andalso IncludeDocs of
        true -> QuerySpec1#{doc_format => binary};
        false -> QuerySpec1
    end,
    case barrel_docdb:find(DbName, QuerySpec) of
        {ok, Results, Meta} ->
            %% Format metadata for JSON/CBOR
            FormattedMeta = format_query_meta(Meta),
            %% Convert documents based on response type
            case IsCbor of
                true ->
                    %% For CBOR with doc_format=binary, results are raw CBOR binaries
                    %% Use encode_cbor_with_raw_docs to embed them directly
                    Body = encode_cbor_with_raw_docs(Results, FormattedMeta),
                    {200, response_headers(Req1), Body, Req1};
                false ->
                    %% For JSON, encode each document properly
                    JsonDocsStr = iolist_to_binary([
                        <<"[">>,
                        lists:join(<<",">>, [doc_to_json(Doc) || Doc <- Results]),
                        <<"]">>
                    ]),
                    JsonMetaStr = iolist_to_binary(json:encode(FormattedMeta)),
                    Body = iolist_to_binary([
                        <<"{\"results\":">>, JsonDocsStr,
                        <<",\"meta\":">>, JsonMetaStr,
                        <<"}">>
                    ]),
                    {200, response_headers(Req1), Body, Req1}
            end;
        {error, Reason} ->
            throw({error, 400, format_error(Reason)})
    end.

%% Convert document to JSON, handling both maps and indexed binary format
doc_to_json(Doc) when is_map(Doc) ->
    iolist_to_binary(json:encode(Doc));
doc_to_json(Doc) when is_binary(Doc) ->
    barrel_doc:to_json(Doc).

%% Convert document to map for CBOR encoding
doc_to_map(Doc) when is_map(Doc) -> Doc;
doc_to_map(Doc) when is_binary(Doc) ->
    barrel_doc:to_map(Doc).

%%====================================================================
%% Materialized Views Handlers
%%====================================================================

handle_list_views(Req) ->
    DbName = cowboy_req:binding(db, Req),
    case barrel_docdb:list_views(DbName) of
        {ok, Views} ->
            %% Convert view info to JSON-safe format
            JsonViews = [format_view_info(V) || V <- Views],
            Body = encode_response(JsonViews, Req),
            {200, response_headers(Req), Body, Req};
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_create_view(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    {ok, ReqBody, Req1} = cowboy_req:read_body(Req0),
    ViewSpec = decode_request_body(ReqBody, Req1),
    ViewId = maps:get(<<"id">>, ViewSpec),
    %% Convert binary keys to internal format and include id
    Config0 = convert_view_config(ViewSpec),
    Config = Config0#{id => ViewId},
    case barrel_docdb:register_view(DbName, ViewId, Config) of
        ok ->
            Response = #{<<"ok">> => true, <<"id">> => ViewId},
            Body = encode_response(Response, Req1),
            {201, response_headers(Req1), Body, Req1};
        {error, Reason} ->
            throw({error, 400, format_error(Reason)})
    end.

handle_get_view(Req) ->
    DbName = cowboy_req:binding(db, Req),
    ViewId = cowboy_req:binding(view_id, Req),
    case barrel_docdb:list_views(DbName) of
        {ok, Views} ->
            case lists:filter(fun(V) -> maps:get(id, V) =:= ViewId end, Views) of
                [ViewInfo] ->
                    Body = encode_response(format_view_info(ViewInfo), Req),
                    {200, response_headers(Req), Body, Req};
                [] ->
                    throw({error, 404, <<"View not found">>})
            end;
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_delete_view(Req) ->
    DbName = cowboy_req:binding(db, Req),
    ViewId = cowboy_req:binding(view_id, Req),
    case barrel_docdb:unregister_view(DbName, ViewId) of
        ok ->
            Response = #{<<"ok">> => true},
            Body = encode_response(Response, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"View not found">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_query_view(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    ViewId = cowboy_req:binding(view_id, Req0),
    Method = cowboy_req:method(Req0),
    {Opts, Req1} = case Method of
        <<"GET">> ->
            Qs = cowboy_req:parse_qs(Req0),
            {parse_view_query_opts(Qs), Req0};
        <<"POST">> ->
            {ok, ReqBody, R1} = cowboy_req:read_body(Req0),
            BodyMap = decode_request_body(ReqBody, R1),
            {convert_view_query_opts(BodyMap), R1}
    end,
    case barrel_docdb:query_view(DbName, ViewId, Opts) of
        {ok, Results} ->
            RespBody = encode_response(Results, Req1),
            {200, response_headers(Req1), RespBody, Req1};
        {error, not_found} ->
            throw({error, 404, <<"View not found">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_refresh_view(Req) ->
    DbName = cowboy_req:binding(db, Req),
    ViewId = cowboy_req:binding(view_id, Req),
    case barrel_docdb:refresh_view(DbName, ViewId) of
        {ok, Hlc} ->
            Response = #{<<"ok">> => true, <<"hlc">> => format_seq(Hlc)},
            Body = encode_response(Response, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"View not found">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

%% Format view info for JSON response
format_view_info(Info) when is_map(Info) ->
    maps:fold(
        fun(K, V, Acc) when is_atom(K) ->
                Acc#{atom_to_binary(K) => format_view_value(V)};
           (K, V, Acc) ->
                Acc#{K => format_view_value(V)}
        end,
        #{},
        Info
    ).

format_view_value(V) when is_atom(V) -> atom_to_binary(V);
format_view_value(V) when is_pid(V) -> iolist_to_binary(pid_to_list(V));
format_view_value(V) when is_map(V) -> format_view_info(V);
format_view_value(V) when is_list(V) -> [format_view_value(E) || E <- V];
format_view_value(V) -> V.

%% Convert view config from JSON to internal format
%% Views expect either #{module => Mod} or #{query => QuerySpec}
convert_view_config(Spec) ->
    %% Build the query spec from where clause
    QuerySpec = case maps:get(<<"where">>, Spec, undefined) of
        undefined -> #{};
        Where -> #{where => convert_where_clauses(Where)}
    end,
    %% Add key if present
    QuerySpec2 = case maps:get(<<"key">>, Spec, undefined) of
        undefined -> QuerySpec;
        Key when is_list(Key) -> QuerySpec#{key => Key}
    end,
    %% Build config with query spec
    BaseConfig = #{query => QuerySpec2},
    %% Add optional refresh mode
    Config = case maps:get(<<"refresh">>, Spec, undefined) of
        <<"on_change">> -> BaseConfig#{refresh => on_change};
        <<"manual">> -> BaseConfig#{refresh => manual};
        _ -> BaseConfig
    end,
    Config.

%% Parse view query options from query string
parse_view_query_opts(Qs) ->
    lists:foldl(
        fun({<<"key">>, Key}, Acc) ->
                Acc#{key => decode_view_key(Key)};
           ({<<"start_key">>, Key}, Acc) ->
                Acc#{start_key => decode_view_key(Key)};
           ({<<"end_key">>, Key}, Acc) ->
                Acc#{end_key => decode_view_key(Key)};
           ({<<"limit">>, LimitBin}, Acc) ->
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
    ).

%% Convert view query options from JSON body
convert_view_query_opts(Body) ->
    maps:fold(
        fun(<<"key">>, V, Acc) ->
                Acc#{key => V};
           (<<"start_key">>, V, Acc) ->
                Acc#{start_key => V};
           (<<"end_key">>, V, Acc) ->
                Acc#{end_key => V};
           (<<"limit">>, V, Acc) when is_integer(V) ->
                Acc#{limit => V};
           (<<"include_docs">>, V, Acc) when is_boolean(V) ->
                Acc#{include_docs => V};
           (<<"descending">>, V, Acc) when is_boolean(V) ->
                Acc#{descending => V};
           (_, _, Acc) ->
                Acc
        end,
        #{},
        Body
    ).

%% Decode view key from query string (JSON encoded)
decode_view_key(KeyBin) ->
    try
        json:decode(KeyBin)
    catch
        _:_ -> KeyBin
    end.

%% Format query result metadata for JSON encoding
format_query_meta(Meta) ->
    maps:fold(
        fun(last_seq, V, Acc) -> Acc#{<<"last_seq">> => format_seq(V)};
           (has_more, V, Acc) -> Acc#{<<"has_more">> => V};
           (continuation, V, Acc) -> Acc#{<<"continuation">> => V};
           (K, V, Acc) when is_atom(K) -> Acc#{atom_to_binary(K) => format_meta_value(V)};
           (K, V, Acc) -> Acc#{K => format_meta_value(V)}
        end,
        #{},
        Meta
    ).

%% Format sequence number for JSON - convert to base64 string
%% The sequence is an opaque CBOR-encoded binary from barrel_hlc:encode/1
format_seq(Seq) when is_binary(Seq) ->
    %% Convert binary to base64 for safe JSON transport
    base64:encode(Seq);
format_seq({Epoch, Counter}) when is_integer(Epoch), is_integer(Counter) ->
    iolist_to_binary(io_lib:format("~p:~p", [Epoch, Counter]));
format_seq(Seq) when is_tuple(Seq) ->
    %% Handle other tuple formats
    iolist_to_binary(io_lib:format("~p", [Seq]));
format_seq(Other) ->
    Other.

%% Format meta values that may contain non-JSON-serializable data
format_meta_value(V) when is_tuple(V) ->
    iolist_to_binary(io_lib:format("~p", [V]));
format_meta_value(V) when is_atom(V) ->
    atom_to_binary(V);
format_meta_value(V) ->
    V.

%% Convert query spec binary keys to internal format
convert_query_spec(Spec) when is_map(Spec) ->
    BaseSpec = maps:fold(
        fun(<<"where">>, V, Acc) ->
                Acc#{where => convert_where_clauses(V)};
           (<<"order_by">>, V, Acc) ->
                Acc#{order_by => convert_order_by(V)};
           (<<"limit">>, V, Acc) when is_integer(V) ->
                Acc#{limit => V};
           (<<"offset">>, V, Acc) when is_integer(V) ->
                Acc#{offset => V};
           (<<"include_docs">>, V, Acc) when is_boolean(V) ->
                Acc#{include_docs => V};
           (<<"continuation">>, V, Acc) when is_binary(V) ->
                Acc#{continuation => V};
           (<<"chunk_size">>, V, Acc) when is_integer(V) ->
                Acc#{chunk_size => V};
           (_, _, Acc) ->
                Acc
        end,
        #{},
        Spec
    ),
    %% Ensure where clause is present (required by query compiler)
    %% If no where clause, use "exists id" condition to match all documents
    case maps:is_key(where, BaseSpec) of
        true -> BaseSpec;
        false -> BaseSpec#{where => [{exists, [<<"id">>]}]}
    end.

convert_where_clauses(Clauses) when is_list(Clauses) ->
    lists:map(fun convert_where_clause/1, Clauses);
convert_where_clauses(_) ->
    [].

convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"eq">>, <<"value">> := Value}) ->
    %% Equality uses {path, Path, Value} format
    {path, Path, Value};
convert_where_clause(#{<<"path">> := Path, <<"op">> := Op, <<"value">> := Value}) ->
    %% Comparison operators use {compare, Path, Op, Value} format
    {compare, Path, convert_op(Op), Value};
convert_where_clause(#{<<"path">> := Path, <<"value">> := Value}) ->
    %% Default to equality
    {path, Path, Value};
convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"exists">>}) ->
    {exists, Path};
convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"missing">>}) ->
    {missing, Path};
convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"prefix">>, <<"value">> := Value}) ->
    {prefix, Path, Value};
convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"in">>, <<"value">> := Value}) when is_list(Value) ->
    {in, Path, Value};
convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"contains">>, <<"value">> := Value}) ->
    {contains, Path, Value};
convert_where_clause(_) ->
    {error, invalid_clause}.

convert_op(<<"ne">>) -> '=/=';
convert_op(<<"gt">>) -> '>';
convert_op(<<"gte">>) -> '>=';
convert_op(<<"lt">>) -> '<';
convert_op(<<"lte">>) -> '=<';
convert_op(Op) when is_binary(Op) -> binary_to_atom(Op);
convert_op(Op) -> Op.

convert_order_by(Orders) when is_list(Orders) ->
    lists:map(fun convert_order_spec/1, Orders);
convert_order_by(_) ->
    [].

convert_order_spec(#{<<"path">> := Path, <<"dir">> := <<"asc">>}) ->
    {Path, asc};
convert_order_spec(#{<<"path">> := Path, <<"dir">> := <<"desc">>}) ->
    {Path, desc};
convert_order_spec(#{<<"path">> := Path}) ->
    {Path, asc};
convert_order_spec(_) ->
    {error, invalid_order}.

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
%% Attachment Handlers
%%====================================================================

handle_list_attachments(Req) ->
    DbName = cowboy_req:binding(db, Req),
    DocId = cowboy_req:binding(doc_id, Req),
    Attachments = barrel_docdb:list_attachments(DbName, DocId),
    Body = encode_response(Attachments, Req),
    {200, response_headers(Req), Body, Req}.

handle_get_attachment(Req) ->
    DbName = cowboy_req:binding(db, Req),
    DocId = cowboy_req:binding(doc_id, Req),
    AttName = cowboy_req:binding(att_name, Req),
    %% First get attachment info to decide streaming vs direct
    case barrel_docdb:get_attachment_info(DbName, DocId, AttName) of
        {ok, #{chunked := true, content_type := ContentType, length := Length} = _Info} ->
            %% Large/chunked attachment - stream it
            Headers = #{
                <<"content-type">> => ContentType,
                <<"content-length">> => integer_to_binary(Length)
            },
            StreamFun = fun(StreamReq) ->
                stream_attachment(DbName, DocId, AttName, StreamReq)
            end,
            {stream, 200, Headers, StreamFun, Req};
        {ok, #{content_type := ContentType}} ->
            %% Small attachment - return directly
            case barrel_docdb:get_attachment(DbName, DocId, AttName) of
                {ok, Data} ->
                    Headers = #{<<"content-type">> => ContentType},
                    {200, Headers, Data, Req};
                {error, Reason} ->
                    throw({error, 500, format_error(Reason)})
            end;
        not_found ->
            throw({error, 404, <<"Attachment not found">>});
        {error, not_found} ->
            throw({error, 404, <<"Attachment not found">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

%% @private Stream attachment chunks to HTTP response
stream_attachment(DbName, DocId, AttName, Req) ->
    case barrel_docdb:open_attachment_stream(DbName, DocId, AttName) of
        {ok, Stream} ->
            stream_attachment_loop(Stream, Req);
        {error, Reason} ->
            logger:error("Failed to open attachment stream: ~p", [Reason])
    end.

stream_attachment_loop(Stream, Req) ->
    case barrel_docdb:read_attachment_chunk(Stream) of
        {ok, Chunk, NewStream} ->
            cowboy_req:stream_body(Chunk, nofin, Req),
            stream_attachment_loop(NewStream, Req);
        eof ->
            cowboy_req:stream_body(<<>>, fin, Req),
            barrel_docdb:close_attachment_stream(Stream)
    end.

handle_put_attachment(Req0) ->
    DbName = cowboy_req:binding(db, Req0),
    DocId = cowboy_req:binding(doc_id, Req0),
    AttName = cowboy_req:binding(att_name, Req0),
    ContentType = cowboy_req:header(<<"content-type">>, Req0, <<"application/octet-stream">>),
    ContentLength = cowboy_req:header(<<"content-length">>, Req0),

    %% Use streaming upload for large files (> 64KB) or when content-length not provided
    UseStreaming = case ContentLength of
        undefined -> true;
        Len -> binary_to_integer(Len) > 65536
    end,

    case UseStreaming of
        true ->
            %% Stream upload
            handle_put_attachment_stream(DbName, DocId, AttName, ContentType, Req0);
        false ->
            %% Small file - read all at once
            {ok, Data, Req1} = cowboy_req:read_body(Req0),
            case barrel_docdb:put_attachment(DbName, DocId, AttName, Data) of
                {ok, Info} ->
                    Response = format_attachment_response(Info),
                    Body = encode_response(Response, Req1),
                    {201, response_headers(Req1), Body, Req1};
                {error, Reason} ->
                    throw({error, 500, format_error(Reason)})
            end
    end.

%% @private Handle streaming upload of attachment
handle_put_attachment_stream(DbName, DocId, AttName, ContentType, Req0) ->
    case barrel_docdb:open_attachment_writer(DbName, DocId, AttName, ContentType) of
        {ok, Writer} ->
            case stream_upload_body(Req0, Writer) of
                {ok, FinalWriter, Req1} ->
                    case barrel_docdb:finish_attachment_writer(FinalWriter) of
                        {ok, Info} ->
                            Response = format_attachment_response(Info),
                            Body = encode_response(Response, Req1),
                            {201, response_headers(Req1), Body, Req1};
                        {error, Reason} ->
                            throw({error, 500, format_error(Reason)})
                    end;
                {error, Reason} ->
                    throw({error, 500, format_error(Reason)})
            end;
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

%% @private Read body in chunks and write to attachment writer
stream_upload_body(Req, Writer) ->
    %% Read body in 64KB chunks
    case cowboy_req:read_body(Req, #{length => 65536}) of
        {ok, Data, Req1} ->
            %% Last chunk
            case barrel_docdb:write_attachment_chunk(Writer, Data) of
                {ok, FinalWriter} ->
                    {ok, FinalWriter, Req1};
                {error, _} = Error ->
                    Error
            end;
        {more, Data, Req1} ->
            %% More data coming
            case barrel_docdb:write_attachment_chunk(Writer, Data) of
                {ok, NewWriter} ->
                    stream_upload_body(Req1, NewWriter);
                {error, _} = Error ->
                    Error
            end
    end.

%% @private Format attachment info for response
format_attachment_response(Info) ->
    maps:fold(
        fun(name, V, Acc) -> Acc#{<<"name">> => V};
           (length, V, Acc) -> Acc#{<<"size">> => V};
           (digest, V, Acc) -> Acc#{<<"digest">> => V};
           (content_type, V, Acc) -> Acc#{<<"content_type">> => V};
           (chunked, V, Acc) -> Acc#{<<"chunked">> => V};
           (chunk_count, V, Acc) -> Acc#{<<"chunk_count">> => V};
           (chunk_size, V, Acc) -> Acc#{<<"chunk_size">> => V};
           (K, V, Acc) when is_atom(K) -> Acc#{atom_to_binary(K) => V};
           (K, V, Acc) -> Acc#{K => V}
        end,
        #{<<"ok">> => true},
        Info
    ).

handle_delete_attachment(Req) ->
    DbName = cowboy_req:binding(db, Req),
    DocId = cowboy_req:binding(doc_id, Req),
    AttName = cowboy_req:binding(att_name, Req),
    case barrel_docdb:delete_attachment(DbName, DocId, AttName) of
        ok ->
            Response = #{<<"ok">> => true},
            Body = encode_response(Response, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Attachment not found">>});
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

%% Encode document with raw CBOR body (zero-copy for CBOR responses)
%% Merges metadata into the raw body CBOR without full decode/re-encode
encode_doc_with_raw_body(CborBin, Meta) ->
    #{id := Id, rev := Rev, deleted := Deleted} = Meta,
    Conflicts = maps:get(conflicts, Meta, []),
    %% Build metadata map
    MetaMap = #{
        <<"id">> => Id,
        <<"_rev">> => Rev
    },
    MetaMap2 = case Deleted of
        true -> MetaMap#{<<"_deleted">> => true};
        false -> MetaMap
    end,
    MetaMap3 = case Conflicts of
        [] -> MetaMap2;
        _ -> MetaMap2#{<<"_conflicts">> => Conflicts}
    end,
    %% Merge metadata with raw body using efficient CBOR merge
    barrel_docdb_codec_cbor:merge_into_cbor(CborBin, MetaMap3).

%% Encode CBOR response with raw document bodies (for query results)
%% Documents may be raw CBOR binaries (doc_format=binary) or maps
encode_cbor_with_raw_docs(Results, Meta) ->
    %% Convert results - raw CBOR docs stay as-is, maps need decoding
    DecodedResults = [decode_doc_for_cbor(Doc) || Doc <- Results],
    Response = #{
        <<"results">> => DecodedResults,
        <<"meta">> => Meta
    },
    barrel_docdb_codec_cbor:encode_cbor(Response).

%% Decode document for CBOR response encoding
%% Raw CBOR binary -> decode to map for inclusion in response
%% Map -> use as-is
decode_doc_for_cbor(Doc) when is_binary(Doc) ->
    barrel_docdb_codec_cbor:decode_any(Doc);
decode_doc_for_cbor(Doc) when is_map(Doc) ->
    Doc.

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
    %% Parse feed type (normal or longpoll)
    Feed = case proplists:get_value(<<"feed">>, Qs) of
        <<"longpoll">> -> longpoll;
        _ -> normal
    end,
    %% Parse filter pattern (MQTT-style)
    FilterPattern = proplists:get_value(<<"filter">>, Qs),
    %% Parse other options
    Opts = lists:foldl(
        fun({<<"limit">>, LimitBin}, Acc) ->
                Acc#{limit => binary_to_integer(LimitBin)};
           ({<<"include_docs">>, <<"true">>}, Acc) ->
                Acc#{include_docs => true};
           ({<<"descending">>, <<"true">>}, Acc) ->
                Acc#{descending => true};
           ({<<"timeout">>, TimeoutBin}, Acc) ->
                Acc#{timeout => binary_to_integer(TimeoutBin)};
           (_, Acc) ->
                Acc
        end,
        #{},
        Qs
    ),
    {Since, Feed, FilterPattern, Opts}.

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

%%====================================================================
%% Health Check
%%====================================================================

%% @doc Collect comprehensive health information for the node
collect_health_info() ->
    %% Basic node info
    NodeName = atom_to_binary(node()),
    {ok, Version} = application:get_key(barrel_docdb, vsn),
    VersionBin = iolist_to_binary(Version),

    %% Memory info
    MemInfo = erlang:memory(),
    TotalMem = proplists:get_value(total, MemInfo),
    ProcessMem = proplists:get_value(processes, MemInfo),
    BinaryMem = proplists:get_value(binary, MemInfo),
    EtsMem = proplists:get_value(ets, MemInfo),

    %% Process info
    ProcessCount = erlang:system_info(process_count),
    ProcessLimit = erlang:system_info(process_limit),

    %% Scheduler info
    SchedulersOnline = erlang:system_info(schedulers_online),
    SchedulerUsage = try
        scheduler_wall_time_usage()
    catch _:_ -> undefined
    end,

    %% Database health
    {DbStatus, DbHealth} = collect_database_health(),

    %% Peer status
    PeerHealth = collect_peer_health(),

    %% Overall status
    Status = case DbStatus of
        ok -> <<"ok">>;
        degraded -> <<"degraded">>;
        unhealthy -> <<"unhealthy">>
    end,

    #{
        <<"status">> => Status,
        <<"node">> => NodeName,
        <<"version">> => VersionBin,
        <<"uptime_seconds">> => uptime_seconds(),
        <<"memory">> => #{
            <<"total_bytes">> => TotalMem,
            <<"process_bytes">> => ProcessMem,
            <<"binary_bytes">> => BinaryMem,
            <<"ets_bytes">> => EtsMem
        },
        <<"processes">> => #{
            <<"count">> => ProcessCount,
            <<"limit">> => ProcessLimit,
            <<"utilization">> => ProcessCount / ProcessLimit
        },
        <<"schedulers">> => #{
            <<"online">> => SchedulersOnline,
            <<"utilization">> => SchedulerUsage
        },
        <<"databases">> => DbHealth,
        <<"peers">> => PeerHealth
    }.

%% @private Get node uptime in seconds
uptime_seconds() ->
    {UpTime, _} = erlang:statistics(wall_clock),
    UpTime div 1000.

%% @private Calculate scheduler utilization
scheduler_wall_time_usage() ->
    case erlang:statistics(scheduler_wall_time) of
        undefined ->
            %% Enable scheduler wall time if not already
            erlang:system_flag(scheduler_wall_time, true),
            undefined;
        SchedulerTimes ->
            TotalActive = lists:sum([A || {_, A, _} <- SchedulerTimes]),
            TotalTime = lists:sum([T || {_, _, T} <- SchedulerTimes]),
            case TotalTime of
                0 -> 0.0;
                _ -> TotalActive / TotalTime
            end
    end.

%% @private Collect health status for all databases
collect_database_health() ->
    DbNames = barrel_docdb:list_dbs(),
    %% Filter out system databases from health reporting
    UserDbNames = [N || N <- DbNames, not is_system_db(N)],
    DbHealthList = lists:map(fun collect_single_db_health/1, UserDbNames),
    %% Determine overall status
    Statuses = [maps:get(<<"status">>, H) || H <- DbHealthList],
    OverallStatus = case lists:member(<<"unhealthy">>, Statuses) of
        true -> unhealthy;
        false ->
            case lists:member(<<"degraded">>, Statuses) of
                true -> degraded;
                false -> ok
            end
    end,
    {OverallStatus, DbHealthList}.

%% @private Check if database is a system database
is_system_db(<<"_", _/binary>>) -> true;
is_system_db(_) -> false.

%% @private Collect health for a single database
collect_single_db_health(DbName) ->
    case barrel_docdb:db_info(DbName) of
        {ok, Info} ->
            DocCount = maps:get(doc_count, Info, 0),
            #{
                <<"name">> => DbName,
                <<"status">> => <<"ok">>,
                <<"doc_count">> => DocCount
            };
        {error, _} ->
            #{
                <<"name">> => DbName,
                <<"status">> => <<"unhealthy">>,
                <<"error">> => <<"unable_to_access">>
            }
    end.

%% @private Collect peer health status
collect_peer_health() ->
    case barrel_discovery:list_peers() of
        {ok, Peers} ->
            TotalPeers = length(Peers),
            ActivePeers = length([P || P <- Peers,
                                       maps:get(status, P, unknown) =:= active]),
            #{
                <<"total">> => TotalPeers,
                <<"active">> => ActivePeers,
                <<"inactive">> => TotalPeers - ActivePeers
            };
        {error, _} ->
            #{
                <<"total">> => 0,
                <<"active">> => 0,
                <<"inactive">> => 0
            }
    end.
