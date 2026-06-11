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

-export([handle/2]).

%% Content types
-define(CT_JSON, <<"application/json">>).
-define(CT_CBOR, <<"application/cbor">>).

%%====================================================================
%% Livery handler entry
%%====================================================================

%% Called by livery for every routed request. The route table in
%% `barrel_http_server' wraps each route with a closure that hands
%% the action atom in as the first argument.
%% Authentication and per-action authorisation run in the
%% `barrel_docdb_auth' livery middleware attached to every route
%% in `barrel_http_server'. The validated key map is available to
%% handler clauses via `livery_req:meta(auth_ctx, Req, _)' if they
%% need it. By the time we get here the request is authorised.
handle(Action, Req0) ->
    Method = livery_req:method(Req0),
    try
        build_response(handle_action(Action, Method, Req0))
    catch
        throw:{error, ErrStatus, Message} ->
            error_response(ErrStatus, Message, Req0);
        error:{query_timeout, Info} ->
            query_timeout_response(Info, Req0);
        Class:Reason:Stack ->
            logger:error("HTTP handler error: ~p:~p~n~p",
                         [Class, Reason, Stack]),
            error_response(500, <<"Internal server error">>, Req0)
    end.

%% Map a `{query_timeout, Info}' error from the query pipeline to a
%% 504 Gateway Timeout with a structured JSON payload so the caller
%% can tell the result was aborted rather than empty.
query_timeout_response(Info, Req) ->
    Body = encode_response(Info#{<<"error">> => <<"query_timeout">>}, Req),
    new_resp(504, response_headers(Req), Body).

%% Map the per-action `{Status, Headers, Body, _Req}' /
%% `{stream, Status, Headers, EmitFun, _Req}' tuples to a livery
%% response value. Plain bodies become a generic typed response;
%% streamed bodies hand the Emit callback to the action's producer.
%% Per-request HTTP trace spans and metrics are recorded by the
%% livery middleware stack (`livery_instrument_trace' and
%% `livery_instrument_metrics' in `barrel_http_server'); no manual
%% wrapping needed here.
build_response({Status, Headers, Body, _Req}) ->
    new_resp(Status, Headers, Body);
build_response({stream, Status, Headers, EmitFun, _Req}) ->
    livery_resp:stream(Status, header_list(Headers), EmitFun).

error_response(Status, Message, Req) ->
    Body = encode_error(Message, Req),
    new_resp(Status, response_headers(Req), Body).

new_resp(Status, Headers, Body) ->
    livery_resp:new(Status, header_list(Headers), {full, iolist_to_binary(Body)}).

%% Header coercion: all handler clauses now build lists, so this is
%% effectively the identity. Kept as a single function so future
%% inputs that arrive as maps stay tolerated without touching the
%% callers.
header_list(H) -> H.

%% Read the full request body. Used by the handler clauses that
%% mirror the old `{ok, Body, Req1} = cowboy_req:read_body/1' pattern;
%% in livery the request value is immutable so we just return the
%% same Req back.
read_full_body(Req) ->
    case livery_req:body(Req) of
        empty ->
            {<<>>, Req};
        {buffered, IoData} ->
            {iolist_to_binary(IoData), Req};
        {stream, Reader} ->
            case livery_body:read_all(Reader) of
                {ok, Bin, _} -> {Bin, Req};
                {error, _, _} -> {<<>>, Req}
            end
    end.

%% Parse the request query string as a proplist. Mirrors
%% `cowboy_req:parse_qs/1' so the existing query-handling code can
%% stay unchanged.
parse_qs(Req) ->
    case livery_req:query(Req) of
        <<>> -> [];
        Raw  -> uri_string:dissect_query(Raw)
    end.

%% Reach into the inbound Authorization header. Used only by the
%% replicate handler to forward the caller's bearer token to a
%% remote replication target when the body doesn't explicitly
%% set `target_auth'.
inherit_bearer_token(Req) ->
    case livery_req:header(<<"authorization">>, Req) of
        undefined                  -> undefined;
        <<"Bearer ", Token/binary>> -> Token;
        <<"bearer ", Token/binary>> -> Token;
        _                          -> undefined
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

%% Note: GET /metrics is now served directly by `livery_metrics:handler/0'
%% wired in `barrel_http_server:router/0'; no `metrics' action here.

%% Node identity (no discovery/federation, just this node's id and version)
handle_action(node_info, <<"GET">>, Req) ->
    Body = encode_response(node_info_map(), Req),
    {200, response_headers(Req), Body, Req};

%% Database info
handle_action(db_info, <<"GET">>, Req) ->
    DbName = livery_req:binding(<<"db">>, Req),
    case barrel_docdb:db_info(DbName) of
        {ok, Info} ->
            %% Sanitize info for JSON/CBOR encoding (remove pid, format atom keys)
            SafeInfo = sanitize_db_info(Info),
            Body = encode_response(SafeInfo, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Database not found">>})
    end;
handle_action(db_info, <<"PUT">>, Req) ->
    DbName = livery_req:binding(<<"db">>, Req),
    case barrel_docdb:create_db(DbName) of
        {ok, _Pid} ->
            Body = encode_response(#{ok => true, name => DbName}, Req),
            {201, response_headers(Req), Body, Req};
        {error, already_exists} ->
            throw({error, 409, <<"Database already exists">>});
        {error, invalid_db_name} ->
            throw({error, 400, <<"Invalid database name">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end;
handle_action(db_info, <<"DELETE">>, Req) ->
    DbName = livery_req:binding(<<"db">>, Req),
    case barrel_docdb:delete_db(DbName) of
        ok ->
            Body = encode_response(#{ok => true}, Req),
            {200, response_headers(Req), Body, Req};
        {error, invalid_db_name} ->
            throw({error, 400, <<"Invalid database name">>});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end;
handle_action(db_info, <<"POST">>, Req) ->
    handle_post_doc(Req);

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

%% Replication: trigger one-shot replication
handle_action(replicate, <<"POST">>, Req) ->
    handle_replicate(Req);

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

%% Replication peer registry (admin only)
handle_action(peers, <<"GET">>, Req) ->
    handle_list_peers(Req);
handle_action(peers, <<"POST">>, Req) ->
    handle_register_peer(Req);
handle_action(peer, <<"GET">>, Req) ->
    handle_get_peer(Req);
handle_action(peer, <<"DELETE">>, Req) ->
    handle_delete_peer(Req);

%% Admin usage endpoints (admin only)
handle_action(admin_usage, <<"GET">>, Req) ->
    handle_admin_usage(Req);
handle_action(admin_db_usage, <<"GET">>, Req) ->
    handle_admin_db_usage(Req);

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
    DbName = livery_req:binding(<<"db">>, Req),
    DocId = livery_req:binding(<<"doc_id">>, Req),
    Qs = parse_qs(Req),
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
    DbName = livery_req:binding(<<"db">>, Req0),
    DocId = livery_req:binding(<<"doc_id">>, Req0),
    {ReqBody, Req1} = read_full_body(Req0),
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

%% @doc Create a document with auto-generated ID.
%% POST /db/:db
handle_post_doc(Req0) ->
    DbName = livery_req:binding(<<"db">>, Req0),
    {ReqBody, Req1} = read_full_body(Req0),
    Doc = decode_request_body(ReqBody, Req1),
    %% Don't set ID - let barrel_docdb auto-generate it
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
    DbName = livery_req:binding(<<"db">>, Req),
    DocId = livery_req:binding(<<"doc_id">>, Req),
    Qs = parse_qs(Req),
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
    DbName = livery_req:binding(<<"db">>, Req0),
    {Since0, Feed, FilterPattern, Opts0} = parse_changes_opts(Req0),
    Timeout = maps:get(timeout, Opts0, 60000),

    %% Read POST body for doc_ids and query filters
    {Opts1, Req1} = case livery_req:method(Req0) of
        <<"POST">> ->
            {Body, Req} = read_full_body(Req0),
            {merge_body_opts(Body, Opts0), Req};
        _ ->
            {Opts0, Req0}
    end,

    %% Resolve 'now' to current HLC for this database
    Since = resolve_since(DbName, Since0),

    %% Handle filter pattern:
    %% - Patterns with + wildcards need client-side filtering (path index doesn't support +)
    %% - Patterns with only # wildcards can use backend paths option
    %% - Exact patterns can use backend paths option
    {Opts, FilterFun} = case FilterPattern of
        undefined ->
            {Opts1, undefined};
        Pattern ->
            case needs_client_side_filter(Pattern) of
                true ->
                    %% Contains + wildcard - use client-side filtering
                    {Opts1, create_filter_fun(Pattern)};
                false ->
                    %% No + wildcard - use backend paths option
                    {Opts1#{paths => [Pattern]}, undefined}
            end
    end,

    case Feed of
        longpoll ->
            handle_longpoll_changes(DbName, Since, FilterFun, Opts, Timeout, Req1);
        normal ->
            handle_normal_changes(DbName, Since, FilterFun, Opts, Req1)
    end.

%% Normal poll - return changes immediately
handle_normal_changes(DbName, Since, FilterFun, Opts, Req0) ->
  {ok, Changes, LastHlc}  =  barrel_docdb:get_changes(DbName, Since, Opts),
  FilteredChanges = filter_changes(Changes, FilterFun),
  FormattedChanges = lists:map(fun format_change/1, FilteredChanges),
  Response = #{
               <<"results">> => FormattedChanges,
               <<"last_seq">> => format_hlc(LastHlc)
              },
  Body = encode_response(Response, Req0),
  {200, response_headers(Req0), Body, Req0}.

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
                    end
            end
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

%% Check if a filter pattern needs client-side filtering
%% Patterns with + wildcards need client-side filtering because the path index
%% only supports exact matches and # (multi-level) wildcards
needs_client_side_filter(Pattern) when is_binary(Pattern) ->
    binary:match(Pattern, <<"+">>) =/= nomatch.

%% Create filter function from MQTT-style pattern
%% Supports:
%%   + : matches one segment
%%   # : matches zero or more segments (must be last)
%% Callers handle the no-filter case before calling, so Pattern is always a binary.
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

%%====================================================================
%% Bulk Docs Handler
%%====================================================================

handle_bulk_docs(Req0) ->
    DbName = livery_req:binding(<<"db">>, Req0),
    {ReqBody, Req1} = read_full_body(Req0),
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
%% Query Handler
%%====================================================================

handle_find(Req0) ->
    DbName = livery_req:binding(<<"db">>, Req0),
    {ReqBody, Req1} = read_full_body(Req0),
    QuerySpec0 = decode_request_body(ReqBody, Req1),
    %% Convert binary keys to atoms for internal API
    QuerySpec = convert_query_spec(QuerySpec0),
    case barrel_docdb:find(DbName, QuerySpec) of
        {ok, Results, Meta} ->
            FormattedMeta = format_query_meta(Meta),
            case response_content_type(Req1) of
                ?CT_CBOR ->
                    Body = encode_cbor_with_raw_docs(Results, FormattedMeta),
                    {200, response_headers(Req1), Body, Req1};
                ?CT_JSON ->
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
    iolist_to_binary(json:encode(Doc)).

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
%% The sequence is an HLC timestamp from barrel_hlc
format_seq(Seq) when is_binary(Seq) ->
    base64:encode(Seq);
format_seq(Hlc) when is_tuple(Hlc), element(1, Hlc) =:= timestamp ->
    base64:encode(barrel_hlc:encode(Hlc)).

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
           (<<"selector">>, V, Acc) when is_map(V) ->
                %% Pass through selector for VDB (CouchDB-style)
                Acc#{<<"selector">> => V};
           (<<"order_by">>, V, Acc) ->
                Acc#{order_by => convert_order_by(V)};
           (<<"sort">>, V, Acc) ->
                %% Also handle "sort" as alias for order_by
                Acc#{sort => V};
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
    %% If no where clause and no selector, use "exists id" condition to match all documents
    case maps:is_key(where, BaseSpec) orelse maps:is_key(<<"selector">>, BaseSpec) of
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

%% @doc Map a JSON `op' value to the internal atom.
%% Whitelist-only: any other binary is rejected as 400. The previous
%% catch-all `binary_to_atom/1' on user input could exhaust the atom
%% table. The set here must match `barrel_query:compare_op()'.
convert_op(<<"eq">>)  -> '==';
convert_op(<<"==">>)  -> '==';
convert_op(<<"ne">>)  -> '=/=';
convert_op(<<"!=">>)  -> '=/=';
convert_op(<<"=/=">>) -> '=/=';
convert_op(<<"gt">>)  -> '>';
convert_op(<<">">>)   -> '>';
convert_op(<<"gte">>) -> '>=';
convert_op(<<">=">>)  -> '>=';
convert_op(<<"lt">>)  -> '<';
convert_op(<<"<">>)   -> '<';
convert_op(<<"lte">>) -> '=<';
convert_op(<<"<=">>)  -> '=<';
convert_op(<<"=<">>)  -> '=<';
convert_op(Op) when is_binary(Op) ->
    throw({error, 400, <<"Unsupported query operator: ", Op/binary>>});
convert_op(Op) when is_atom(Op) -> Op.

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

%% @doc Handle one-shot replication request
%% POST /db/:db/_replicate
%% Body: {"target": "http://...", "filter": {...}, "target_auth": "..."}
handle_replicate(Req0) ->
    DbName = livery_req:binding(<<"db">>, Req0),
    {ReqBody, Req1} = read_full_body(Req0),
    Spec = decode_request_body(ReqBody, Req1),
    %% Build replication options
    Opts0 = #{},
    Opts1 = case maps:get(<<"filter">>, Spec, undefined) of
        undefined -> Opts0;
        #{<<"paths">> := Paths} ->
            %% Convert filter paths to internal format
            Opts0#{filter => #{paths => Paths}};
        Filter when is_map(Filter) ->
            Opts0#{filter => Filter}
    end,
    %% Resolve the target. Either:
    %%   - `peer_id': an exact registry hit, URL pulled from the
    %%     registered entry (no URL in the request body at all);
    %%   - `target': must match a registered peer's canonical URL.
    %% Anything else is rejected (closes SSRF: only registered hosts
    %% are reachable). Local-db replication (`target' = a plain db
    %% name) is unchanged.
    {FinalTarget, Opts} = resolve_replicate_target(Spec, Req1, Opts1),
    %% Execute one-shot replication
    case barrel_rep:replicate(DbName, FinalTarget, Opts) of
        {ok, Result} ->
            %% Format result for JSON response
            Response = format_rep_result(Result),
            Body = encode_response(Response, Req1),
            {200, response_headers(Req1), Body, Req1};
        {error, {target_error, Reason}} ->
            throw({error, 400, format_error({target_error, Reason})});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

%% Check if a string is a URL
is_url(<<"http://", _/binary>>) -> true;
is_url(<<"https://", _/binary>>) -> true;
is_url(_) -> false.

%% Resolve `/_replicate' target into either:
%%   - `{Endpoint, Opts}' for a registered remote peer
%%     (Endpoint = #{url, bearer_token, peer_auth => true}); or
%%   - `{LocalDbName, Opts}' when the request is local-db
%%     replication (target is a plain db name, no URL).
%% Rejects unregistered URLs with a 403 — that's the SSRF gate.
%% Operators can flip `replication_require_registered_peer' to
%% `false' for migration, in which case the legacy free-form target
%% URL behaviour returns.
resolve_replicate_target(Spec, Req, Opts) ->
    Strict = application:get_env(barrel_docdb,
                                 replication_require_registered_peer,
                                 true),
    case maps:get(<<"peer_id">>, Spec, undefined) of
        undefined ->
            resolve_by_target(Spec, Req, Opts, Strict);
        PeerId when is_binary(PeerId) ->
            case barrel_peer_registry:get(PeerId) of
                {ok, #{url := Url}} ->
                    {build_remote_endpoint(Url, target_bearer(Spec, Req)),
                     Opts#{target_transport => barrel_rep_transport_http}};
                {error, not_found} ->
                    throw({error, 403,
                           <<"unregistered_peer: unknown peer_id">>})
            end
    end.

resolve_by_target(Spec, Req, Opts, Strict) ->
    case maps:get(<<"target">>, Spec, undefined) of
        undefined ->
            throw({error, 400, <<"missing target or peer_id">>});
        Target ->
            case is_url(Target) of
                false ->
                    %% Local-db replication (target is a db name).
                    {Target, Opts};
                true ->
                    case Strict of
                        true ->
                            case barrel_peer_registry:lookup_by_url(Target) of
                                {ok, #{url := Url}} ->
                                    {build_remote_endpoint(Url, target_bearer(Spec, Req)),
                                     Opts#{target_transport => barrel_rep_transport_http}};
                                {error, not_found} ->
                                    throw({error, 403,
                                           <<"unregistered_peer: target URL is not in the peer registry">>})
                            end;
                        false ->
                            %% Migration escape hatch. Legacy
                            %% behaviour: trust whatever URL the
                            %% caller asked for.
                            {build_remote_endpoint(Target, target_bearer(Spec, Req)),
                             Opts#{target_transport => barrel_rep_transport_http}}
                    end
            end
    end.

target_bearer(Spec, Req) ->
    case maps:get(<<"target_auth">>, Spec, undefined) of
        undefined -> inherit_bearer_token(Req);
        Token     -> Token
    end.

build_remote_endpoint(Url, undefined) ->
    #{url => Url, peer_auth => true};
build_remote_endpoint(Url, Token) when is_binary(Token) ->
    #{url => Url, bearer_token => Token, peer_auth => true}.

%% Format replication result for JSON response
format_rep_result(Result) when is_map(Result) ->
    maps:fold(
        fun(ok, V, Acc) -> Acc#{<<"ok">> => V};
           (docs_written, V, Acc) -> Acc#{<<"docs_written">> => V};
           (docs_read, V, Acc) -> Acc#{<<"docs_read">> => V};
           (doc_read_failures, V, Acc) -> Acc#{<<"doc_read_failures">> => V};
           (doc_write_failures, V, Acc) -> Acc#{<<"doc_write_failures">> => V};
           (start_seq, first, Acc) -> Acc#{<<"start_seq">> => <<"first">>};
           (start_seq, V, Acc) -> Acc#{<<"start_seq">> => format_seq_for_rep(V)};
           (last_seq, first, Acc) -> Acc#{<<"last_seq">> => <<"first">>};
           (last_seq, V, Acc) -> Acc#{<<"last_seq">> => format_seq_for_rep(V)};
           (source, V, Acc) when is_binary(V) -> Acc#{<<"source">> => V};
           (target, V, Acc) when is_binary(V) -> Acc#{<<"target">> => V};
           (_, _, Acc) -> Acc  %% Skip unknown fields to avoid serialization issues
        end,
        #{},
        Result
    ).

%% Format seq for replication result (convert tuples to strings)
format_seq_for_rep({timestamp, Ts, Counter}) ->
    iolist_to_binary(io_lib:format("~p:~p", [Ts, Counter]));
format_seq_for_rep(Seq) when is_tuple(Seq) ->
    iolist_to_binary(io_lib:format("~p", [Seq]));
format_seq_for_rep(Seq) when is_binary(Seq) ->
    Seq;
format_seq_for_rep(Seq) when is_integer(Seq) ->
    integer_to_binary(Seq);
format_seq_for_rep(_) ->
    <<"unknown">>.

handle_revsdiff(Req0) ->
    DbName = livery_req:binding(<<"db">>, Req0),
    {ReqBody, Req1} = read_full_body(Req0),
    ok = require_peer_signature(<<"POST">>, Req1, ReqBody),
    RequestBody = decode_request_body(ReqBody, Req1),
    %% Support both single-doc and batch formats:
    %% Single-doc: {"id": DocId, "revs": [RevIds]}
    %% Batch: {"revs": {DocId => [RevIds], ...}} (no "id" key, "revs" is a map)
    case RequestBody of
        #{<<"id">> := DocId, <<"revs">> := RevIds} when is_list(RevIds) ->
            %% Single document format
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
            end;
        #{<<"revs">> := RevsMap} when is_map(RevsMap) ->
            %% Batch format: map of DocId => [RevIds]
            {ok, Results} = barrel_docdb:revsdiff_batch(DbName, RevsMap),
            Body = encode_response(Results, Req1),
            {200, response_headers(Req1), Body, Req1};
        _ ->
            throw({error, 400, <<"Invalid revsdiff request format">>})
    end.

handle_put_rev(Req0) ->
    DbName = livery_req:binding(<<"db">>, Req0),
    {ReqBody, Req1} = read_full_body(Req0),
    ok = require_peer_signature(<<"POST">>, Req1, ReqBody),
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
    DbName = livery_req:binding(<<"db">>, Req0),
    {ReqBody, Req1} = read_full_body(Req0),
    ok = require_peer_signature(<<"POST">>, Req1, ReqBody),
    case decode_request_body(ReqBody, Req1) of
        #{<<"hlc">> := HlcBin} when is_binary(HlcBin) ->
            case parse_hlc(HlcBin) of
                {ok, RemoteHlc} ->
                    do_sync_hlc(DbName, RemoteHlc, Req1);
                {error, invalid_hlc} ->
                    throw({error, 400, <<"Invalid hlc value">>})
            end;
        _ ->
            throw({error, 400, <<"Missing hlc field">>})
    end.

do_sync_hlc(DbName, RemoteHlc, Req) ->
    case barrel_docdb:sync_hlc(RemoteHlc) of
        {ok, LocalHlc} ->
            Response = #{<<"hlc">> => format_hlc(LocalHlc), <<"db">> => DbName},
            Body = encode_response(Response, Req),
            {200, response_headers(Req), Body, Req};
        {error, clock_skew} ->
            throw({error, 409, <<"Clock skew rejected">>})
    end.

%% @private Require a valid Ed25519 signature from a registered
%% peer on inbound replication-receiving requests. The API-key gate
%% has already run in `barrel_docdb_auth' middleware; this is the
%% additional peer-trust check that scopes who may push data in.
%% Disabled when `replication_require_registered_peer' is `false'.
require_peer_signature(Method, Req, Body) ->
    Strict = application:get_env(barrel_docdb,
                                 replication_require_registered_peer,
                                 true),
    case Strict of
        false -> ok;
        true  ->
            Path = livery_req:path(Req),
            Headers = livery_req:headers(Req),
            LookupFun = fun barrel_peer_registry:lookup/1,
            case barrel_peer_auth:verify_request(LookupFun, Method, Path,
                                                 Body, Headers, #{}) of
                ok ->
                    case maps:get(<<"x-peer-id">>,
                                  normalise_headers(Headers), undefined) of
                        undefined -> ok;
                        PeerId    ->
                            barrel_peer_registry:touch_last_used(PeerId),
                            ok
                    end;
                {error, missing_peer_id} ->
                    throw({error, 401, <<"peer_signature_required">>});
                {error, missing_timestamp} ->
                    throw({error, 401, <<"peer_signature_required">>});
                {error, missing_signature} ->
                    throw({error, 401, <<"peer_signature_required">>});
                {error, not_found} ->
                    throw({error, 401, <<"unregistered_peer">>});
                {error, timestamp_expired} ->
                    throw({error, 401, <<"peer_signature_expired">>});
                {error, invalid_signature} ->
                    throw({error, 401, <<"invalid_peer_signature">>});
                {error, _Reason} ->
                    throw({error, 401, <<"peer_signature_required">>})
            end
    end.

normalise_headers(Headers) when is_list(Headers) ->
    lists:foldl(fun({K, V}, Acc) ->
                        maps:put(string:lowercase(K), V, Acc)
                end, #{}, Headers).

%%====================================================================
%% Local Document Handlers
%%====================================================================

handle_get_local_doc(Req) ->
    DbName = livery_req:binding(<<"db">>, Req),
    DocId = livery_req:binding(<<"doc_id">>, Req),
    case barrel_docdb:get_local_doc(DbName, DocId) of
        {ok, Doc} ->
            Body = encode_response(Doc, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Local document not found">>})
    end.

handle_put_local_doc(Req0) ->
    DbName = livery_req:binding(<<"db">>, Req0),
    DocId = livery_req:binding(<<"doc_id">>, Req0),
    {ReqBody, Req1} = read_full_body(Req0),
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
    DbName = livery_req:binding(<<"db">>, Req),
    DocId = livery_req:binding(<<"doc_id">>, Req),
    case barrel_docdb:delete_local_doc(DbName, DocId) of
        ok ->
            Response = #{<<"ok">> => true},
            Body = encode_response(Response, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Local document not found">>})
    end.

%%====================================================================
%% Attachment Handlers
%%====================================================================

handle_list_attachments(Req) ->
    DbName = livery_req:binding(<<"db">>, Req),
    DocId = livery_req:binding(<<"doc_id">>, Req),
    Attachments = barrel_docdb:list_attachments(DbName, DocId),
    Body = encode_response(Attachments, Req),
    {200, response_headers(Req), Body, Req}.

handle_get_attachment(Req) ->
    DbName = livery_req:binding(<<"db">>, Req),
    DocId = livery_req:binding(<<"doc_id">>, Req),
    AttName = livery_req:binding(<<"att_name">>, Req),
    %% First get attachment info to decide streaming vs direct
    case barrel_docdb:get_attachment_info(DbName, DocId, AttName) of
        {ok, #{chunked := true, content_type := ContentType, length := Length} = _Info} ->
            %% Large/chunked attachment - stream it
            Headers = [
                {<<"content-type">>, ContentType},
                {<<"content-length">>, integer_to_binary(Length)}
            ],
            StreamFun = fun(Emit) ->
                stream_attachment(DbName, DocId, AttName, Emit)
            end,
            {stream, 200, Headers, StreamFun, Req};
        {ok, #{content_type := ContentType}} ->
            %% Small attachment - return directly
            case barrel_docdb:get_attachment(DbName, DocId, AttName) of
                {ok, Data} ->
                    Headers = [{<<"content-type">>, ContentType}],
                    {200, Headers, Data, Req};
                {error, Reason} ->
                    throw({error, 500, format_error(Reason)})
            end;
        {error, not_found} ->
            throw({error, 404, <<"Attachment not found">>})
    end.

%% @private Stream attachment chunks via the livery Emit callback.
stream_attachment(DbName, DocId, AttName, Emit) ->
    case barrel_docdb:open_attachment_stream(DbName, DocId, AttName) of
        {ok, Stream} ->
            stream_attachment_loop(Stream, Emit);
        {error, Reason} ->
            logger:error("Failed to open attachment stream: ~p", [Reason]),
            ok
    end.

stream_attachment_loop(Stream, Emit) ->
    case barrel_docdb:read_attachment_chunk(Stream) of
        {ok, Chunk, NewStream} ->
            case Emit(Chunk) of
                ok ->
                    stream_attachment_loop(NewStream, Emit);
                {error, _} ->
                    %% Client disconnected; stop and release the stream.
                    barrel_docdb:close_attachment_stream(NewStream)
            end;
        eof ->
            barrel_docdb:close_attachment_stream(Stream),
            ok
    end.

handle_put_attachment(Req0) ->
    DbName = livery_req:binding(<<"db">>, Req0),
    DocId = livery_req:binding(<<"doc_id">>, Req0),
    AttName = livery_req:binding(<<"att_name">>, Req0),
    ContentType = livery_req:header(<<"content-type">>, Req0, <<"application/octet-stream">>),
    ContentLength = livery_req:header(<<"content-length">>, Req0),

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
            {Data, Req1} = read_full_body(Req0),
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
                            barrel_docdb:abort_attachment_writer(FinalWriter),
                            throw({error, 500, format_error(Reason)})
                    end;
                {error, Reason, FailedWriter} ->
                    barrel_docdb:abort_attachment_writer(FailedWriter),
                    throw({error, 500, format_error(Reason)})
            end;
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

%% @private Read body in chunks and write to attachment writer.
%% For buffered/empty bodies the data is already in memory, so we
%% just hand it to the writer in one call. For streamed bodies we
%% drain `livery_body:read/2' chunk by chunk.
stream_upload_body(Req, Writer) ->
    case livery_req:body(Req) of
        empty ->
            finalize_chunk(Writer, <<>>, Req);
        {buffered, IoData} ->
            finalize_chunk(Writer, iolist_to_binary(IoData), Req);
        {stream, Reader} ->
            stream_upload_loop(Reader, Writer, Req)
    end.

finalize_chunk(Writer, Bin, Req) ->
    case barrel_docdb:write_attachment_chunk(Writer, Bin) of
        {ok, FinalWriter} -> {ok, FinalWriter, Req};
        {error, Reason}    -> {error, Reason, Writer}
    end.

stream_upload_loop(Reader, Writer, Req) ->
    case livery_body:read(Reader, 5000) of
        {ok, Chunk, Reader1} ->
            case barrel_docdb:write_attachment_chunk(Writer, Chunk) of
                {ok, NewWriter} ->
                    stream_upload_loop(Reader1, NewWriter, Req);
                {error, Reason} ->
                    {error, Reason, Writer}
            end;
        {done, _Reader1} ->
            {ok, Writer, Req};
        {error, Reason, _Reader1} ->
            {error, Reason, Writer}
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
    DbName = livery_req:binding(<<"db">>, Req),
    DocId = livery_req:binding(<<"doc_id">>, Req),
    AttName = livery_req:binding(<<"att_name">>, Req),
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
    case livery_req:header(<<"accept">>, Req) of
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
    case livery_req:header(<<"content-type">>, Req) of
        ?CT_CBOR -> cbor;
        _ -> json
    end.

%% Response headers with content type, HLC for clock sync, and trace context
response_headers(Req) ->
    Hlc = barrel_hlc:get_hlc(),
    HlcBin = barrel_hlc:encode(Hlc),
    Base = [
        {<<"content-type">>, response_content_type(Req)},
        {<<"x-barrel-hlc">>, base64:encode(HlcBin)}
    ],
    %% Trace context headers for distributed tracing.
    TraceHeaders = barrel_trace:inject_headers([]),
    Base ++ TraceHeaders.

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

%% Encode CBOR response for query results
encode_cbor_with_raw_docs(Results, Meta) ->
    Response = #{
        <<"results">> => Results,
        <<"meta">> => Meta
    },
    barrel_docdb_codec_cbor:encode_cbor(Response).

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
    Qs = parse_qs(Req),
    Since = case proplists:get_value(<<"since">>, Qs, <<"0">>) of
        <<"0">> -> first;
        <<"first">> -> first;
        <<"now">> -> now;  %% Signal to fetch current HLC
        HlcBin ->
            case parse_hlc(HlcBin) of
                {ok, Hlc} -> Hlc;
                {error, invalid_hlc} -> first
            end
    end,
    %% Parse feed type (normal or longpoll)
    Feed = case proplists:get_value(<<"feed">>, Qs) of
        <<"longpoll">> -> longpoll;
        _ -> normal
    end,
    %% Parse other options
    Opts0 = lists:foldl(
        fun({<<"limit">>, LimitBin}, Acc) ->
                Acc#{limit => binary_to_integer(LimitBin)};
           ({<<"include_docs">>, <<"true">>}, Acc) ->
                Acc#{include_docs => true};
           ({<<"descending">>, <<"true">>}, Acc) ->
                Acc#{descending => true};
           ({<<"timeout">>, TimeoutBin}, Acc) ->
                Acc#{timeout => binary_to_integer(TimeoutBin)};
           ({<<"path">>, PathBin}, Acc) ->
                %% Single path converts to paths list for backend
                Acc#{paths => [PathBin]};
           ({<<"doc_ids">>, DocIdsBin}, Acc) ->
                %% Comma-separated doc_ids from query string
                DocIds = binary:split(DocIdsBin, <<",">>, [global]),
                Acc#{doc_ids => DocIds};
           (_, Acc) ->
                Acc
        end,
        #{},
        Qs
    ),
    %% Get filter parameter - will be handled separately
    FilterPattern = proplists:get_value(<<"filter">>, Qs),
    {Since, Feed, FilterPattern, Opts0}.

%% @doc Merge doc_ids and query from POST body into options
merge_body_opts(<<>>, Opts) -> Opts;
merge_body_opts(Body, Opts) ->
    try json:decode(Body) of
        BodyMap when is_map(BodyMap) ->
            Opts1 = case maps:get(<<"doc_ids">>, BodyMap, undefined) of
                undefined -> Opts;
                DocIds when is_list(DocIds) -> Opts#{doc_ids => DocIds};
                _ -> Opts
            end,
            case maps:get(<<"query">>, BodyMap, undefined) of
                undefined -> Opts1;
                Query when is_map(Query) ->
                    %% Use convert_query_spec to normalize the query format
                    Opts1#{query => convert_query_spec(Query)};
                _ -> Opts1
            end;
        _ -> Opts
    catch
        _:_ -> Opts
    end.

%%====================================================================
%% HLC Formatting
%%====================================================================

format_hlc(Hlc) when is_tuple(Hlc) ->
    %% Convert HLC timestamp to string representation
    iolist_to_binary(io_lib:format("~p", [Hlc]));
format_hlc(Hlc) ->
    Hlc.

%% @doc Parse an HLC binary in Erlang term syntax (e.g. <<"{timestamp,1,1}">>).
%% Returns {ok, Term} on a valid {timestamp, W, L} tuple, {error, invalid_hlc}
%% otherwise. Earlier versions of this function returned the sentinel atom
%% `first` on any failure, which laundered bad input into a value the rest
%% of the system treated as legitimate.
parse_hlc(HlcBin) when is_binary(HlcBin) ->
    try
        {ok, Tokens, _} = erl_scan:string(binary_to_list(HlcBin) ++ "."),
        {ok, Term} = erl_parse:parse_term(Tokens),
        case Term of
            {timestamp, W, L} when is_integer(W), is_integer(L),
                                   W >= 0, L >= 0 ->
                {ok, Term};
            _ ->
                {error, invalid_hlc}
        end
    catch
        _:_ -> {error, invalid_hlc}
    end.

%% @doc Resolve 'now' to the current HLC for the database
%% Returns the last sequence number so only future changes are returned.
resolve_since(DbName, now) ->
    %% Get the current last sequence for this database
    case barrel_docdb:db_pid(DbName) of
        {ok, Pid} ->
            case barrel_db_server:get_store_ref(Pid) of
                {ok, StoreRef} ->
                    barrel_changes:get_last_hlc(StoreRef, DbName);
                {error, _} ->
                    first
            end;
        {error, _} ->
            first
    end;
resolve_since(_DbName, Other) ->
    Other.

%%====================================================================
%% API Key Management Handlers
%%====================================================================

handle_list_keys(Req) ->
    {ok, Keys} = barrel_http_api_keys:list_keys(),
    Body = encode_response(Keys, Req),
    {200, response_headers(Req), Body, Req}.

handle_create_key(Req0) ->
    {ReqBody, Req1} = read_full_body(Req0),
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
    KeyPrefix = livery_req:binding(<<"key_prefix">>, Req),
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
    KeyPrefix = livery_req:binding(<<"key_prefix">>, Req),
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
%% Replication Peer Registry Handlers
%%====================================================================

handle_list_peers(Req) ->
    {ok, Peers} = barrel_peer_registry:list(),
    Body = encode_response(Peers, Req),
    {200, response_headers(Req), Body, Req}.

handle_register_peer(Req0) ->
    {ReqBody, Req1} = read_full_body(Req0),
    Spec0 = decode_request_body(ReqBody, Req1),
    %% Translate binary keys to the atom keys
    %% `barrel_peer_registry:register_peer/1' expects.
    Spec = maps:fold(
             fun(<<"name">>, V, Acc)          -> Acc#{name => V};
                (<<"url">>, V, Acc)           -> Acc#{url => V};
                (<<"public_key">>, V, Acc)    -> Acc#{public_key => V};
                (<<"peer_id">>, V, Acc)       -> Acc#{peer_id => V};
                (<<"discover">>, true, Acc)   ->
                     Acc#{discover_from => maps:get(<<"url">>, Spec0, undefined)};
                (<<"databases">>, <<"all">>, Acc) -> Acc#{databases => all};
                (<<"databases">>, V, Acc) when is_list(V) -> Acc#{databases => V};
                (_, _, Acc) -> Acc
             end, #{}, Spec0),
    case barrel_peer_registry:register_peer(Spec) of
        {ok, PeerInfo} ->
            Body = encode_response(PeerInfo, Req1),
            {201, response_headers(Req1), Body, Req1};
        {error, missing_name} ->
            throw({error, 400, <<"missing field: name">>});
        {error, missing_url} ->
            throw({error, 400, <<"missing field: url">>});
        {error, missing_public_key_or_discover} ->
            throw({error, 400, <<"missing field: public_key or discover">>});
        {error, missing_peer_id} ->
            throw({error, 400, <<"missing field: peer_id (required when public_key is supplied directly)">>});
        {error, invalid_public_key} ->
            throw({error, 400, <<"invalid public_key">>});
        {error, well_known_invalid_json} ->
            throw({error, 502, <<"discover: remote /.well-known/barrel did not return JSON">>});
        {error, well_known_missing_fields} ->
            throw({error, 502, <<"discover: remote /.well-known/barrel missing node_id or public_key">>});
        {error, {discover_status, Status}} ->
            throw({error, 502, iolist_to_binary(io_lib:format("discover: remote returned status ~p", [Status]))});
        {error, {discover_failed, _} = R} ->
            throw({error, 502, format_error(R)});
        {error, Reason} ->
            throw({error, 500, format_error(Reason)})
    end.

handle_get_peer(Req) ->
    PeerId = livery_req:binding(<<"peer_id">>, Req),
    case barrel_peer_registry:get(PeerId) of
        {ok, PeerInfo} ->
            Body = encode_response(PeerInfo, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Peer not found">>})
    end.

handle_delete_peer(Req) ->
    PeerId = livery_req:binding(<<"peer_id">>, Req),
    case barrel_peer_registry:delete(PeerId) of
        ok ->
            Body = encode_response(#{<<"ok">> => true}, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Peer not found">>})
    end.

%%====================================================================
%% Admin Usage Handlers
%%====================================================================

%% @doc Get usage statistics for all databases
handle_admin_usage(Req) ->
  {ok, Stats} = barrel_docdb_usage:get_all_usage(),
  Response = #{
               <<"databases">> => Stats,
               <<"total_databases">> => length(Stats)
              },
  Body = encode_response(Response, Req),
  {200, response_headers(Req), Body, Req}.

%% @doc Get usage statistics for a specific database
handle_admin_db_usage(Req) ->
    DbName = livery_req:binding(<<"db">>, Req),
    case barrel_docdb_usage:get_db_usage(DbName) of
        {ok, Stats} ->
            Body = encode_response(Stats, Req),
            {200, response_headers(Req), Body, Req};
        {error, not_found} ->
            throw({error, 404, <<"Database not found">>});
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
%% Node identity
%%====================================================================

%% @doc Build the node identity map served at /.well-known/barrel.
%% Includes the persistent node id, the release version, and (when peer
%% auth is initialized) the Ed25519 public key. No discovery/federation.
node_info_map() ->
    Version = case application:get_key(barrel_docdb, vsn) of
        {ok, Vsn} -> list_to_binary(Vsn);
        undefined -> <<"dev">>
    end,
    Base = #{
        <<"node_id">> => barrel_docdb:node_id(),
        <<"version">> => Version
    },
    case barrel_peer_auth:get_public_key_base64() of
        {ok, PubKeyB64} -> Base#{<<"public_key">> => PubKeyB64};
        _ -> Base
    end.

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
        <<"databases">> => DbHealth
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

