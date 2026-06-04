%%%-------------------------------------------------------------------
%%% @doc HTTP Changes Stream Handler (Server-Sent Events).
%%%
%%% Provides SSE streaming for the changes feed. Clients receive
%%% real-time document-change notifications over a persistent HTTP
%%% connection.
%%%
%%% Endpoint: `GET /db/:db/_changes/stream?since=0&filter=users/+/profile'
%%%
%%% Query params: `since', `filter', `path', `doc_ids', `include_docs',
%%% `heartbeat'. A POST body may carry `doc_ids' and a `query' filter
%%% (mirrors the JSON-mode of `_changes').
%%%
%%% SSE frames:
%%% ```
%%% event: change
%%% data: {"id":"doc1","rev":"1-abc","hlc":"..."}
%%%
%%% event: heartbeat
%%% data: {}
%%% '''
%%%
%%% Runs over livery: the handler returns a `livery_resp:sse/3' value
%%% whose producer closure owns the poll/heartbeat loop.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_http_changes_stream).

-export([handle/1]).

-define(DEFAULT_HEARTBEAT_MS, 30000).
-define(POLL_INTERVAL_MS, 100).

-record(stream_state, {
    db_name :: binary(),
    since :: term(),
    filter_fun :: fun((map()) -> boolean()) | undefined,
    filter_opts :: map(),
    include_docs :: boolean(),
    heartbeat_ms :: pos_integer(),
    next_heartbeat_ms :: integer()
}).

%%====================================================================
%% Entry
%%====================================================================

handle(Req) ->
    DbName = livery_req:binding(<<"db">>, Req),
    case barrel_docdb:db_info(DbName) of
        {error, not_found} ->
            livery_resp:json(404, <<"{\"error\":\"Database not found\"}">>);
        {ok, _Info} ->
            start_stream(DbName, Req)
    end.

start_stream(DbName, Req) ->
    Qs = parse_qs(Req),
    Since0 = parse_since(proplists:get_value(<<"since">>, Qs, <<"first">>)),
    Since = resolve_since(DbName, Since0),
    IncludeDocs =
        proplists:get_value(<<"include_docs">>, Qs, <<"false">>) =:= <<"true">>,
    HeartbeatMs = parse_heartbeat(proplists:get_value(<<"heartbeat">>, Qs)),

    FilterOpts0 = parse_filter_opts_from_qs(Qs),
    FilterPattern = proplists:get_value(<<"filter">>, Qs),
    {FilterOpts1, FilterFun} =
        case FilterPattern of
            undefined ->
                {FilterOpts0, undefined};
            Pattern ->
                case needs_client_side_filter(Pattern) of
                    true  -> {FilterOpts0, create_filter_fun(Pattern)};
                    false -> {FilterOpts0#{paths => [Pattern]}, undefined}
                end
        end,

    FilterOpts =
        case livery_req:method(Req) of
            <<"POST">> ->
                Body = read_body(Req),
                merge_body_filter_opts(Body, FilterOpts1);
            _ ->
                FilterOpts1
        end,

    Now = monotonic_ms(),
    State = #stream_state{
        db_name = DbName,
        since = Since,
        filter_fun = FilterFun,
        filter_opts = FilterOpts,
        include_docs = IncludeDocs,
        heartbeat_ms = HeartbeatMs,
        next_heartbeat_ms = Now + HeartbeatMs
    },
    Headers = [
        {<<"cache-control">>, <<"no-cache">>},
        {<<"access-control-allow-origin">>, <<"*">>}
    ],
    livery_resp:sse(200, Headers, fun(Emit) -> sse_loop(Emit, State) end).

%%====================================================================
%% Producer loop
%%====================================================================

%% The producer runs in the per-request process. Each iteration:
%%   1. Poll once; emit any new changes.
%%   2. If the heartbeat deadline has passed, emit a heartbeat.
%%   3. Sleep `min(POLL_INTERVAL_MS, time-until-heartbeat)`.
%%   4. Repeat unless the client disconnected.
sse_loop(Emit, State0) ->
    case poll_and_emit(Emit, State0) of
        {stop, _Reason} ->
            ok;
        {ok, State1} ->
            State2 = maybe_heartbeat(Emit, State1),
            SleepMs = sleep_ms(State2),
            timer:sleep(SleepMs),
            sse_loop(Emit, State2)
    end.

poll_and_emit(Emit, #stream_state{db_name = DbName,
                                   since = Since,
                                   filter_fun = FilterFun,
                                   filter_opts = FilterOpts,
                                   include_docs = IncludeDocs} = State) ->
    BaseOpts = case IncludeDocs of
        true  -> #{include_docs => true, limit => 100};
        false -> #{limit => 100}
    end,
    Opts = maps:merge(FilterOpts, BaseOpts),
    try barrel_docdb:get_changes(DbName, Since, Opts) of
        {ok, [], _LastHlc} ->
            {ok, State};
        {ok, Changes, LastHlc} ->
            Filtered = filter_changes(Changes, FilterFun),
            case emit_changes(Emit, Filtered) of
                ok          -> {ok, State#stream_state{since = LastHlc}};
                {error, _} = E -> {stop, E}
            end
    catch
        _:_ ->
            _ = emit_error(Emit, <<"Database error">>),
            {stop, db_error}
    end.

emit_changes(_Emit, []) ->
    ok;
emit_changes(Emit, [Change | Rest]) ->
    Data = format_change_json(Change),
    case Emit(#{event => <<"change">>, data => Data}) of
        ok          -> emit_changes(Emit, Rest);
        {error, _} = E -> E
    end.

emit_error(Emit, Message) ->
    Data = iolist_to_binary(json:encode(#{<<"error">> => Message})),
    Emit(#{event => <<"error">>, data => Data}).

maybe_heartbeat(Emit, #stream_state{next_heartbeat_ms = Deadline,
                                     heartbeat_ms = HbMs} = State) ->
    Now = monotonic_ms(),
    case Now >= Deadline of
        true ->
            _ = Emit(#{event => <<"heartbeat">>, data => <<"{}">>}),
            State#stream_state{next_heartbeat_ms = Now + HbMs};
        false ->
            State
    end.

sleep_ms(#stream_state{next_heartbeat_ms = Deadline}) ->
    TimeUntilHeartbeat = max(0, Deadline - monotonic_ms()),
    min(?POLL_INTERVAL_MS, max(1, TimeUntilHeartbeat)).

monotonic_ms() ->
    erlang:monotonic_time(millisecond).

%%====================================================================
%% Request helpers (livery)
%%====================================================================

parse_qs(Req) ->
    case livery_req:query(Req) of
        <<>> -> [];
        Raw  -> uri_string:dissect_query(Raw)
    end.

read_body(Req) ->
    case livery_req:body(Req) of
        empty -> <<>>;
        {buffered, IoData} -> iolist_to_binary(IoData);
        {stream, Reader} ->
            case livery_body:read_all(Reader) of
                {ok, Bin, _} -> Bin;
                {error, _, _} -> <<>>
            end
    end.

%%====================================================================
%% Change formatting
%%====================================================================

format_change_json(Change) when is_map(Change) ->
    Formatted = maps:map(
        fun(hlc, V)          -> format_hlc(V);
           (<<"hlc">>, V)    -> format_hlc(V);
           (_, V)            -> V
        end,
        Change),
    iolist_to_binary(json:encode(Formatted)).

format_hlc(Hlc) when is_binary(Hlc) -> base64:encode(Hlc);
format_hlc(Hlc) when is_tuple(Hlc) -> iolist_to_binary(io_lib:format("~p", [Hlc]));
format_hlc(Other) -> Other.

%%====================================================================
%% Since / heartbeat parsing
%%====================================================================

parse_since(<<"0">>)     -> first;
parse_since(<<"first">>) -> first;
parse_since(<<"now">>)   -> now;
parse_since(HlcBin) when is_binary(HlcBin) ->
    try base64:decode(HlcBin) of
        Decoded when byte_size(Decoded) =:= 12 -> Decoded;
        _ -> first
    catch
        _:_ -> first
    end.

parse_heartbeat(undefined) ->
    ?DEFAULT_HEARTBEAT_MS;
parse_heartbeat(Bin) when is_binary(Bin) ->
    try binary_to_integer(Bin) of
        N when N >= 1000 -> N;
        _ -> ?DEFAULT_HEARTBEAT_MS
    catch
        _:_ -> ?DEFAULT_HEARTBEAT_MS
    end.

resolve_since(DbName, now) ->
    case barrel_docdb:db_pid(DbName) of
        {ok, Pid} ->
            case barrel_db_server:get_store_ref(Pid) of
                {ok, StoreRef} -> barrel_changes:get_last_hlc(StoreRef, DbName);
                {error, _}     -> first
            end;
        {error, _} ->
            first
    end;
resolve_since(_DbName, Other) ->
    Other.

%%====================================================================
%% Filter pattern (MQTT-style with `+`/`#`)
%%====================================================================

needs_client_side_filter(Pattern) when is_binary(Pattern) ->
    binary:match(Pattern, <<"+">>) =/= nomatch.

create_filter_fun(Pattern) when is_binary(Pattern) ->
    case match_trie:validate({filter, Pattern}) of
        true ->
            PatternWords = pattern_to_words(Pattern),
            fun(Change) ->
                DocId = maps:get(<<"id">>, Change, maps:get(id, Change, <<>>)),
                DocWords = binary:split(DocId, <<"/">>, [global]),
                match_mqtt_pattern(PatternWords, DocWords)
            end;
        false ->
            undefined
    end.

pattern_to_words(Pattern) ->
    [pattern_word(W) || W <- binary:split(Pattern, <<"/">>, [global])].

pattern_word(<<"+">>) -> '+';
pattern_word(<<"#">>) -> '#';
pattern_word(W)        -> W.

match_mqtt_pattern([], []) -> true;
match_mqtt_pattern(['#'], _) -> true;
match_mqtt_pattern(['+' | PRest], [_ | DRest]) -> match_mqtt_pattern(PRest, DRest);
match_mqtt_pattern([P | PRest], [P | DRest])   -> match_mqtt_pattern(PRest, DRest);
match_mqtt_pattern(_, _) -> false.

filter_changes(Changes, undefined) -> Changes;
filter_changes(Changes, FilterFun) -> lists:filter(FilterFun, Changes).

%%====================================================================
%% Query-string / body filter parsing
%%====================================================================

parse_filter_opts_from_qs(Qs) ->
    Opts0 =
        case proplists:get_value(<<"path">>, Qs) of
            undefined -> #{};
            Path      -> #{paths => [Path]}
        end,
    case proplists:get_value(<<"doc_ids">>, Qs) of
        undefined -> Opts0;
        DocIdsBin ->
            DocIds = binary:split(DocIdsBin, <<",">>, [global]),
            Opts0#{doc_ids => DocIds}
    end.

merge_body_filter_opts(<<>>, Opts) -> Opts;
merge_body_filter_opts(Body, Opts) ->
    try json:decode(Body) of
        BodyMap when is_map(BodyMap) ->
            Opts1 = case maps:get(<<"doc_ids">>, BodyMap, undefined) of
                undefined -> Opts;
                DocIds when is_list(DocIds) -> Opts#{doc_ids => DocIds};
                _ -> Opts
            end,
            case maps:get(<<"query">>, BodyMap, undefined) of
                undefined -> Opts1;
                Query when is_map(Query) -> Opts1#{query => convert_query_spec(Query)};
                _ -> Opts1
            end;
        _ ->
            Opts
    catch
        _:_ -> Opts
    end.

convert_query_spec(Spec) when is_map(Spec) ->
    BaseSpec = maps:fold(
        fun(<<"where">>, V, Acc)               -> Acc#{where => convert_where_clauses(V)};
           (<<"selector">>, V, Acc) when is_map(V)     -> Acc#{<<"selector">> => V};
           (<<"limit">>, V, Acc)    when is_integer(V) -> Acc#{limit => V};
           (<<"offset">>, V, Acc)   when is_integer(V) -> Acc#{offset => V};
           (_, _, Acc)                          -> Acc
        end,
        #{},
        Spec),
    case maps:is_key(where, BaseSpec) orelse maps:is_key(<<"selector">>, BaseSpec) of
        true  -> BaseSpec;
        false -> BaseSpec#{where => [{exists, [<<"id">>]}]}
    end.

convert_where_clauses(Clauses) when is_list(Clauses) ->
    lists:map(fun convert_where_clause/1, Clauses);
convert_where_clauses(_) ->
    [].

convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"eq">>, <<"value">> := Value}) ->
    {path, Path, Value};
convert_where_clause(#{<<"path">> := Path, <<"op">> := Op, <<"value">> := Value}) ->
    {compare, Path, convert_op(Op), Value};
convert_where_clause(#{<<"path">> := Path, <<"value">> := Value}) ->
    {path, Path, Value};
convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"exists">>}) ->
    {exists, Path};
convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"missing">>}) ->
    {missing, Path};
convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"in">>, <<"value">> := Value})
        when is_list(Value) ->
    {in, Path, Value};
convert_where_clause(_) ->
    {error, invalid_clause}.

%% Whitelist-only; matches barrel_http_handler:convert_op/1. Unknown
%% ops cause the change-feed query to be ignored (the per-change
%% filter is best-effort).
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
convert_op(Op) when is_atom(Op) -> Op;
convert_op(_) -> '=='.
