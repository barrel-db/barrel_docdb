%%%-------------------------------------------------------------------
%%% @doc HTTP Changes Stream Handler (Server-Sent Events)
%%%
%%% Provides SSE streaming for changes feed. Clients receive real-time
%%% document change notifications over a persistent HTTP connection.
%%%
%%% Usage:
%%%   GET /db/:db/_changes/stream?since=0&filter=users/+/profile
%%%
%%% Supports:
%%%   - `since` - Start position (0, first, or HLC)
%%%   - `filter` - MQTT-style pattern (uses barrel_sub)
%%%   - `include_docs` - Include full documents
%%%   - `heartbeat` - Heartbeat interval in ms (default: 60000)
%%%
%%% SSE Event format:
%%%   event: change
%%%   data: {"id":"doc1","rev":"1-abc","hlc":"..."}
%%%
%%%   event: heartbeat
%%%   data: {}
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_http_changes_stream).

-export([init/2]).
-export([info/3]).
-export([terminate/3]).

%% Heartbeat every 30 seconds - well below Cowboy's default 60s idle_timeout
-define(DEFAULT_HEARTBEAT_MS, 30000).
-define(POLL_INTERVAL_MS, 100).

-record(state, {
    db_name :: binary(),
    since :: term(),
    filter_fun :: fun((map()) -> boolean()) | undefined,
    filter_opts :: map(),  %% doc_ids, query, paths for backend
    include_docs :: boolean(),
    heartbeat_ms :: pos_integer(),
    heartbeat_ref :: reference() | undefined,
    poll_ref :: reference() | undefined
}).

%%====================================================================
%% Cowboy Loop Handler
%%====================================================================

init(Req0, _State) ->
    DbName = cowboy_req:binding(db, Req0),
    Qs = cowboy_req:parse_qs(Req0),

    %% Verify database exists
    case barrel_docdb:db_info(DbName) of
        {error, not_found} ->
            Req = cowboy_req:reply(404,
                #{<<"content-type">> => <<"application/json">>},
                <<"{\"error\":\"Database not found\"}">>,
                Req0),
            {ok, Req, undefined};
        {ok, _Info} ->
            %% Parse options from query string
            Since = parse_since(proplists:get_value(<<"since">>, Qs, <<"first">>)),
            IncludeDocs = proplists:get_value(<<"include_docs">>, Qs, <<"false">>) =:= <<"true">>,
            HeartbeatMs = parse_heartbeat(proplists:get_value(<<"heartbeat">>, Qs)),
            FilterFun = parse_filter(DbName, proplists:get_value(<<"filter">>, Qs)),

            %% Parse filter options from query string
            FilterOpts0 = parse_filter_opts_from_qs(Qs),

            %% Read POST body for doc_ids and query filters
            {FilterOpts, Req1} = case cowboy_req:method(Req0) of
                <<"POST">> ->
                    {ok, Body, ReqWithBody} = cowboy_req:read_body(Req0),
                    {merge_body_filter_opts(Body, FilterOpts0), ReqWithBody};
                _ ->
                    {FilterOpts0, Req0}
            end,

            %% Start SSE response
            Headers = #{
                <<"content-type">> => <<"text/event-stream">>,
                <<"cache-control">> => <<"no-cache">>,
                <<"connection">> => <<"keep-alive">>,
                <<"access-control-allow-origin">> => <<"*">>
            },
            Req = cowboy_req:stream_reply(200, Headers, Req1),

            %% Initialize state
            State = #state{
                db_name = DbName,
                since = Since,
                filter_fun = FilterFun,
                filter_opts = FilterOpts,
                include_docs = IncludeDocs,
                heartbeat_ms = HeartbeatMs
            },

            %% Schedule first poll and heartbeat
            PollRef = erlang:send_after(?POLL_INTERVAL_MS, self(), poll_changes),
            HbRef = erlang:send_after(HeartbeatMs, self(), heartbeat),

            {cowboy_loop, Req, State#state{poll_ref = PollRef, heartbeat_ref = HbRef}}
    end.

info(poll_changes, Req, #state{db_name = DbName,
                               since = Since,
                               filter_fun = FilterFun,
                               filter_opts = FilterOpts,
                               include_docs = IncludeDocs} = State) ->
    %% Build options: merge filter_opts with include_docs and limit
    BaseOpts = case IncludeDocs of
        true -> #{include_docs => true, limit => 100};
        false -> #{limit => 100}
    end,
    Opts = maps:merge(FilterOpts, BaseOpts),

    case barrel_docdb:get_changes(DbName, Since, Opts) of
        {ok, [], _LastHlc} ->
            %% No changes, reschedule poll
            PollRef = erlang:send_after(?POLL_INTERVAL_MS, self(), poll_changes),
            {ok, Req, State#state{poll_ref = PollRef}};

        {ok, Changes, LastHlc} ->
            %% Filter and send changes
            FilteredChanges = filter_changes(Changes, FilterFun),
            lists:foreach(
                fun(Change) ->
                    send_change_event(Req, Change)
                end,
                FilteredChanges
            ),

            %% Schedule next poll
            PollRef = erlang:send_after(?POLL_INTERVAL_MS, self(), poll_changes),
            {ok, Req, State#state{since = LastHlc, poll_ref = PollRef}};

        {error, _Reason} ->
            %% Database error - close stream
            send_error_event(Req, <<"Database error">>),
            {stop, Req, State}
    end;

info(heartbeat, Req, #state{heartbeat_ms = HeartbeatMs} = State) ->
    %% Send heartbeat to keep connection alive
    send_heartbeat_event(Req),
    HbRef = erlang:send_after(HeartbeatMs, self(), heartbeat),
    {ok, Req, State#state{heartbeat_ref = HbRef}};

info(_Info, Req, State) ->
    {ok, Req, State}.

terminate(_Reason, _Req, #state{heartbeat_ref = HbRef, poll_ref = PollRef}) ->
    %% Cancel timers
    cancel_timer(HbRef),
    cancel_timer(PollRef),
    ok;
terminate(_Reason, _Req, _State) ->
    ok.

%%====================================================================
%% SSE Event Helpers
%%====================================================================

send_change_event(Req, Change) ->
    Data = format_change_json(Change),
    Event = iolist_to_binary([
        <<"event: change\n">>,
        <<"data: ">>, Data, <<"\n\n">>
    ]),
    cowboy_req:stream_body(Event, nofin, Req).

send_heartbeat_event(Req) ->
    Event = <<"event: heartbeat\ndata: {}\n\n">>,
    cowboy_req:stream_body(Event, nofin, Req).

send_error_event(Req, Message) ->
    Data = iolist_to_binary(json:encode(#{<<"error">> => Message})),
    Event = iolist_to_binary([
        <<"event: error\n">>,
        <<"data: ">>, Data, <<"\n\n">>
    ]),
    cowboy_req:stream_body(Event, fin, Req).

%%====================================================================
%% Internal Functions
%%====================================================================

parse_since(<<"0">>) -> first;
parse_since(<<"first">>) -> first;
parse_since(<<"now">>) ->
    %% "now" means start from current position - get the latest HLC
    %% Note: This returns 'first' as fallback; the caller should handle
    %% this by fetching the latest sequence before starting the stream
    first;
parse_since(HlcBin) when is_binary(HlcBin) ->
    %% Try to parse as base64-encoded binary (our standard format)
    try base64:decode(HlcBin) of
        Decoded when byte_size(Decoded) =:= 12 ->
            %% Valid HLC binary (12 bytes: 8 wall_time + 4 logical)
            Decoded;
        _ ->
            %% Invalid size, fall back
            first
    catch
        _:_ ->
            %% Not valid base64, default to first
            first
    end.

parse_heartbeat(undefined) -> ?DEFAULT_HEARTBEAT_MS;
parse_heartbeat(Bin) when is_binary(Bin) ->
    try binary_to_integer(Bin) of
        N when N >= 1000 -> N;  %% Minimum 1 second
        _ -> ?DEFAULT_HEARTBEAT_MS
    catch
        _:_ -> ?DEFAULT_HEARTBEAT_MS
    end.

parse_filter(_DbName, undefined) -> undefined;
parse_filter(_DbName, Pattern) when is_binary(Pattern) ->
    %% Validate pattern
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

filter_changes(Changes, undefined) -> Changes;
filter_changes(Changes, FilterFun) ->
    lists:filter(FilterFun, Changes).

format_change_json(Change) when is_map(Change) ->
    %% Format HLC for JSON
    FormattedChange = maps:map(
        fun(hlc, V) -> format_hlc(V);
           (<<"hlc">>, V) -> format_hlc(V);
           (_, V) -> V
        end,
        Change
    ),
    iolist_to_binary(json:encode(FormattedChange)).

format_hlc(Hlc) when is_binary(Hlc) ->
    base64:encode(Hlc);
format_hlc(Hlc) when is_tuple(Hlc) ->
    iolist_to_binary(io_lib:format("~p", [Hlc]));
format_hlc(Other) ->
    Other.

cancel_timer(undefined) -> ok;
cancel_timer(Ref) -> erlang:cancel_timer(Ref).

%% @doc Parse filter options from query string (path, doc_ids)
parse_filter_opts_from_qs(Qs) ->
    Opts0 = case proplists:get_value(<<"path">>, Qs) of
        undefined -> #{};
        Path -> #{paths => [Path]}
    end,
    case proplists:get_value(<<"doc_ids">>, Qs) of
        undefined -> Opts0;
        DocIdsBin ->
            %% Comma-separated doc_ids
            DocIds = binary:split(DocIdsBin, <<",">>, [global]),
            Opts0#{doc_ids => DocIds}
    end.

%% @doc Merge doc_ids and query from POST body into filter options
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
        _ -> Opts
    catch
        _:_ -> Opts
    end.

%% @doc Convert JSON query spec to internal format
%% Converts binary keys to atoms and normalizes the structure
convert_query_spec(Spec) when is_map(Spec) ->
    BaseSpec = maps:fold(
        fun(<<"where">>, V, Acc) ->
                Acc#{where => convert_where_clauses(V)};
           (<<"selector">>, V, Acc) when is_map(V) ->
                Acc#{<<"selector">> => V};
           (<<"limit">>, V, Acc) when is_integer(V) ->
                Acc#{limit => V};
           (<<"offset">>, V, Acc) when is_integer(V) ->
                Acc#{offset => V};
           (_, _, Acc) ->
                Acc
        end,
        #{},
        Spec
    ),
    %% Ensure where clause is present
    case maps:is_key(where, BaseSpec) orelse maps:is_key(<<"selector">>, BaseSpec) of
        true -> BaseSpec;
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
convert_where_clause(#{<<"path">> := Path, <<"op">> := <<"in">>, <<"value">> := Value}) when is_list(Value) ->
    {in, Path, Value};
convert_where_clause(_) ->
    {error, invalid_clause}.

convert_op(<<"ne">>) -> '=/=';
convert_op(<<"gt">>) -> '>';
convert_op(<<"gte">>) -> '>=';
convert_op(<<"lt">>) -> '<';
convert_op(<<"lte">>) -> '=<';
convert_op(<<"==">>) -> '==';
convert_op(Op) when is_binary(Op) -> binary_to_atom(Op);
convert_op(Op) -> Op.
