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

-define(DEFAULT_HEARTBEAT_MS, 60000).
-define(POLL_INTERVAL_MS, 100).

-record(state, {
    db_name :: binary(),
    since :: term(),
    filter_fun :: fun((map()) -> boolean()) | undefined,
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
            %% Parse options
            Since = parse_since(proplists:get_value(<<"since">>, Qs, <<"first">>)),
            IncludeDocs = proplists:get_value(<<"include_docs">>, Qs, <<"false">>) =:= <<"true">>,
            HeartbeatMs = parse_heartbeat(proplists:get_value(<<"heartbeat">>, Qs)),
            FilterFun = parse_filter(DbName, proplists:get_value(<<"filter">>, Qs)),

            %% Start SSE response
            Headers = #{
                <<"content-type">> => <<"text/event-stream">>,
                <<"cache-control">> => <<"no-cache">>,
                <<"connection">> => <<"keep-alive">>,
                <<"access-control-allow-origin">> => <<"*">>
            },
            Req = cowboy_req:stream_reply(200, Headers, Req0),

            %% Initialize state
            State = #state{
                db_name = DbName,
                since = Since,
                filter_fun = FilterFun,
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
                               include_docs = IncludeDocs} = State) ->
    %% Get changes since last position
    Opts = case IncludeDocs of
        true -> #{include_docs => true, limit => 100};
        false -> #{limit => 100}
    end,

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
parse_since(HlcBin) when is_binary(HlcBin) ->
    %% Try to parse as base64-encoded binary (our standard format)
    try base64:decode(HlcBin) of
        Decoded -> Decoded
    catch
        _:_ ->
            %% Try to parse as Erlang term
            try
                {ok, Tokens, _} = erl_scan:string(binary_to_list(HlcBin) ++ "."),
                {ok, Term} = erl_parse:parse_term(Tokens),
                Term
            catch
                _:_ -> first
            end
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
