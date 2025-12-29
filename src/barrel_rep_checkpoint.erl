%%%-------------------------------------------------------------------
%%% @doc barrel_rep_checkpoint - Checkpoint management for replication
%%%
%%% Manages replication checkpoints, which track the progress of
%%% replication between source and target databases. Checkpoints
%%% are stored as local documents (not replicated).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep_checkpoint).

-include("barrel_docdb.hrl").

%% API
-export([
    new/1,
    get_start_seq/1,
    get_last_seq/1,
    set_last_seq/2,
    maybe_write_checkpoint/1,
    write_checkpoint/1,
    delete/1
]).

%% Internal API for reading checkpoints
-export([
    read_checkpoint_doc/3
]).

-record(checkpoint, {
    rep_id :: binary(),
    session_id :: binary(),
    source :: term(),
    target :: term(),
    source_transport :: module(),
    target_transport :: module(),
    start_seq :: seq(),
    last_seq :: seq(),
    target_seq :: seq() | undefined,
    options :: map()
}).

-opaque checkpoint() :: #checkpoint{}.
-export_type([checkpoint/0]).

-define(CHECKPOINT_SIZE, 10).
-define(MAX_CHECKPOINT_HISTORY, 20).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new checkpoint state
-spec new(map()) -> checkpoint().
new(RepConfig) ->
    #{
        id := RepId,
        source := Source,
        target := Target,
        source_transport := SourceTransport,
        target_transport := TargetTransport
    } = RepConfig,

    Options = maps:get(options, RepConfig, #{}),

    %% Get start sequence from existing checkpoints
    StartSeq = checkpoint_start_seq(Source, Target, SourceTransport, TargetTransport, RepId),

    Checkpoint = #checkpoint{
        rep_id = RepId,
        session_id = generate_session_id(),
        source = Source,
        target = Target,
        source_transport = SourceTransport,
        target_transport = TargetTransport,
        start_seq = StartSeq,
        last_seq = StartSeq,
        options = Options
    },
    set_next_target_seq(Checkpoint).

%% @doc Get the starting sequence for this replication session
-spec get_start_seq(checkpoint()) -> seq().
get_start_seq(#checkpoint{start_seq = Seq}) ->
    Seq.

%% @doc Get the last processed sequence
-spec get_last_seq(checkpoint()) -> seq().
get_last_seq(#checkpoint{last_seq = Seq}) ->
    Seq.

%% @doc Set the last processed sequence
-spec set_last_seq(seq(), checkpoint()) -> checkpoint().
set_last_seq(Seq, Checkpoint) ->
    Checkpoint#checkpoint{last_seq = Seq}.

%% @doc Check if checkpoint should be written and write it if needed
-spec maybe_write_checkpoint(checkpoint()) -> checkpoint().
maybe_write_checkpoint(#checkpoint{last_seq = LastSeq, target_seq = TargetSeq} = Checkpoint)
  when LastSeq >= TargetSeq ->
    ok = write_checkpoint(Checkpoint),
    set_next_target_seq(Checkpoint);
maybe_write_checkpoint(Checkpoint) ->
    Checkpoint.

%% @doc Write checkpoint to both source and target databases
-spec write_checkpoint(checkpoint()) -> ok.
write_checkpoint(#checkpoint{
    rep_id = RepId,
    session_id = SessionId,
    source = Source,
    target = Target,
    source_transport = SourceTransport,
    target_transport = TargetTransport,
    start_seq = StartSeq,
    last_seq = LastSeq,
    options = Options
}) ->
    HistorySize = maps:get(checkpoint_max_history, Options, ?MAX_CHECKPOINT_HISTORY),

    Checkpoint = #{
        <<"source_last_seq">> => encode_seq(LastSeq),
        <<"source_start_seq">> => encode_seq(StartSeq),
        <<"session_id">> => SessionId,
        <<"end_time">> => timestamp(),
        <<"end_time_microsec">> => erlang:system_time(microsecond)
    },

    %% Write to both source and target
    _ = add_checkpoint(Source, SourceTransport, RepId, SessionId, HistorySize, Checkpoint),
    _ = add_checkpoint(Target, TargetTransport, RepId, SessionId, HistorySize, Checkpoint),
    ok.

%% @doc Delete checkpoints from both databases
-spec delete(checkpoint()) -> ok.
delete(#checkpoint{
    rep_id = RepId,
    source = Source,
    target = Target,
    source_transport = SourceTransport,
    target_transport = TargetTransport
}) ->
    DocId = checkpoint_docid(RepId),
    _ = SourceTransport:delete_local_doc(Source, DocId),
    _ = TargetTransport:delete_local_doc(Target, DocId),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Add a checkpoint entry to history
add_checkpoint(Db, Transport, RepId, SessionId, HistorySize, Checkpoint) ->
    DocId = checkpoint_docid(RepId),
    Doc = case Transport:get_local_doc(Db, DocId) of
        {ok, #{<<"history">> := H} = PreviousDoc} ->
            H2 = update_history(H, SessionId, HistorySize, Checkpoint),
            PreviousDoc#{<<"history">> => H2};
        {error, not_found} ->
            #{<<"history">> => [Checkpoint]}
    end,
    Transport:put_local_doc(Db, DocId, Doc).

%% @doc Update checkpoint history
update_history(History, SessionId, HistorySize, Checkpoint) ->
    case History of
        [#{<<"session_id">> := SessionId} | Rest] ->
            %% Same session - replace last entry
            [Checkpoint | Rest];
        _ ->
            %% New session - add to history, trim if needed
            NewHistory = [Checkpoint | History],
            case length(NewHistory) > HistorySize of
                true ->
                    lists:sublist(NewHistory, HistorySize);
                false ->
                    NewHistory
            end
    end.

%% @doc Compute replication starting sequence from checkpoints
checkpoint_start_seq(Source, Target, SourceTransport, TargetTransport, RepId) ->
    LastSeqSource = read_last_seq(Source, SourceTransport, RepId),
    LastSeqTarget = read_last_seq(Target, TargetTransport, RepId),
    min_seq(LastSeqSource, LastSeqTarget).

%% @doc Read last sequence from checkpoint
read_last_seq(Db, Transport, RepId) ->
    case read_checkpoint_doc(Db, Transport, RepId) of
        {ok, Doc} ->
            History = maps:get(<<"history">>, Doc, []),
            case History of
                [] ->
                    first;
                _ ->
                    %% Sort by end_time_microsec descending
                    Sorted = lists:sort(
                        fun(H1, H2) ->
                            T1 = maps:get(<<"end_time_microsec">>, H1, 0),
                            T2 = maps:get(<<"end_time_microsec">>, H2, 0),
                            T1 > T2
                        end,
                        History
                    ),
                    LastHistory = hd(Sorted),
                    decode_seq(maps:get(<<"source_last_seq">>, LastHistory))
            end;
        {error, not_found} ->
            first
    end.

%% @doc Read checkpoint document
-spec read_checkpoint_doc(term(), module(), binary()) ->
    {ok, map()} | {error, not_found}.
read_checkpoint_doc(Db, Transport, RepId) ->
    Transport:get_local_doc(Db, checkpoint_docid(RepId)).

%% @doc Set next target sequence for checkpoint writing
set_next_target_seq(#checkpoint{last_seq = LastSeq, options = Options} = Checkpoint) ->
    CheckpointSize = maps:get(checkpoint_size, Options, ?CHECKPOINT_SIZE),
    TargetSeq = case LastSeq of
        first -> barrel_sequence:min_seq();
        {Epoch, Counter} -> {Epoch, Counter + CheckpointSize}
    end,
    Checkpoint#checkpoint{target_seq = TargetSeq}.

%% @doc Generate checkpoint document ID
checkpoint_docid(RepId) ->
    <<"replication-checkpoint-", RepId/binary>>.

%% @doc Generate unique session ID
generate_session_id() ->
    Rand = crypto:strong_rand_bytes(16),
    base64:encode(Rand).

%% @doc RFC3339 timestamp
timestamp() ->
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:local_time(),
    iolist_to_binary(
        io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ",
                      [Year, Month, Day, Hour, Min, Sec])
    ).

%% @doc Encode sequence for storage
encode_seq(first) -> <<"first">>;
encode_seq({Epoch, Counter}) ->
    iolist_to_binary(io_lib:format("~w:~w", [Epoch, Counter])).

%% @doc Decode sequence from storage
decode_seq(<<"first">>) -> first;
decode_seq(Bin) when is_binary(Bin) ->
    case binary:split(Bin, <<":">>) of
        [EpochBin, CounterBin] ->
            {binary_to_integer(EpochBin), binary_to_integer(CounterBin)};
        _ ->
            first
    end.

%% @doc Get minimum of two sequences
min_seq(first, _) -> first;
min_seq(_, first) -> first;
min_seq({E1, C1} = S1, {E2, C2} = S2) ->
    case {E1, C1} =< {E2, C2} of
        true -> S1;
        false -> S2
    end.
