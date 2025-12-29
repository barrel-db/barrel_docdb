%%%-------------------------------------------------------------------
%%% @doc barrel_rep - Replication API for barrel_docdb
%%%
%%% This module provides the public API for replicating documents
%%% between barrel_docdb databases. Supports both one-shot and
%%% continuous replication.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_rep).

-include("barrel_docdb.hrl").

%% API
-export([
    replicate/2,
    replicate/3,
    replicate_one_shot/1,
    replicate_one_shot/2
]).

%% Types
-type rep_config() :: #{
    source := term(),
    target := term(),
    source_transport => module(),
    target_transport => module(),
    batch_size => pos_integer(),
    checkpoint_size => pos_integer()
}.

-type rep_result() :: #{
    ok := boolean(),
    docs_read := non_neg_integer(),
    docs_written := non_neg_integer(),
    doc_read_failures := non_neg_integer(),
    doc_write_failures := non_neg_integer(),
    start_seq := seq() | first,
    last_seq := seq() | first
}.

-export_type([rep_config/0, rep_result/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Replicate from source to target (one-shot)
%% Shorthand that uses local transport for both endpoints
-spec replicate(binary(), binary()) -> {ok, rep_result()} | {error, term()}.
replicate(Source, Target) ->
    replicate(Source, Target, #{}).

%% @doc Replicate from source to target with options (one-shot)
-spec replicate(binary(), binary(), map()) -> {ok, rep_result()} | {error, term()}.
replicate(Source, Target, Opts) ->
    Config = #{
        source => Source,
        target => Target,
        source_transport => maps:get(source_transport, Opts, barrel_rep_transport_local),
        target_transport => maps:get(target_transport, Opts, barrel_rep_transport_local)
    },
    replicate_one_shot(Config, Opts).

%% @doc One-shot replication with full config
-spec replicate_one_shot(rep_config(), map()) -> {ok, rep_result()} | {error, term()}.
replicate_one_shot(Config, Opts) ->
    #{
        source := Source,
        target := Target,
        source_transport := SourceTransport,
        target_transport := TargetTransport
    } = Config,

    %% Generate replication ID
    RepId = generate_rep_id(Source, Target),

    %% Create checkpoint state
    CheckpointConfig = Config#{
        id => RepId,
        options => Opts
    },
    Checkpoint = barrel_rep_checkpoint:new(CheckpointConfig),

    %% Get starting sequence
    StartSeq = barrel_rep_checkpoint:get_start_seq(Checkpoint),

    %% Run replication
    BatchSize = maps:get(batch_size, Opts, 100),
    CheckpointSize = maps:get(checkpoint_size, Opts, 10),

    case do_replicate(Source, Target, SourceTransport, TargetTransport,
                      StartSeq, BatchSize, CheckpointSize, Checkpoint) of
        {ok, Stats, FinalCheckpoint} ->
            %% Write final checkpoint
            ok = barrel_rep_checkpoint:write_checkpoint(FinalCheckpoint),

            %% Build result
            Result = Stats#{
                ok => true,
                start_seq => StartSeq,
                last_seq => barrel_rep_checkpoint:get_last_seq(FinalCheckpoint)
            },
            {ok, Result};
        {error, _} = Error ->
            Error
    end.

%% @doc Replicate one-shot with config only (uses default options)
-spec replicate_one_shot(rep_config()) -> {ok, rep_result()} | {error, term()}.
replicate_one_shot(Config) ->
    replicate_one_shot(Config, #{}).

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Run replication loop
do_replicate(Source, Target, SourceTransport, TargetTransport, Since,
             BatchSize, CheckpointSize, Checkpoint) ->
    do_replicate(Source, Target, SourceTransport, TargetTransport, Since,
                 BatchSize, CheckpointSize, Checkpoint, new_stats(), 0).

do_replicate(Source, Target, SourceTransport, TargetTransport, Since,
             BatchSize, CheckpointSize, Checkpoint, AccStats, DocsProcessed) ->
    %% Get next batch of changes
    case SourceTransport:get_changes(Source, Since, #{limit => BatchSize}) of
        {ok, [], _LastSeq} ->
            %% No more changes
            {ok, AccStats, Checkpoint};

        {ok, Changes, LastSeq} ->
            %% Replicate this batch
            {ok, BatchStats} = barrel_rep_alg:replicate(
                Source, Target, SourceTransport, TargetTransport, Changes
            ),

            %% Merge stats
            MergedStats = merge_stats(AccStats, BatchStats),

            %% Update checkpoint
            Checkpoint2 = barrel_rep_checkpoint:set_last_seq(LastSeq, Checkpoint),
            NewDocsProcessed = DocsProcessed + length(Changes),

            %% Maybe write checkpoint
            Checkpoint3 = case NewDocsProcessed >= CheckpointSize of
                true ->
                    barrel_rep_checkpoint:maybe_write_checkpoint(Checkpoint2);
                false ->
                    Checkpoint2
            end,

            %% Continue with next batch
            do_replicate(Source, Target, SourceTransport, TargetTransport, LastSeq,
                         BatchSize, CheckpointSize, Checkpoint3, MergedStats,
                         NewDocsProcessed rem CheckpointSize);

        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Generate unique replication ID
generate_rep_id(Source, Target) ->
    Data = term_to_binary({Source, Target, erlang:monotonic_time()}),
    Hash = crypto:hash(md5, Data),
    binary:encode_hex(Hash, lowercase).

%% @doc Create new stats map
new_stats() ->
    #{
        docs_read => 0,
        doc_read_failures => 0,
        docs_written => 0,
        doc_write_failures => 0
    }.

%% @doc Merge two stats maps
merge_stats(Stats1, Stats2) ->
    maps:merge_with(fun(_K, V1, V2) -> V1 + V2 end, Stats1, Stats2).
