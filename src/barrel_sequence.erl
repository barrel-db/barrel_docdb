%%%-------------------------------------------------------------------
%%% @doc Sequence number utilities for barrel_docdb
%%%
%%% Sequences are used to track changes in the database. Each change
%%% gets a monotonically increasing sequence number within its epoch.
%%% Format: {Epoch, Counter} where both are non-negative integers.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_sequence).

-include("barrel_docdb.hrl").

%% API
-export([
    new/0,
    new/1,
    inc/1,
    compare/2
]).

%% Encoding/Decoding
-export([
    encode/1,
    decode/1,
    to_string/1,
    from_string/1
]).

%% Constants
-export([
    min_seq/0,
    max_seq/0
]).

%%====================================================================
%% Types
%%====================================================================

%% Sequence is already defined in barrel_docdb.hrl as:
%% -type seq() :: {Epoch :: non_neg_integer(), Counter :: non_neg_integer()}.

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new sequence starting at {0, 0}
-spec new() -> seq().
new() ->
    {0, 0}.

%% @doc Create a new sequence with specified epoch
-spec new(non_neg_integer()) -> seq().
new(Epoch) ->
    {Epoch, 0}.

%% @doc Increment sequence counter
-spec inc(seq()) -> seq().
inc({Epoch, Counter}) ->
    {Epoch, Counter + 1}.

%% @doc Compare two sequences
%% Returns: -1 if Seq1 < Seq2, 0 if equal, 1 if Seq1 > Seq2
-spec compare(seq(), seq()) -> -1 | 0 | 1.
compare({E1, _}, {E2, _}) when E1 < E2 -> -1;
compare({E1, _}, {E2, _}) when E1 > E2 -> 1;
compare({_, C1}, {_, C2}) when C1 < C2 -> -1;
compare({_, C1}, {_, C2}) when C1 > C2 -> 1;
compare(_, _) -> 0.

%%====================================================================
%% Encoding/Decoding
%%====================================================================

%% @doc Encode sequence to binary (big-endian for sort order)
-spec encode(seq()) -> binary().
encode({Epoch, Counter}) when is_integer(Epoch), is_integer(Counter),
                              Epoch >= 0, Counter >= 0 ->
    <<Epoch:32/big-unsigned, Counter:32/big-unsigned>>;
encode(_) ->
    erlang:error(badarg).

%% @doc Decode binary to sequence
-spec decode(binary()) -> seq().
decode(<<Epoch:32/big-unsigned, Counter:32/big-unsigned>>) ->
    {Epoch, Counter};
decode(_) ->
    erlang:error(badarg).

%% @doc Convert sequence to human-readable string
-spec to_string(seq()) -> binary().
to_string({Epoch, Counter}) ->
    EpochBin = integer_to_binary(Epoch),
    CounterBin = integer_to_binary(Counter),
    <<EpochBin/binary, "-", CounterBin/binary>>.

%% @doc Parse sequence from string
-spec from_string(binary()) -> seq() | {error, invalid_sequence}.
from_string(Bin) when is_binary(Bin) ->
    case binary:split(Bin, <<"-">>) of
        [EpochBin, CounterBin] ->
            try
                Epoch = binary_to_integer(EpochBin),
                Counter = binary_to_integer(CounterBin),
                {Epoch, Counter}
            catch
                _:_ -> {error, invalid_sequence}
            end;
        _ ->
            {error, invalid_sequence}
    end;
from_string(_) ->
    {error, invalid_sequence}.

%%====================================================================
%% Constants
%%====================================================================

%% @doc Minimum sequence value
-spec min_seq() -> seq().
min_seq() ->
    {0, 0}.

%% @doc Maximum sequence value (for range scans)
-spec max_seq() -> seq().
max_seq() ->
    {16#FFFFFFFF, 16#FFFFFFFF}.
