%%%-------------------------------------------------------------------
%%% @doc CBOR Document Codec with Structural Index
%%%
%%% Implements a Cosmos-DB-like document storage codec using canonical
%%% CBOR (RFC 8949) with a structural index for BSON-like navigation
%%% without full document decoding.
%%%
%%% Record Format:
%%%   [ HEADER | INDEX | PAYLOAD ]
%%%
%%% Features:
%%% - Canonical CBOR encoding (deterministic, sorted map keys)
%%% - Structural index for O(1) peek and path navigation
%%% - SHA-256 hash of payload for revision IDs
%%% - Iterator API for streaming access
%%% - JSON import/export via OTP stdlib json module
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_docdb_codec_cbor).

%% Encoding API
-export([encode/1, encode/2]).

%% Decoding API
-export([decode/1]).

%% Record Info
-export([hash/1, payload/1, index_bin/1]).

%% Iterator API
-export([new_iterator/1, next/1, peek/2, find_path/2, decode_value/2]).

%% JSON Conversion
-export([to_json/1, to_json_iolist/1]).
-export([from_json/1]).

%% Internal exports for testing
-export([encode_varint/1, decode_varint/1]).
-export([encode_cbor/1, decode_cbor/1]).
-export([encode_bytes/1]).  %% For byte string encoding

%%====================================================================
%% Records
%%====================================================================

%% Value reference for partial decoding
-record(vref, {
    offset :: non_neg_integer(),
    length :: non_neg_integer(),
    type :: cbor_type(),
    container_id :: non_neg_integer() | undefined
}).

%% Iterator state
-record(iter, {
    record :: binary(),
    index :: parsed_index(),
    payload_start :: non_neg_integer(),
    container_id :: non_neg_integer(),
    entry_idx :: non_neg_integer(),
    entry_end :: non_neg_integer()
}).

%% Parsed index structure
-record(parsed_index, {
    containers :: #{non_neg_integer() => container()},
    entries :: tuple(),  %% tuple for O(1) access
    top_keys :: #{binary() => non_neg_integer()}
}).

%% Container info
-record(container, {
    id :: non_neg_integer(),
    kind :: map | array,
    start_off :: non_neg_integer(),
    end_off :: non_neg_integer(),
    parent_id :: non_neg_integer(),
    first_entry :: non_neg_integer(),
    entry_count :: non_neg_integer()
}).

%% Entry info
-record(entry, {
    key :: binary() | non_neg_integer(),
    value_off :: non_neg_integer(),
    value_len :: non_neg_integer(),
    value_type :: cbor_type(),
    container_id :: non_neg_integer() | undefined
}).

-type container() :: #container{}.
-type entry() :: #entry{}.
-type parsed_index() :: #parsed_index{}.

%%====================================================================
%% Types
%%====================================================================

-type record_bin() :: binary().
-type iter() :: #iter{}.
-type vref() :: #vref{}.
-type path() :: [binary() | non_neg_integer()].
-type cbor_type() :: uint | nint | bytes | text | array | map |
                     tag | simple | float_type | true | false | null.

-export_type([record_bin/0, iter/0, vref/0, path/0, cbor_type/0]).
-export_type([container/0, entry/0, parsed_index/0]).

%%====================================================================
%% Constants
%%====================================================================

-define(MAGIC, <<"CB">>).
-define(VERSION, 1).
-define(FLAG_EXTENDED_LEN, 1).

%% CBOR major types
-define(CBOR_UINT, 0).
-define(CBOR_NINT, 1).
-define(CBOR_BYTES, 2).
-define(CBOR_TEXT, 3).
-define(CBOR_ARRAY, 4).
-define(CBOR_MAP, 5).
-define(CBOR_TAG, 6).
-define(CBOR_SIMPLE, 7).

%% CBOR simple values
-define(CBOR_FALSE, 16#f4).
-define(CBOR_TRUE, 16#f5).
-define(CBOR_NULL, 16#f6).
-define(CBOR_FLOAT16, 16#f9).
-define(CBOR_FLOAT32, 16#fa).
-define(CBOR_FLOAT64, 16#fb).

%%====================================================================
%% Varint Encoding (LEB128 unsigned)
%%====================================================================

%% @doc Encode an unsigned integer as a varint (LEB128).
-spec encode_varint(non_neg_integer()) -> binary().
encode_varint(N) when N < 128 ->
    <<N>>;
encode_varint(N) ->
    <<1:1, (N band 127):7, (encode_varint(N bsr 7))/binary>>.

%% @doc Decode a varint from binary, returning {Value, Rest}.
-spec decode_varint(binary()) -> {non_neg_integer(), binary()}.
decode_varint(Bin) ->
    decode_varint(Bin, 0, 0).

decode_varint(<<0:1, Byte:7, Rest/binary>>, Acc, Shift) ->
    {Acc bor (Byte bsl Shift), Rest};
decode_varint(<<1:1, Byte:7, Rest/binary>>, Acc, Shift) ->
    decode_varint(Rest, Acc bor (Byte bsl Shift), Shift + 7).

%%====================================================================
%% CBOR Encoding (Canonical - RFC 8949)
%%====================================================================

%% @doc Encode an Erlang term to canonical CBOR.
-spec encode_cbor(term()) -> binary().
encode_cbor(N) when is_integer(N), N >= 0 ->
    encode_uint(?CBOR_UINT, N);
encode_cbor(N) when is_integer(N), N < 0 ->
    encode_uint(?CBOR_NINT, -1 - N);
encode_cbor(true) ->
    <<?CBOR_TRUE>>;
encode_cbor(false) ->
    <<?CBOR_FALSE>>;
encode_cbor(null) ->
    <<?CBOR_NULL>>;
encode_cbor(F) when is_float(F) ->
    encode_float(F);
encode_cbor(B) when is_binary(B) ->
    %% Always encode as UTF-8 text (type 3) per design decision
    encode_text(B);
encode_cbor(L) when is_list(L) ->
    encode_array(L);
encode_cbor(M) when is_map(M) ->
    encode_map(M);
encode_cbor(A) when is_atom(A) ->
    %% Atoms other than true/false/null encoded as text
    encode_text(atom_to_binary(A, utf8)).

%% Encode unsigned integer with major type
encode_uint(Major, N) when N < 24 ->
    <<(Major bsl 5 bor N)>>;
encode_uint(Major, N) when N < 256 ->
    <<(Major bsl 5 bor 24), N>>;
encode_uint(Major, N) when N < 65536 ->
    <<(Major bsl 5 bor 25), N:16/big>>;
encode_uint(Major, N) when N < 4294967296 ->
    <<(Major bsl 5 bor 26), N:32/big>>;
encode_uint(Major, N) ->
    <<(Major bsl 5 bor 27), N:64/big>>.

%% Encode text string (UTF-8)
encode_text(Bin) ->
    Len = byte_size(Bin),
    <<(encode_uint(?CBOR_TEXT, Len))/binary, Bin/binary>>.

%% Encode byte string
encode_bytes(Bin) ->
    Len = byte_size(Bin),
    <<(encode_uint(?CBOR_BYTES, Len))/binary, Bin/binary>>.

%% Encode array
encode_array(List) ->
    Len = length(List),
    Elements = << <<(encode_cbor(E))/binary>> || E <- List >>,
    <<(encode_uint(?CBOR_ARRAY, Len))/binary, Elements/binary>>.

%% Encode map with canonical key ordering
encode_map(Map) ->
    Pairs = maps:to_list(Map),
    %% Sort by encoded key representation (canonical CBOR requirement)
    SortedPairs = lists:sort(
        fun({K1, _}, {K2, _}) ->
            encode_cbor(K1) =< encode_cbor(K2)
        end,
        Pairs
    ),
    Len = length(SortedPairs),
    Elements = << <<(encode_cbor(K))/binary, (encode_cbor(V))/binary>>
                 || {K, V} <- SortedPairs >>,
    <<(encode_uint(?CBOR_MAP, Len))/binary, Elements/binary>>.

%% Encode float (prefer smaller representation when lossless)
encode_float(F) ->
    %% Try to encode as float32 if no precision loss
    %% For canonical CBOR, we prefer smallest encoding
    case can_use_float32(F) of
        true ->
            <<?CBOR_FLOAT32, F:32/float-big>>;
        false ->
            <<?CBOR_FLOAT64, F:64/float-big>>
    end.

%% Check if float can be represented in float32 without precision loss
can_use_float32(F) ->
    %% Float32 max is ~3.4e38, check range first to avoid infinity
    case abs(F) < 3.4e38 of
        false ->
            false;
        true ->
            %% Check if no precision loss
            Float32Bin = <<F:32/float-big>>,
            <<F32:32/float-big>> = Float32Bin,
            F32 == F
    end.

%%====================================================================
%% CBOR Decoding
%%====================================================================

%% @doc Decode CBOR binary to Erlang term.
-spec decode_cbor(binary()) -> term().
decode_cbor(Bin) ->
    {Term, <<>>} = decode_cbor_value(Bin),
    Term.

decode_cbor_value(<<MajorInfo, Rest/binary>>) ->
    Major = MajorInfo bsr 5,
    Info = MajorInfo band 31,
    decode_cbor_major(Major, Info, Rest).

%% Unsigned integer
decode_cbor_major(?CBOR_UINT, Info, Rest) ->
    decode_uint(Info, Rest);

%% Negative integer
decode_cbor_major(?CBOR_NINT, Info, Rest) ->
    {N, Rest2} = decode_uint(Info, Rest),
    {-1 - N, Rest2};

%% Byte string
decode_cbor_major(?CBOR_BYTES, Info, Rest) ->
    {Len, Rest2} = decode_uint(Info, Rest),
    <<Bytes:Len/binary, Rest3/binary>> = Rest2,
    {Bytes, Rest3};

%% Text string
decode_cbor_major(?CBOR_TEXT, Info, Rest) ->
    {Len, Rest2} = decode_uint(Info, Rest),
    <<Text:Len/binary, Rest3/binary>> = Rest2,
    {Text, Rest3};

%% Array
decode_cbor_major(?CBOR_ARRAY, Info, Rest) ->
    {Len, Rest2} = decode_uint(Info, Rest),
    decode_array(Len, Rest2, []);

%% Map
decode_cbor_major(?CBOR_MAP, Info, Rest) ->
    {Len, Rest2} = decode_uint(Info, Rest),
    decode_map(Len, Rest2, #{});

%% Simple values and floats
decode_cbor_major(?CBOR_SIMPLE, 20, Rest) ->
    {false, Rest};
decode_cbor_major(?CBOR_SIMPLE, 21, Rest) ->
    {true, Rest};
decode_cbor_major(?CBOR_SIMPLE, 22, Rest) ->
    {null, Rest};
decode_cbor_major(?CBOR_SIMPLE, 25, <<F:16/float-big, Rest/binary>>) ->
    {F, Rest};
decode_cbor_major(?CBOR_SIMPLE, 26, <<F:32/float-big, Rest/binary>>) ->
    {F, Rest};
decode_cbor_major(?CBOR_SIMPLE, 27, <<F:64/float-big, Rest/binary>>) ->
    {F, Rest}.

decode_uint(Info, Rest) when Info < 24 ->
    {Info, Rest};
decode_uint(24, <<N, Rest/binary>>) ->
    {N, Rest};
decode_uint(25, <<N:16/big, Rest/binary>>) ->
    {N, Rest};
decode_uint(26, <<N:32/big, Rest/binary>>) ->
    {N, Rest};
decode_uint(27, <<N:64/big, Rest/binary>>) ->
    {N, Rest}.

decode_array(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_array(N, Bin, Acc) ->
    {Value, Rest} = decode_cbor_value(Bin),
    decode_array(N - 1, Rest, [Value | Acc]).

decode_map(0, Rest, Acc) ->
    {Acc, Rest};
decode_map(N, Bin, Acc) ->
    {Key, Rest1} = decode_cbor_value(Bin),
    {Value, Rest2} = decode_cbor_value(Rest1),
    decode_map(N - 1, Rest2, Acc#{Key => Value}).

%%====================================================================
%% Record Encoding (with Index)
%%====================================================================

%% @doc Encode an Erlang term to a CBOR record with structural index.
-spec encode(term()) -> record_bin().
encode(Term) ->
    encode(Term, #{}).

%% @doc Encode with options.
-spec encode(term(), map()) -> record_bin().
encode(Term, _Opts) ->
    %% TODO: Implement full record encoding with index
    %% For now, just encode CBOR payload
    Payload = encode_cbor(Term),
    Hash = crypto:hash(sha256, Payload),
    HashLen = byte_size(Hash),
    PayloadLen = byte_size(Payload),
    IndexLen = 0,
    Flags = 0,
    Header = <<?MAGIC/binary, ?VERSION, Flags, HashLen,
               PayloadLen:32/big, IndexLen:32/big,
               Hash/binary>>,
    <<Header/binary, Payload/binary>>.

%%====================================================================
%% Record Decoding
%%====================================================================

%% @doc Decode a CBOR record to Erlang term.
-spec decode(record_bin()) -> term().
decode(RecordBin) ->
    {_Header, _IndexBin, PayloadBin} = parse_record(RecordBin),
    decode_cbor(PayloadBin).

%% Parse record into components
parse_record(<<"CB", Version, Flags, HashLen,
               PayloadLen:32/big, IndexLen:32/big,
               Rest/binary>>) when Version == ?VERSION,
                                   Flags band ?FLAG_EXTENDED_LEN == 0 ->
    <<Hash:HashLen/binary, IndexBin:IndexLen/binary,
      PayloadBin:PayloadLen/binary>> = Rest,
    Header = #{magic => ?MAGIC, version => Version, flags => Flags,
               hash => Hash, payload_len => PayloadLen, index_len => IndexLen},
    {Header, IndexBin, PayloadBin}.

%%====================================================================
%% Record Info
%%====================================================================

%% @doc Get the SHA-256 hash of the payload.
-spec hash(record_bin()) -> binary().
hash(RecordBin) ->
    {#{hash := Hash}, _, _} = parse_record(RecordBin),
    Hash.

%% @doc Get the raw payload binary.
-spec payload(record_bin()) -> binary().
payload(RecordBin) ->
    {_, _, PayloadBin} = parse_record(RecordBin),
    PayloadBin.

%% @doc Get the raw index binary.
-spec index_bin(record_bin()) -> binary().
index_bin(RecordBin) ->
    {_, IndexBin, _} = parse_record(RecordBin),
    IndexBin.

%%====================================================================
%% Iterator API (Stubs)
%%====================================================================

%% @doc Create a new iterator over a CBOR record.
-spec new_iterator(record_bin()) -> {ok, iter()} | {error, term()}.
new_iterator(_RecordBin) ->
    {error, not_implemented}.

%% @doc Get the next entry from the iterator.
-spec next(iter()) -> {ok, {binary() | non_neg_integer(), cbor_type(), vref()}, iter()} | done.
next(_Iter) ->
    done.

%% @doc Peek at a top-level key without iteration.
-spec peek(record_bin(), binary()) -> {ok, {cbor_type(), vref()}} | not_found | {error, term()}.
peek(_RecordBin, _Key) ->
    {error, not_implemented}.

%% @doc Find a value by path.
-spec find_path(record_bin(), path()) -> {ok, {cbor_type(), vref()}} | not_found | {error, term()}.
find_path(_RecordBin, _Path) ->
    {error, not_implemented}.

%% @doc Decode a value using a ValueRef.
-spec decode_value(record_bin(), vref()) -> {ok, term()} | {error, term()}.
decode_value(_RecordBin, _VRef) ->
    {error, not_implemented}.

%%====================================================================
%% JSON Conversion (Stubs)
%%====================================================================

%% @doc Convert CBOR record to JSON binary.
-spec to_json(record_bin()) -> binary().
to_json(RecordBin) ->
    iolist_to_binary(to_json_iolist(RecordBin)).

%% @doc Convert CBOR record to JSON iolist.
-spec to_json_iolist(record_bin()) -> iolist().
to_json_iolist(RecordBin) ->
    %% For now, decode and re-encode via json module
    Term = decode(RecordBin),
    json:encode(Term).

%% @doc Parse JSON and encode to CBOR record.
-spec from_json(binary()) -> record_bin().
from_json(JsonBin) ->
    %% For now, use simple decode/encode
    Term = json:decode(JsonBin),
    encode(Term).
