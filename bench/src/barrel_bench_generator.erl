%%%-------------------------------------------------------------------
%%% @doc Document generator for barrel_bench
%%%
%%% Generates documents for benchmarking with configurable patterns
%%% that support different query types.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_bench_generator).

-export([user_doc/1, user_doc/2]).
-export([random_doc/1, random_doc/2]).
-export([batch/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type doc_options() :: #{
    id => binary(),
    age_range => {non_neg_integer(), non_neg_integer()},
    cities => [binary()],
    statuses => [binary()]
}.

-export_type([doc_options/0]).

%% Cities for random selection
-define(DEFAULT_CITIES, [
    <<"Paris">>, <<"London">>, <<"Berlin">>, <<"Madrid">>,
    <<"Rome">>, <<"Amsterdam">>, <<"Vienna">>, <<"Prague">>
]).

%% Statuses for random selection
-define(DEFAULT_STATUSES, [
    <<"active">>, <<"inactive">>, <<"pending">>
]).

%% @doc Generate a user document with auto-generated ID
-spec user_doc(non_neg_integer()) -> map().
user_doc(Index) ->
    user_doc(Index, #{}).

%% @doc Generate a user document with options
-spec user_doc(non_neg_integer(), doc_options()) -> map().
user_doc(Index, Opts) ->
    Id = maps:get(id, Opts, make_id(<<"user">>, Index)),
    {MinAge, MaxAge} = maps:get(age_range, Opts, {18, 80}),
    Cities = maps:get(cities, Opts, ?DEFAULT_CITIES),
    Statuses = maps:get(statuses, Opts, ?DEFAULT_STATUSES),

    Age = MinAge + (Index rem (MaxAge - MinAge + 1)),
    City = pick(Cities, Index),
    Status = pick(Statuses, Index),

    #{
        <<"_id">> => Id,
        <<"type">> => <<"user">>,
        <<"name">> => <<"User ", (integer_to_binary(Index))/binary>>,
        <<"age">> => Age,
        <<"status">> => Status,
        <<"profile">> => #{
            <<"city">> => City,
            <<"country">> => city_country(City)
        },
        <<"tags">> => generate_tags(Index),
        <<"created_at">> => Index
    }.

%% @doc Generate a random document with auto-generated ID
-spec random_doc(non_neg_integer()) -> map().
random_doc(Index) ->
    random_doc(Index, #{}).

%% @doc Generate a random document with configurable payload size
-spec random_doc(non_neg_integer(), doc_options()) -> map().
random_doc(Index, Opts) ->
    Id = maps:get(id, Opts, make_id(<<"doc">>, Index)),
    #{
        <<"_id">> => Id,
        <<"type">> => <<"generic">>,
        <<"index">> => Index,
        <<"data">> => generate_payload(Index)
    }.

%% @doc Generate a batch of documents
-spec batch(fun((non_neg_integer()) -> map()), non_neg_integer(), non_neg_integer()) -> [map()].
batch(Generator, Start, Count) ->
    [Generator(I) || I <- lists:seq(Start, Start + Count - 1)].

%%====================================================================
%% Internal functions
%%====================================================================

make_id(Prefix, Index) ->
    <<Prefix/binary, "_", (integer_to_binary(Index))/binary>>.

pick(List, Index) ->
    lists:nth((Index rem length(List)) + 1, List).

city_country(<<"Paris">>) -> <<"France">>;
city_country(<<"London">>) -> <<"UK">>;
city_country(<<"Berlin">>) -> <<"Germany">>;
city_country(<<"Madrid">>) -> <<"Spain">>;
city_country(<<"Rome">>) -> <<"Italy">>;
city_country(<<"Amsterdam">>) -> <<"Netherlands">>;
city_country(<<"Vienna">>) -> <<"Austria">>;
city_country(<<"Prague">>) -> <<"Czech Republic">>;
city_country(_) -> <<"Unknown">>.

generate_tags(Index) ->
    Tags = [<<"tag1">>, <<"tag2">>, <<"tag3">>, <<"tag4">>],
    NumTags = (Index rem 3) + 1,
    lists:sublist(Tags, NumTags).

generate_payload(Index) ->
    %% Generate a simple payload that varies by index
    base64:encode(crypto:strong_rand_bytes(64 + (Index rem 64))).

%%====================================================================
%% EUnit Tests
%%====================================================================

-ifdef(TEST).

user_doc_test() ->
    Doc = user_doc(0),
    ?assertEqual(<<"user_0">>, maps:get(<<"_id">>, Doc)),
    ?assertEqual(<<"user">>, maps:get(<<"type">>, Doc)),
    ?assertEqual(<<"User 0">>, maps:get(<<"name">>, Doc)),
    ?assert(is_integer(maps:get(<<"age">>, Doc))),
    ?assert(is_binary(maps:get(<<"status">>, Doc))),
    ?assert(is_map(maps:get(<<"profile">>, Doc))).

user_doc_custom_id_test() ->
    Doc = user_doc(0, #{id => <<"custom_id">>}),
    ?assertEqual(<<"custom_id">>, maps:get(<<"_id">>, Doc)).

user_doc_age_range_test() ->
    Doc0 = user_doc(0, #{age_range => {20, 30}}),
    Doc10 = user_doc(10, #{age_range => {20, 30}}),
    Age0 = maps:get(<<"age">>, Doc0),
    Age10 = maps:get(<<"age">>, Doc10),
    ?assert(Age0 >= 20 andalso Age0 =< 30),
    ?assert(Age10 >= 20 andalso Age10 =< 30).

user_doc_profile_test() ->
    Doc = user_doc(0),
    Profile = maps:get(<<"profile">>, Doc),
    ?assert(is_binary(maps:get(<<"city">>, Profile))),
    ?assert(is_binary(maps:get(<<"country">>, Profile))).

random_doc_test() ->
    Doc = random_doc(0),
    ?assertEqual(<<"doc_0">>, maps:get(<<"_id">>, Doc)),
    ?assertEqual(<<"generic">>, maps:get(<<"type">>, Doc)),
    ?assertEqual(0, maps:get(<<"index">>, Doc)),
    ?assert(is_binary(maps:get(<<"data">>, Doc))).

batch_test() ->
    Docs = batch(fun user_doc/1, 0, 5),
    ?assertEqual(5, length(Docs)),
    Ids = [maps:get(<<"_id">>, D) || D <- Docs],
    ?assertEqual([<<"user_0">>, <<"user_1">>, <<"user_2">>, <<"user_3">>, <<"user_4">>], Ids).

batch_offset_test() ->
    Docs = batch(fun user_doc/1, 100, 3),
    ?assertEqual(3, length(Docs)),
    Ids = [maps:get(<<"_id">>, D) || D <- Docs],
    ?assertEqual([<<"user_100">>, <<"user_101">>, <<"user_102">>], Ids).

tags_test() ->
    Doc0 = user_doc(0),
    Doc1 = user_doc(1),
    Doc2 = user_doc(2),
    ?assertEqual(1, length(maps:get(<<"tags">>, Doc0))),
    ?assertEqual(2, length(maps:get(<<"tags">>, Doc1))),
    ?assertEqual(3, length(maps:get(<<"tags">>, Doc2))).

-endif.
