-module(debug_query).
-export([test/0]).

test() ->
    %% Simulate what the benchmark does
    Conditions = [
        {path, [<<"type">>], <<"user">>},
        {path, [<<"status">>], <<"active">>},
        {compare, [<<"age">>], '>', 50}
    ],
    
    IndexConditions = find_all_index_conditions(Conditions),
    io:format("IndexConditions: ~p~n", [IndexConditions]),
    
    AllEquality = lists:all(fun({path, _, _}) -> true; (_) -> false end, IndexConditions),
    io:format("AllEquality: ~p~n", [AllEquality]),
    
    {EqConds, NonEqConds} = lists:partition(fun
        ({path, _, _}) -> true;
        (_) -> false
    end, IndexConditions),
    io:format("EqConds: ~p~n", [EqConds]),
    io:format("NonEqConds: ~p~n", [NonEqConds]),
    
    ok.

find_all_index_conditions(Conditions) ->
    find_all_index_conditions(Conditions, []).

find_all_index_conditions([], Acc) ->
    lists:reverse(Acc);
find_all_index_conditions([{path, Path, Value} | Rest], Acc) ->
    find_all_index_conditions(Rest, [{path, Path, Value} | Acc]);
find_all_index_conditions([{compare, Path, Op, Value} | Rest], Acc)
  when Op =:= '>' orelse Op =:= '<' orelse Op =:= '>=' orelse Op =:= '=<' ->
    find_all_index_conditions(Rest, [{compare, Path, Op, Value} | Acc]);
find_all_index_conditions([_ | Rest], Acc) ->
    find_all_index_conditions(Rest, Acc).
