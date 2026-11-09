-module(connection_gateway_workload).

%% Fixed, bounded CPU work used by the publish_bad benchmark operation.

-export([run/0]).

-define(ITERATIONS, 1000000).
-define(MODULUS, 2147483648).

-spec run() -> non_neg_integer().
run() ->
    iterate(?ITERATIONS, 1).

iterate(0, Accumulator) ->
    Accumulator;
iterate(Remaining, Accumulator) ->
    Next = (Accumulator * 1103515245 + 12345) rem ?MODULUS,
    iterate(Remaining - 1, Next).
