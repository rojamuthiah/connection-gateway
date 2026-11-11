-module(connection_gateway_workload).

%% CPU work performed before a publish_bad message is broadcast.

-export([run/0, duration_ms/0]).

-define(DEFAULT_DURATION_MS, 250).
-define(MAX_DURATION_MS, 10000).
-define(MODULUS, 2147483648).

-spec duration_ms() -> {ok, pos_integer()} | {error, term()}.
duration_ms() ->
    case os:getenv("CONNECTION_GATEWAY_BAD_WORK_MS") of
        false ->
            {ok, ?DEFAULT_DURATION_MS};
        Value ->
            case string:to_integer(Value) of
                {DurationMs, []}
                    when DurationMs >= 1, DurationMs =< ?MAX_DURATION_MS ->
                    {ok, DurationMs};
                _ ->
                    {error,
                        {invalid_connection_gateway_bad_work_ms, Value}}
            end
    end.

-spec run() -> non_neg_integer().
run() ->
    case duration_ms() of
        {ok, DurationMs} ->
            Deadline = erlang:monotonic_time(microsecond) +
                DurationMs * 1000,
            iterate_until(Deadline, 1);
        {error, Reason} ->
            error(Reason)
    end.

iterate_until(Deadline, Accumulator) ->
    case erlang:monotonic_time(microsecond) >= Deadline of
        true ->
            Accumulator;
        false ->
            iterate_until(Deadline, next_value(Accumulator))
    end.

next_value(Accumulator) ->
    Next = (Accumulator * 1103515245 + 12345) rem ?MODULUS,
    Next.
