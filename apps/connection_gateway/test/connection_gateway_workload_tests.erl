-module(connection_gateway_workload_tests).

-include_lib("eunit/include/eunit.hrl").

bad_workload_runs_for_configured_duration_test() ->
    {ok, DurationMs} = connection_gateway_workload:duration_ms(),
    StartedAt = erlang:monotonic_time(millisecond),
    _Checksum = connection_gateway_workload:run(),
    ElapsedMs = erlang:monotonic_time(millisecond) - StartedAt,
    ?assert(ElapsedMs >= DurationMs).

bad_workloads_run_in_parallel_test() ->
    {ok, DurationMs} = connection_gateway_workload:duration_ms(),
    Parent = self(),
    StartedAt = erlang:monotonic_time(millisecond),
    _Pids = [
        spawn(fun() ->
            _Checksum = connection_gateway_workload:run(),
            Parent ! workload_finished
        end)
     || _ <- lists:seq(1, 2)
    ],
    receive
        workload_finished -> ok
    after 1000 ->
        ?assert(false)
    end,
    receive
        workload_finished -> ok
    after 1000 ->
        ?assert(false)
    end,
    ElapsedMs = erlang:monotonic_time(millisecond) - StartedAt,
    ?assert(ElapsedMs < DurationMs * 2).
