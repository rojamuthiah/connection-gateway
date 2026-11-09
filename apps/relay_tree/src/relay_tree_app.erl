
-module(relay_tree_app).

-behaviour(application).

-export([
    start/2,
    stop/1
]).

start(_StartType, _StartArgs) ->
    case application:ensure_all_started(cowboy) of
        {ok, _Started} ->
            relay_tree_sup:start_link();

        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
