-module(connection_gateway_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    connection_gateway_sup:start_link().

stop(_State) ->
    ok.
