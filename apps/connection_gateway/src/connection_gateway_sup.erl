-module(connection_gateway_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link(
        {local, ?MODULE},
        ?MODULE,
        []
    ).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    ChildSpecs = [
        #{
            id => connection_gateway_manager,
            start => {connection_gateway_manager, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [connection_gateway_manager]
        },
        #{
            id => connection_gateway_http_server,
            start => {connection_gateway_http_server, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [connection_gateway_http_server]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
