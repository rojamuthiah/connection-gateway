
-module(relay_tree_sup).

-behaviour(supervisor).

-export([
    start_link/0,
    init/1
]).

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
            id => http_server,
            start => {http_server, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [http_server]
        },
        #{
            id => relay_tree,
            start => {relay_tree, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [relay_tree]
        }
    ],

    {ok, {SupFlags, ChildSpecs}}.
