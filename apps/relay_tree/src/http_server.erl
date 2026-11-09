
-module(http_server).

-export([start_link/0]).

start_link() ->
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/health", http_handler, []}
        ]}
    ]),

    cowboy:start_clear(
        relay_tree_listener,
        [{port, 8080}],
        #{
            env => #{
                dispatch => Dispatch
            }
        }
    ).
