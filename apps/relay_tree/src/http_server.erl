
-module(http_server).

%% Owns the Cowboy listener and keeps route definitions in one place.

-export([start_link/0]).

start_link() ->
    %% Keep endpoint-to-handler mappings together for easy API discovery.
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/health", http_handler, []},
            {"/publish", publish_handler, []},
            {"/ws", websocket_handler, []}
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
