
-module(http_server).

%% Owns the Cowboy listener and keeps route definitions in one place.

-export([start_link/0]).

-define(DEFAULT_PORT, 8000).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    case listener_port() of
        {ok, Port} ->
            %% Keep endpoint-to-handler mappings together for easy API discovery.
            Dispatch = cowboy_router:compile([
                {'_', [
                    {"/health", http_handler, []},
                    {"/publish", publish_handler, []},
                    {"/tree/create", tree_handler, [create]},
                    {"/tree", tree_handler, [tree]},
                    {"/tree/relays", tree_handler, [relays]},
                    {"/tree/leaves", tree_handler, [leaves]},
                    {"/ws", websocket_handler, []}
                ]}
            ]),

            logger:info("Starting HTTP listener on port ~p", [Port]),
            cowboy:start_clear(
                relay_tree_listener,
                [{port, Port}],
                #{
                    env => #{
                        dispatch => Dispatch
                    }
                }
            );
        {error, Reason} ->
            logger:error("Could not start HTTP listener: ~p", [Reason]),
            {error, Reason}
    end.

listener_port() ->
    case os:getenv("RELAY_TREE_PORT") of
        false ->
            {ok, ?DEFAULT_PORT};
        PortString ->
            parse_port(PortString)
    end.

parse_port(PortString) ->
    case string:to_integer(PortString) of
        {Port, []} when Port >= 1, Port =< 65535 ->
            {ok, Port};
        _ ->
            {error, {invalid_relay_tree_port, PortString}}
    end.
