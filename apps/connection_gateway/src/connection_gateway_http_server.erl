-module(connection_gateway_http_server).

-behaviour(gen_server).

-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(DEFAULT_PORT, 8080).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    case listener_port() of
        {ok, Port} ->
            Dispatch = cowboy_router:compile([
                {'_', [
                    {"/health", connection_gateway_health_handler, []},
                    {"/channels", connection_gateway_channels_handler, []},
                    {"/ws", connection_gateway_ws_handler, []}
                ]}
            ]),
            case cowboy:start_clear(
                connection_gateway_listener,
                [{port, Port}],
                #{env => #{dispatch => Dispatch}}
            ) of
                {ok, ListenerPid} ->
                    logger:info(
                        "Connection gateway listening on port ~p",
                        [Port]
                    ),
                    {ok, ListenerPid};
                {error, Reason} ->
                    {stop, {listener_start_failed, Reason}}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _ListenerPid) ->
    _ = cowboy:stop_listener(connection_gateway_listener),
    ok.

listener_port() ->
    case os:getenv("CONNECTION_GATEWAY_PORT") of
        false ->
            {ok, ?DEFAULT_PORT};
        PortString ->
            case string:to_integer(PortString) of
                {Port, []} when Port >= 1, Port =< 65535 ->
                    {ok, Port};
                _ ->
                    {error, {invalid_connection_gateway_port, PortString}}
            end
    end.
