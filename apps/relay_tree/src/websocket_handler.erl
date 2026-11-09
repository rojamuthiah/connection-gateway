
-module(websocket_handler).

%% Cowboy WebSocket callbacks for clients that receive relay broadcasts.

-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).

init(Req, _State) ->
    {cowboy_websocket, Req, #{}}.

websocket_init(_State) ->
    case relay_tree:subscribe(self()) of
        {ok, LeafPid} ->
            logger:info(
                "WebSocket ~p subscribed to leaf ~p",
                [self(), LeafPid]
            ),

            {ok, #{leaf => LeafPid}};

        {error, Reason} ->
            logger:error(
                "WebSocket subscription failed: ~p",
                [Reason]
            ),

            {stop, #{}}
    end.

%% This endpoint only delivers broadcasts; client frames are ignored.
websocket_handle(_Frame, State) ->
    {ok, State}.

websocket_info({comment, Message}, State)
    when is_binary(Message) ->

    {reply, {text, Message}, State};

websocket_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _Req, _State) ->
    ok.
