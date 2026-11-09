
-module(websocket_handler).

-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).

%% Upgrade HTTP connection to WebSocket

init(Req, _State) ->
    {cowboy_websocket, Req, #{}}.

%% Automatically subscribe this WebSocket process

websocket_init(_State) ->
    case relay_tree:subscribe(self()) of
        {ok, LeafPid} ->
            io:format(
                "WebSocket ~p subscribed to leaf ~p~n",
                [self(), LeafPid]
            ),

            {ok, #{leaf => LeafPid}};

        {error, Reason} ->
            io:format(
                "WebSocket subscription failed: ~p~n",
                [Reason]
            ),

            {stop, #{}}
    end.

%% Ignore incoming messages from subscribers

websocket_handle(_Frame, State) ->
    {ok, State}.

%% Receive broadcasts from leaf relays

websocket_info({comment, Message}, State)
    when is_binary(Message) ->

    {reply, {text, Message}, State};

websocket_info(_Info, State) ->
    {ok, State}.

%% Called when connection terminates

terminate(_Reason, _Req, _State) ->
    ok.

