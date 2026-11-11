-module(connection_gateway_ws_handler).

-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).

init(Req, _RouteState) ->
    {cowboy_websocket, Req, #{}}.

websocket_init(_State) ->
    case connection_gateway_manager:register_connection(self()) of
        {ok, ConnectionId} ->
            logger:info(
                "WebSocket connected: connection_id=~p pid=~p node=~p",
                [ConnectionId, self(), node()]
            ),
            {ok, #{connection_id => ConnectionId}};
        {error, Reason} ->
            logger:error("Could not register WebSocket connection: ~p", [Reason]),
            {stop, #{}}
    end.

websocket_handle({text, Frame}, State) ->
    handle_text_frame(Frame, State);
websocket_handle(_Frame, State) ->
    {ok, State}.

websocket_info({channel_message, Message}, State) ->
    {reply, {text, Message}, State};
websocket_info(_Info, State) ->
    {ok, State}.

terminate(TerminationReason, _Req, State) ->
    case maps:find(connection_id, State) of
        {ok, ConnectionId} ->
            logger:info(
                "WebSocket disconnected: connection_id=~p pid=~p reason=~p",
                [ConnectionId, self(), TerminationReason]
            );
        error ->
            logger:warning(
                "WebSocket terminated before registration: pid=~p reason=~p",
                [self(), TerminationReason]
            )
    end,
    try connection_gateway_manager:disconnect(self()) of
        ok ->
            ok
    catch
        exit:ManagerExitReason ->
            logger:debug(
                "Channel manager unavailable while disconnecting WebSocket: ~p",
                [ManagerExitReason]
            ),
            ok
    end.

handle_text_frame(Frame, State) ->
    Decoded =
        try json:decode(Frame) of
            Json ->
                {ok, Json}
        catch
            error:_ ->
                error
        end,
    case Decoded of
        {ok, #{<<"type">> := <<"subscribe">>, <<"channelId">> := ChannelId}}
            when is_binary(ChannelId) ->
            subscribe(ChannelId, State);
        {ok, #{
            <<"type">> := <<"publish_normal">>,
            <<"channelId">> := ChannelId,
            <<"message">> := Message
        }}
            when is_binary(ChannelId), is_binary(Message) ->
            publish_normal(ChannelId, Message, State);
        {ok, #{
            <<"type">> := <<"publish_bad">>,
            <<"channelId">> := ChannelId,
            <<"message">> := Message
        }}
            when is_binary(ChannelId), is_binary(Message) ->
            publish_bad(ChannelId, Message, State);
        {ok, _} ->
            error_frame(<<"invalid_message">>, State);
        error ->
            error_frame(<<"invalid_json">>, State)
    end.

subscribe(ChannelId, State) ->
    try connection_gateway_manager:subscribe(ChannelId, self()) of
        {ok, ConnectionId} ->
            logger:info(
                "WebSocket subscribed: connection_id=~p channel_id=~p manager=~p node=~p",
                [
                    ConnectionId,
                    ChannelId,
                    whereis(connection_gateway_manager),
                    node()
                ]
            ),
            reply_json(
                #{
                    <<"type">> => <<"subscribed">>,
                    <<"channelId">> => ChannelId,
                    <<"connectionId">> => ConnectionId
                },
                State
            );
        {error, unknown_channel} ->
            logger:warning(
                "WebSocket subscription rejected: unknown channel "
                "channel_id=~p connection_id=~p manager=~p node=~p",
                [
                    ChannelId,
                    maps:get(connection_id, State, undefined),
                    whereis(connection_gateway_manager),
                    node()
                ]
            ),
            error_frame(<<"unknown_channel">>, State);
        {error, Reason} ->
            logger:error("Could not subscribe WebSocket: ~p", [Reason]),
            error_frame(<<"subscription_failed">>, State)
    catch
        exit:Reason ->
            logger:error("Channel manager unavailable during subscribe: ~p", [Reason]),
            error_frame(<<"channel_manager_unavailable">>, State)
    end.

publish_normal(ChannelId, Message, State) ->
    try connection_gateway_manager:publish_normal(
        ChannelId,
        self(),
        Message
    ) of
        ok ->
            {ok, State};
        {error, Reason} ->
            error_frame(error_name(Reason), State)
    catch
        exit:Reason ->
            logger:error("Channel manager unavailable during publish: ~p", [Reason]),
            error_frame(<<"channel_manager_unavailable">>, State)
    end.

publish_bad(ChannelId, Message, State) ->
    try connection_gateway_manager:validate_publish(ChannelId, self()) of
        ok ->
            _Checksum = connection_gateway_workload:run(),
            try connection_gateway_manager:publish_bad(
                ChannelId,
                self(),
                Message
            ) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    error_frame(error_name(Reason), State)
            catch
                exit:Reason ->
                    logger:error(
                        "Channel manager unavailable during bad publish: ~p",
                        [Reason]
                    ),
                    error_frame(<<"channel_manager_unavailable">>, State)
            end;
        {error, Reason} ->
            error_frame(error_name(Reason), State)
    catch
        exit:Reason ->
            logger:error(
                "Channel manager unavailable during bad publish: ~p",
                [Reason]
            ),
            error_frame(<<"channel_manager_unavailable">>, State)
    end.

error_name(Reason) ->
    list_to_binary(atom_to_list(Reason)).

error_frame(Error, State) ->
    reply_json(#{<<"type">> => <<"error">>, <<"error">> => Error}, State).

reply_json(Message, State) ->
    Encoded = iolist_to_binary(json:encode(Message)),
    {reply, {text, Encoded}, State}.
