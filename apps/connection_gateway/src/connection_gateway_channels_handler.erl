-module(connection_gateway_channels_handler).

-export([init/2]).

-define(MAX_BODY_SIZE, 1024).
-define(MAX_CHANNEL_ID_SIZE, 128).

init(Req, State) ->
    case cowboy_req:method(Req) of
        <<"POST">> ->
            create_channel(Req, State);
        _ ->
            Req1 = cowboy_req:reply(
                405,
                #{<<"allow">> => <<"POST">>},
                <<>>,
                Req
            ),
            {ok, Req1, State}
    end.

create_channel(Req0, State) ->
    case cowboy_req:read_body(
        Req0,
        #{length => ?MAX_BODY_SIZE, period => 5000}
    ) of
        {ok, Body, Req1} ->
            decode_channel_request(Body, Req1, State);
        {more, _PartialBody, Req1} ->
            reply(
                413,
                #{<<"error">> => <<"request_body_too_large">>},
                Req1,
                State
            )
    end.

decode_channel_request(Body, Req, State) ->
    Decoded =
        try json:decode(Body) of
            Json ->
                {ok, Json}
        catch
            error:_ ->
                error
        end,
    case Decoded of
        {ok, #{<<"channelId">> := ChannelId}}
            when is_binary(ChannelId),
                 byte_size(ChannelId) > 0,
                 byte_size(ChannelId) =< ?MAX_CHANNEL_ID_SIZE ->
            create_channel_id(ChannelId, Req, State);
        {ok, _} ->
            reply(
                400,
                #{<<"error">> => <<"invalid_channel_id">>},
                Req,
                State
            );
        error ->
            reply(
                400,
                #{<<"error">> => <<"invalid_json">>},
                Req,
                State
            )
    end.

create_channel_id(ChannelId, Req, State) ->
    try connection_gateway_manager:create_channel(ChannelId) of
        ok ->
            logger:info(
                "Channel created: channel_id=~p manager=~p node=~p",
                [ChannelId, whereis(connection_gateway_manager), node()]
            ),
            reply(
                201,
                #{
                    <<"status">> => <<"created">>,
                    <<"channelId">> => ChannelId
                },
                Req,
                State
            );
        {error, already_exists} ->
            logger:warning(
                "Channel creation rejected: already exists: ~p",
                [ChannelId]
            ),
            reply(
                409,
                #{<<"error">> => <<"channel_already_exists">>},
                Req,
                State
            );
        {error, Reason} ->
            logger:error("Could not create channel: ~p", [Reason]),
            manager_unavailable(Req, State)
    catch
        exit:Reason ->
            logger:error("Channel manager call failed: ~p", [Reason]),
            manager_unavailable(Req, State)
    end.

manager_unavailable(Req, State) ->
    reply(
        503,
        #{<<"error">> => <<"channel_manager_unavailable">>},
        Req,
        State
    ).

reply(Status, Body, Req, State) ->
    Response = iolist_to_binary(json:encode(Body)),
    Req1 = cowboy_req:reply(
        Status,
        #{<<"content-type">> => <<"application/json">>},
        Response,
        Req
    ),
    {ok, Req1, State}.
