
-module(publish_handler).

-export([init/2]).

-define(MAX_BODY_SIZE, 1048576).

init(Req0, State) ->
    case cowboy_req:method(Req0) of
        <<"POST">> ->
            read_and_publish(Req0, State);

        _ ->
            Req1 = cowboy_req:reply(
                405,
                #{<<"allow">> => <<"POST">>},
                <<>>,
                Req0
            ),

            {ok, Req1, State}
    end.

read_and_publish(Req0, State) ->
    case cowboy_req:read_body(
        Req0,
        #{
            length => ?MAX_BODY_SIZE,
            period => 5000
        }
    ) of
        {ok, Message, Req1} when byte_size(Message) > 0 ->
            publish(Message, Req1, State);

        {ok, <<>>, Req1} ->
            reply(
                400,
                <<"{\"error\":\"empty_message\"}">>,
                Req1,
                State
            );

        {more, _PartialBody, Req1} ->
            reply(
                413,
                <<"{\"error\":\"message_too_large\"}">>,
                Req1,
                State
            )
    end.

publish(Message, Req, State) ->
    case catch relay_tree:broadcast(Message) of
        ok ->
            reply(
                202,
                <<"{\"status\":\"accepted\"}">>,
                Req,
                State
            );

        _ ->
            reply(
                503,
                <<"{\"error\":\"relay_tree_unavailable\"}">>,
                Req,
                State
            )
    end.

reply(Status, Body, Req, State) ->
    Req1 = cowboy_req:reply(
        Status,
        #{<<"content-type">> => <<"application/json">>},
        Body,
        Req
    ),

    {ok, Req1, State}.
