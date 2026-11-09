-module(connection_gateway_health_handler).

-export([init/2]).

init(Req, State) ->
    Response = json:encode(#{<<"status">> => <<"ok">>}),
    Req1 = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        Response,
        Req
    ),
    {ok, Req1, State}.
