
-module(http_handler).

-export([init/2]).

init(Req, State) ->
    Req2 = cowboy_req:reply(
        200,
        #{<<"content-type">> => <<"application/json">>},
        <<"{\"status\":\"ok\"}">>,
        Req
    ),

    {ok, Req2, State}.
