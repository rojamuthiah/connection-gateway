
-module(mock_client).

-export([
    start/1,
    loop/1
]).

start(ClientId) ->
    spawn(?MODULE, loop, [ClientId]).

loop(ClientId) ->
    receive
        {comment, Message} ->
            io:format(
                "Client ~p received: ~p~n",
                [ClientId, Message]
            ),

            loop(ClientId);

        stop ->
            ok
    end.
