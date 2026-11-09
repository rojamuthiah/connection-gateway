-module(tree_handler).

%% HTTP API for creating and inspecting the relay tree.

-export([init/2]).

-define(MAX_BODY_SIZE, 1024).
-define(MAX_LAYERS, 10).

init(Req, [Action]) ->
    Method = cowboy_req:method(Req),
    handle_request(Action, Method, Req, [Action]).

handle_request(create, <<"POST">>, Req, State) ->
    create_tree(Req, State);
handle_request(tree, <<"GET">>, Req, State) ->
    get_tree(Req, State);
handle_request(relays, <<"GET">>, Req, State) ->
    get_relays(Req, State);
handle_request(leaves, <<"GET">>, Req, State) ->
    get_leaves(Req, State);
handle_request(create, _Method, Req, State) ->
    method_not_allowed(<<"POST">>, Req, State);
handle_request(_Action, _Method, Req, State) ->
    method_not_allowed(<<"GET">>, Req, State).

create_tree(Req0, State) ->
    case cowboy_req:read_body(
        Req0,
        #{length => ?MAX_BODY_SIZE, period => 5000}
    ) of
        {ok, Body, Req1} ->
            decode_create_request(Body, Req1, State);
        {more, _PartialBody, Req1} ->
            reply(
                413,
                #{<<"error">> => <<"request_body_too_large">>},
                Req1,
                State
            )
    end.

decode_create_request(<<>>, Req, State) ->
    bad_request(<<"request_body_required">>, Req, State);
decode_create_request(Body, Req, State) ->
    Decoded =
        try json:decode(Body) of
            Json ->
                {ok, Json}
        catch
            error:_ ->
                error
        end,
    case Decoded of
        {ok, #{<<"layers">> := Layers}} when is_integer(Layers) ->
            create_tree_with_layers(Layers, Req, State);
        _ ->
            case Decoded of
                error ->
                    bad_request(<<"invalid_json">>, Req, State);
                _ ->
                    bad_request(<<"expected_integer_layers">>, Req, State)
            end
    end.

create_tree_with_layers(Layers, Req, State)
    when Layers >= 0, Layers =< ?MAX_LAYERS ->
    with_tree_manager(
        fun() ->
            case relay_tree:create_tree(Layers) of
                {ok, Leaves} ->
                    Root = relay_tree:get_root(),
                    reply(
                        201,
                        #{
                            <<"status">> => <<"created">>,
                            <<"layers">> => Layers,
                            <<"root">> => pid_json(Root),
                            <<"leaf_count">> => length(Leaves)
                        },
                        Req,
                        State
                    );
                {error, invalid_layers} ->
                    bad_request(<<"invalid_layers">>, Req, State);
                {error, Reason} ->
                    logger:error("Could not create relay tree: ~p", [Reason]),
                    unavailable(Req, State)
            end
        end,
        Req,
        State
    );
create_tree_with_layers(_Layers, Req, State) ->
    bad_request(<<"invalid_layers">>, Req, State).

get_tree(Req, State) ->
    with_tree_manager(
        fun() ->
            Root = relay_tree:get_root(),
            Relays = relay_tree:get_relays(),
            Leaves = relay_tree:get_leaves(),
            reply(
                200,
                #{
                    <<"root">> => pid_json(Root),
                    <<"relay_count">> => map_size(Relays),
                    <<"leaf_count">> => length(Leaves)
                },
                Req,
                State
            )
        end,
        Req,
        State
    ).

get_relays(Req, State) ->
    with_tree_manager(
        fun() ->
            Relays = relay_tree:get_relays(),
            RelayInfos = maps:values(Relays),
            SortedInfos = lists:sort(
                fun(Left, Right) ->
                    maps:get(id, Left) < maps:get(id, Right)
                end,
                RelayInfos
            ),
            RelayList = [relay_json(Info) || Info <- SortedInfos],
            reply(200, #{<<"relays">> => RelayList}, Req, State)
        end,
        Req,
        State
    ).

get_leaves(Req, State) ->
    with_tree_manager(
        fun() ->
            Leaves = relay_tree:get_leaves(),
            Relays = relay_tree:get_relays(),
            LeafList = [
                relay_json(maps:get(Pid, Relays))
                || Pid <- Leaves
            ],
            reply(200, #{<<"leaves">> => LeafList}, Req, State)
        end,
        Req,
        State
    ).

relay_json(Info) ->
    #{
        <<"id">> => list_to_binary(io_lib:format("~p", [maps:get(id, Info)])),
        <<"pid">> => pid_json(maps:get(pid, Info)),
        <<"level">> => maps:get(level, Info),
        <<"parent">> => pid_json(maps:get(parent, Info)),
        <<"children">> => [
            pid_json(Pid)
            || Pid <- maps:get(children, Info)
        ]
    }.

pid_json(undefined) ->
    null;
pid_json(Pid) when is_pid(Pid) ->
    list_to_binary(pid_to_list(Pid)).

with_tree_manager(Fun, Req, State) ->
    try Fun() of
        Result ->
            Result
    catch
        exit:Reason ->
            logger:error("Tree API request failed: ~p", [Reason]),
            unavailable(Req, State)
    end.

method_not_allowed(AllowedMethod, Req, State) ->
    Req1 = cowboy_req:reply(
        405,
        #{<<"allow">> => AllowedMethod},
        <<>>,
        Req
    ),
    {ok, Req1, State}.

bad_request(Error, Req, State) ->
    reply(400, #{<<"error">> => Error}, Req, State).

unavailable(Req, State) ->
    reply(503, #{<<"error">> => <<"relay_tree_unavailable">>}, Req, State).

reply(Status, Body, Req, State) ->
    ResponseBody = iolist_to_binary(json:encode(Body)),
    Req1 = cowboy_req:reply(
        Status,
        #{<<"content-type">> => <<"application/json">>},
        ResponseBody,
        Req
    ),
    {ok, Req1, State}.
