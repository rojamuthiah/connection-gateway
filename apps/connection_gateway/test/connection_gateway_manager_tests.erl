-module(connection_gateway_manager_tests).

-include_lib("eunit/include/eunit.hrl").

connection_ids_are_generated_by_the_server_test() ->
    with_manager(
        fun() ->
            ok = connection_gateway_manager:create_channel(<<"stream-101">>),
            {ok, ConnectionId} =
                connection_gateway_manager:register_connection(self()),

            ?assertMatch(<<"conn-", _/binary>>, ConnectionId),
            ?assertEqual(
                {ok, ConnectionId},
                connection_gateway_manager:subscribe(
                    <<"stream-101">>,
                    self()
                )
            )
        end
    ).

normal_publish_reaches_other_subscribers_only_test() ->
    with_manager(
        fun() ->
            ok = connection_gateway_manager:create_channel(<<"stream-101">>),
            {ok, _PublisherId} =
                connection_gateway_manager:register_connection(self()),
            {ok, _} =
                connection_gateway_manager:subscribe(
                    <<"stream-101">>,
                    self()
                ),
            Subscriber = spawn_subscriber(),
            {ok, _SubscriberId} =
                connection_gateway_manager:register_connection(Subscriber),
            {ok, _} =
                connection_gateway_manager:subscribe(
                    <<"stream-101">>,
                    Subscriber
                ),

            ok = connection_gateway_manager:publish_normal(
                <<"stream-101">>,
                self(),
                <<"Hello everyone!">>
            ),

            assert_channel_message(<<"Hello everyone!">>),
            assert_no_channel_message(),
            stop_subscriber(Subscriber)
        end
    ).

bad_publish_broadcasts_to_other_subscribers_test() ->
    with_manager(
        fun() ->
            ok = connection_gateway_manager:create_channel(<<"stream-101">>),
            {ok, _PublisherId} =
                connection_gateway_manager:register_connection(self()),
            {ok, _} =
                connection_gateway_manager:subscribe(
                    <<"stream-101">>,
                    self()
                ),

            SecondSubscriber = spawn_subscriber(),
            {ok, _SecondId} =
                connection_gateway_manager:register_connection(
                    SecondSubscriber
                ),
            {ok, _} =
                connection_gateway_manager:subscribe(
                    <<"stream-101">>,
                    SecondSubscriber
                ),

            ok = connection_gateway_manager:publish_bad(
                <<"stream-101">>,
                self(),
                <<"EXPENSIVE">>
            ),
            assert_channel_message(<<"EXPENSIVE">>),
            assert_no_channel_message(),
            stop_subscriber(SecondSubscriber)
        end
    ).

assert_channel_message(ExpectedMessage) ->
    receive
        {subscriber_message, _Subscriber, Encoded} ->
            ?assertEqual(
                #{
                    <<"type">> => <<"message">>,
                    <<"channelId">> => <<"stream-101">>,
                    <<"message">> => ExpectedMessage
                },
                json:decode(Encoded)
            )
    after 1000 ->
        ?assert(false)
    end.

assert_no_channel_message() ->
    receive
        {channel_message, _Encoded} ->
            ?assert(false)
    after 20 ->
        ok
    end.

with_manager(Test) ->
    {ok, ManagerPid} = connection_gateway_manager:start_link(),
    unlink(ManagerPid),
    try Test()
    after
        gen_server:stop(ManagerPid)
    end.

spawn_subscriber() ->
    Parent = self(),
    spawn(fun() -> subscriber_loop(Parent) end).

stop_subscriber(Pid) ->
    Pid ! stop.

subscriber_loop(Parent) ->
    receive
        stop ->
            ok;
        {channel_message, Encoded} ->
            Parent ! {subscriber_message, self(), Encoded},
            subscriber_loop(Parent);
        Message ->
            Parent ! {unexpected_subscriber_message, self(), Message},
            subscriber_loop(Parent)
    end.
