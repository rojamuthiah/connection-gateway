-module(connection_gateway_manager).

-behaviour(gen_server).

-export([
    start_link/0,
    create_channel/1,
    register_connection/1,
    subscribe/2,
    validate_publish/2,
    publish_normal/3,
    publish_bad/3,
    disconnect/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-type channel() :: #{
    members := #{pid() => true}
}.
-type state() :: #{
    channels := #{binary() => channel()},
    connections := #{binary() => pid()},
    connection_ids := #{pid() => binary()},
    monitors := #{reference() => pid()},
    pid_monitors := #{pid() => reference()}
}.

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create_channel(binary()) -> ok | {error, already_exists}.
create_channel(ChannelId) ->
    gen_server:call(?MODULE, {create_channel, ChannelId}).

-spec register_connection(pid()) -> {ok, binary()} | {error, term()}.
register_connection(Pid) ->
    gen_server:call(?MODULE, {register_connection, Pid}).

-spec subscribe(binary(), pid()) -> {ok, binary()} | {error, term()}.
subscribe(ChannelId, Pid) ->
    gen_server:call(?MODULE, {subscribe, ChannelId, Pid}).

-spec validate_publish(binary(), pid()) -> ok | {error, term()}.
validate_publish(ChannelId, Pid) ->
    gen_server:call(?MODULE, {validate_publish, ChannelId, Pid}).

-spec publish_normal(binary(), pid(), binary()) -> ok | {error, term()}.
publish_normal(ChannelId, Pid, Message) ->
    gen_server:call(?MODULE, {publish_normal, ChannelId, Pid, Message}).

-spec publish_bad(binary(), pid(), binary()) -> ok | {error, term()}.
publish_bad(ChannelId, Pid, Message) ->
    gen_server:call(?MODULE, {publish_bad, ChannelId, Pid, Message}).

-spec disconnect(pid()) -> ok.
disconnect(Pid) ->
    gen_server:call(?MODULE, {disconnect, Pid}).

-spec init([]) -> {ok, state()}.
init([]) ->
    {ok, #{
        channels => #{},
        connections => #{},
        connection_ids => #{},
        monitors => #{},
        pid_monitors => #{}
    }}.

-spec handle_call(term(), term(), state()) -> {reply, term(), state()}.
handle_call({create_channel, ChannelId}, _From, State) ->
    Channels = maps:get(channels, State),
    case maps:is_key(ChannelId, Channels) of
        true ->
            {reply, {error, already_exists}, State};
        false ->
            Channel = #{members => #{}},
            {reply, ok, State#{channels => Channels#{ChannelId => Channel}}}
    end;

handle_call({register_connection, Pid}, _From, State)
    when is_pid(Pid) ->
    case maps:find(Pid, maps:get(connection_ids, State)) of
        {ok, ConnectionId} ->
            {reply, {ok, ConnectionId}, State};
        error ->
            register_new_connection(Pid, State)
    end;

handle_call({subscribe, ChannelId, Pid}, _From, State) ->
    subscribe_connection(ChannelId, Pid, State);

handle_call({validate_publish, ChannelId, Pid}, _From, State) ->
    case subscribed_channel(ChannelId, Pid, State) of
        {ok, _Channel} ->
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(
    {publish_normal, ChannelId, Pid, Message},
    _From,
    State
) ->
    case subscribed_channel(ChannelId, Pid, State) of
        {ok, Channel} ->
            Event = iolist_to_binary(json:encode(#{
                <<"type">> => <<"message">>,
                <<"channelId">> => ChannelId,
                <<"message">> => Message
            })),
            broadcast_to_subscribers(Channel, Pid, Event),
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(
    {publish_bad, ChannelId, Pid, Message},
    _From,
    State
) ->
    case subscribed_channel(ChannelId, Pid, State) of
        {ok, Channel} ->
            Event = iolist_to_binary(json:encode(#{
                <<"type">> => <<"message">>,
                <<"channelId">> => ChannelId,
                <<"message">> => Message
            })),
            broadcast_to_subscribers(Channel, Pid, Event),
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({disconnect, Pid}, _From, State) ->
    {reply, ok, remove_connection(Pid, State)};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case maps:find(Ref, maps:get(monitors, State)) of
        {ok, Pid} ->
            {noreply, remove_connection(Pid, State)};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

register_new_connection(Pid, State) ->
    case is_process_alive(Pid) of
        false ->
            {reply, {error, connection_not_alive}, State};
        true ->
            ConnectionId = connection_id(),
            Ref = erlang:monitor(process, Pid),
            {reply, {ok, ConnectionId}, State#{
                connections =>
                    (maps:get(connections, State))#{ConnectionId => Pid},
                connection_ids =>
                    (maps:get(connection_ids, State))#{Pid => ConnectionId},
                monitors => (maps:get(monitors, State))#{Ref => Pid},
                pid_monitors =>
                    (maps:get(pid_monitors, State))#{Pid => Ref}
            }}
    end.

connection_id() ->
    Number = erlang:unique_integer([positive, monotonic]),
    <<"conn-", (integer_to_binary(Number))/binary>>.

subscribe_connection(ChannelId, Pid, State) ->
    Channels = maps:get(channels, State),
    case {
        maps:find(ChannelId, Channels),
        maps:find(Pid, maps:get(connection_ids, State))
    } of
        {error, _} ->
            {reply, {error, unknown_channel}, State};
        {_, error} ->
            {reply, {error, unknown_connection}, State};
        {{ok, Channel}, {ok, ConnectionId}} ->
            Members = maps:get(members, Channel),
            UpdatedChannel = Channel#{
                members => Members#{Pid => true}
            },
            {reply, {ok, ConnectionId}, State#{
                channels => Channels#{ChannelId => UpdatedChannel}
            }}
    end.

subscribed_channel(ChannelId, Pid, State) ->
    case maps:find(ChannelId, maps:get(channels, State)) of
        error ->
            {error, unknown_channel};
        {ok, Channel} ->
            case maps:is_key(Pid, maps:get(members, Channel)) of
                true -> {ok, Channel};
                false -> {error, not_subscribed}
            end
    end.

broadcast_to_subscribers(Channel, PublisherPid, Event) ->
    maps:foreach(
        fun(ConnectionPid, _Present) when ConnectionPid =/= PublisherPid ->
            ConnectionPid ! {channel_message, Event};
           (_ConnectionPid, _Present) ->
            ok
        end,
        maps:get(members, Channel)
    ).

remove_connection(Pid, State) ->
    PidMonitors = maps:get(pid_monitors, State),
    case maps:take(Pid, PidMonitors) of
        error ->
            State;
        {Ref, RemainingPidMonitors} ->
            erlang:demonitor(Ref, [flush]),
            ConnectionIds = maps:get(connection_ids, State),
            {ConnectionId, RemainingConnectionIds} =
                maps:take(Pid, ConnectionIds),
            Connections = maps:get(connections, State),
            Channels = maps:map(
                fun(_ChannelId, Channel) ->
                    remove_channel_member(Pid, Channel)
                end,
                maps:get(channels, State)
            ),
            State#{
                channels => Channels,
                connections => maps:remove(ConnectionId, Connections),
                connection_ids => RemainingConnectionIds,
                monitors => maps:remove(Ref, maps:get(monitors, State)),
                pid_monitors => RemainingPidMonitors
            }
    end.

remove_channel_member(Pid, Channel) ->
    Members = maps:remove(Pid, maps:get(members, Channel)),
    Channel#{members => Members}.
