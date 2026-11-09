-module(connection_gateway_manager).

-behaviour(gen_server).

-export([
    start_link/0,
    create_channel/1,
    register_connection/1,
    subscribe/2,
    publish_normal/3,
    publish_bad/3,
    complete_bad_workload/3,
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
    members := #{pid() => true},
    designated := pid() | undefined
}.
-type state() :: #{
    channels := #{binary() => channel()},
    connections := #{binary() => pid()},
    connection_ids := #{pid() => binary()},
    monitors := #{reference() => pid()},
    pid_monitors := #{pid() => reference()},
    busy_channels := #{binary() => pid()}
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

-spec publish_normal(binary(), pid(), binary()) -> ok | {error, term()}.
publish_normal(ChannelId, Pid, Message) ->
    gen_server:call(?MODULE, {publish_normal, ChannelId, Pid, Message}).

-spec publish_bad(binary(), pid(), binary()) -> ok | {error, term()}.
publish_bad(ChannelId, Pid, Message) ->
    gen_server:call(?MODULE, {publish_bad, ChannelId, Pid, Message}).

-spec complete_bad_workload(binary(), pid(), non_neg_integer()) -> ok.
complete_bad_workload(ChannelId, Pid, Checksum) ->
    gen_server:cast(
        ?MODULE,
        {complete_bad_workload, ChannelId, Pid, Checksum}
    ).

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
        pid_monitors => #{},
        busy_channels => #{}
    }}.

-spec handle_call(term(), term(), state()) -> {reply, term(), state()}.
handle_call({create_channel, ChannelId}, _From, State) ->
    Channels = maps:get(channels, State),
    case maps:is_key(ChannelId, Channels) of
        true ->
            {reply, {error, already_exists}, State};
        false ->
            Channel = #{members => #{}, designated => undefined},
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
            schedule_bad_workload(ChannelId, Channel, Pid, State);
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({disconnect, Pid}, _From, State) ->
    {reply, ok, remove_connection(Pid, State)};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(
    {complete_bad_workload, ChannelId, Pid, Checksum},
    State
) ->
    BusyChannels = maps:get(busy_channels, State),
    case maps:find(ChannelId, BusyChannels) of
        {ok, Pid} ->
            logger:debug(
                "Bad workload completed for channel ~p on ~p (checksum ~p)",
                [ChannelId, Pid, Checksum]
            ),
            {noreply, State#{
                busy_channels => maps:remove(ChannelId, BusyChannels)
            }};
        _ ->
            {noreply, State}
    end;
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
            Designated =
                case maps:get(designated, Channel) of
                    undefined -> Pid;
                    Existing -> Existing
                end,
            UpdatedChannel = Channel#{
                members => Members#{Pid => true},
                designated => Designated
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

schedule_bad_workload(ChannelId, Channel, PublisherPid, State) ->
    Designated = workload_subscriber(Channel, PublisherPid),
    BusyChannels = maps:get(busy_channels, State),
    case {Designated, maps:is_key(ChannelId, BusyChannels)} of
        {undefined, _} ->
            {reply, {error, no_designated_subscriber}, State};
        {_Pid, true} ->
            {reply, {error, workload_in_progress}, State};
        {Pid, false} ->
            Pid ! {run_bad_workload, ChannelId},
            {reply, ok, State#{
                busy_channels => BusyChannels#{ChannelId => Pid}
            }}
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

workload_subscriber(Channel, PublisherPid) ->
    Designated = maps:get(designated, Channel),
    Members = maps:get(members, Channel),
    case Designated =/= PublisherPid
        andalso maps:is_key(Designated, Members)
        andalso is_process_alive(Designated)
    of
        true ->
            Designated;
        false ->
            first_live_subscriber(maps:keys(Members), PublisherPid)
    end.

first_live_subscriber([], _PublisherPid) ->
    undefined;
first_live_subscriber([Pid | Remaining], PublisherPid) ->
    case Pid =/= PublisherPid andalso is_process_alive(Pid) of
        true -> Pid;
        false -> first_live_subscriber(Remaining, PublisherPid)
    end.

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
            BusyChannels = maps:filter(
                fun(_ChannelId, BusyPid) -> BusyPid =/= Pid end,
                maps:get(busy_channels, State)
            ),
            State#{
                channels => Channels,
                connections => maps:remove(ConnectionId, Connections),
                connection_ids => RemainingConnectionIds,
                monitors => maps:remove(Ref, maps:get(monitors, State)),
                pid_monitors => RemainingPidMonitors,
                busy_channels => BusyChannels
            }
    end.

remove_channel_member(Pid, Channel) ->
    Members = maps:remove(Pid, maps:get(members, Channel)),
    Designated =
        case maps:get(designated, Channel) of
            Pid -> first_member(Members);
            Existing -> Existing
        end,
    Channel#{members => Members, designated => Designated}.

first_member(Members) ->
    case maps:keys(Members) of
        [] -> undefined;
        [Pid | _] -> Pid
    end.
