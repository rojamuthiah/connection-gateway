
-module(relay_server).

-behaviour(gen_server).

-export([
    start/1,
    start_link/1,
    get_state/1,
    set_parent/2,
    add_child/2,
    remove_child/2,
    add_client/2,
    remove_client/2,
    broadcast/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% ==========================================
%% Public API
%% ==========================================

start(Id) ->
    gen_server:start(?MODULE, Id, []).

start_link(Id) ->
    gen_server:start_link(?MODULE, Id, []).

get_state(Pid) ->
    gen_server:call(Pid, get_state).

set_parent(Pid, ParentPid) ->
    gen_server:call(Pid, {set_parent, ParentPid}).

add_child(Pid, ChildPid) ->
    gen_server:call(Pid, {add_child, ChildPid}).

remove_child(Pid, ChildPid) ->
    gen_server:call(Pid, {remove_child, ChildPid}).

add_client(Pid, ClientPid) ->
    gen_server:call(Pid, {add_client, ClientPid}).

remove_client(Pid, ClientPid) ->
    gen_server:call(Pid, {remove_client, ClientPid}).

broadcast(Pid, Message) ->
    gen_server:cast(Pid, {broadcast, Message}).

%% ==========================================
%% Initialization
%% ==========================================

init(Id) ->
    State = #{
        id => Id,
        parent => undefined,
        children => [],
        clients => #{}
    },

    {ok, State}.

%% ==========================================
%% Synchronous Requests
%% ==========================================

handle_call(get_state, _From, State) ->
    {reply, State, State};

%% Update parent relay

handle_call({set_parent, ParentPid}, _From, State) ->
    NewState = State#{
        parent => ParentPid
    },

    {reply, ok, NewState};

%% Add child relay

handle_call({add_child, ChildPid}, _From, State) ->
    Children = maps:get(children, State),

    NewChildren =
        case lists:member(ChildPid, Children) of
            true ->
                Children;

            false ->
                Children ++ [ChildPid]
        end,

    NewState = State#{
        children => NewChildren
    },

    {reply, ok, NewState};

%% Remove child relay

handle_call({remove_child, ChildPid}, _From, State) ->
    Children = maps:get(children, State),

    NewChildren = lists:delete(
        ChildPid,
        Children
    ),

    NewState = State#{
        children => NewChildren
    },

    {reply, ok, NewState};

%% Register WebSocket client and monitor it

handle_call({add_client, ClientPid}, _From, State)
    when is_pid(ClientPid) ->

    Clients = maps:get(clients, State),

    case maps:is_key(ClientPid, Clients) of
        true ->
            {reply, ok, State};

        false ->
            case is_process_alive(ClientPid) of
                false ->
                    {reply, {error, client_not_alive}, State};

                true ->
                    MonitorRef = erlang:monitor(
                        process,
                        ClientPid
                    ),

                    NewClients = Clients#{
                        ClientPid => MonitorRef
                    },

                    NewState = State#{
                        clients => NewClients
                    },

                    {reply, ok, NewState}
            end
    end;

%% Explicitly remove WebSocket client

handle_call({remove_client, ClientPid}, _From, State) ->
    Clients = maps:get(clients, State),

    case maps:take(ClientPid, Clients) of
        error ->
            {reply, ok, State};

        {MonitorRef, RemainingClients} ->
            erlang:demonitor(
                MonitorRef,
                [flush]
            ),

            NewState = State#{
                clients => RemainingClients
            },

            {reply, ok, NewState}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% ==========================================
%% Broadcast Handling
%% ==========================================

%% Root receives broadcast through gen_server:cast

handle_cast({broadcast, Message}, State) ->
    forward_message(Message, State),

    {noreply, State};

handle_cast(_Request, State) ->
    {noreply, State}.

%% Child relays receive broadcast through process messages

handle_info({broadcast, Message}, State) ->
    forward_message(Message, State),

    {noreply, State};

%% ==========================================
%% Client Disconnection Detection
%% ==========================================

handle_info(
    {'DOWN', MonitorRef, process, ClientPid, Reason},
    State
) ->

    Clients = maps:get(clients, State),

    case maps:find(ClientPid, Clients) of
        {ok, MonitorRef} ->
            io:format(
                "Client ~p disconnected. Reason: ~p~n",
                [ClientPid, Reason]
            ),

            NewClients = maps:remove(
                ClientPid,
                Clients
            ),

            NewState = State#{
                clients => NewClients
            },

            {noreply, NewState};

        _ ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%% ==========================================
%% Message Propagation
%% ==========================================

forward_message(Message, State) ->

    RelayId = maps:get(id, State),
    Children = maps:get(children, State),
    Clients = maps:get(clients, State),

    io:format(
        "Relay ~p processing message: ~p~n",
        [RelayId, Message]
    ),

    %% Deliver message to connected WebSocket clients

    maps:foreach(
        fun(ClientPid, _MonitorRef) ->
            ClientPid ! {comment, Message}
        end,
        Clients
    ),

    %% Forward message to child relays

    lists:foreach(
        fun(ChildPid) ->
            ChildPid ! {broadcast, Message}
        end,
        Children
    ),

    ok.

%% ==========================================
%% Termination
%% ==========================================

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.
