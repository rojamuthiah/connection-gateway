
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

%% Public API

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

%% Initialization

init(Id) ->
    State = #{
        id => Id,
        parent => undefined,
        children => [],
        clients => []
    },
    {ok, State}.

%% Synchronous requests

handle_call(get_state, _From, State) ->
    {reply, State, State};

handle_call({set_parent, ParentPid}, _From, State) ->
    {reply, ok, State#{parent => ParentPid}};

handle_call({add_child, ChildPid}, _From, State) ->
    Children = maps:get(children, State),

    NewChildren =
        case lists:member(ChildPid, Children) of
            true -> Children;
            false -> Children ++ [ChildPid]
        end,

    {reply, ok, State#{children => NewChildren}};

handle_call({remove_child, ChildPid}, _From, State) ->
    Children = maps:get(children, State),
    NewChildren = lists:delete(ChildPid, Children),

    {reply, ok, State#{children => NewChildren}};

handle_call({add_client, ClientPid}, _From, State)
    when is_pid(ClientPid) ->

    Clients = maps:get(clients, State),

    NewClients =
        case lists:member(ClientPid, Clients) of
            true -> Clients;
            false -> Clients ++ [ClientPid]
        end,

    {reply, ok, State#{clients => NewClients}};

handle_call({remove_client, ClientPid}, _From, State) ->
    Clients = maps:get(clients, State),
    NewClients = lists:delete(ClientPid, Clients),

    {reply, ok, State#{clients => NewClients}};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% Asynchronous broadcast initiated through gen_server:cast

handle_cast({broadcast, Message}, State) ->
    forward_message(Message, State),
    {noreply, State};

handle_cast(_Request, State) ->
    {noreply, State}.

%% Messages received directly from parent relays

handle_info({broadcast, Message}, State) ->
    forward_message(Message, State),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

%% Deliver to clients and forward to child relays

forward_message(Message, State) ->
    RelayId = maps:get(id, State),
    Children = maps:get(children, State),
    Clients = maps:get(clients, State),

    io:format(
        "Relay ~p processing message: ~p~n",
        [RelayId, Message]
    ),

    %% Deliver to connected clients
    lists:foreach(
        fun(ClientPid) ->
            ClientPid ! {comment, Message}
        end,
        Clients
    ),

    %% Forward to child relays
    lists:foreach(
        fun(ChildPid) ->
            ChildPid ! {broadcast, Message}
        end,
        Children
    ),

    ok.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.
