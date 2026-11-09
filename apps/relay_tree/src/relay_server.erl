
-module(relay_server).

%% A single relay worker: maintains its local topology and forwards messages
%% to connected clients and child relays.

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

-type relay_state() :: #{
    id := term(),
    parent := pid() | undefined,
    children := [pid()],
    clients := #{pid() => reference()}
}.

-spec start(term()) -> {ok, pid()} | ignore | {error, term()}.
start(Id) ->
    gen_server:start(?MODULE, Id, []).

-spec start_link(term()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Id) ->
    gen_server:start_link(?MODULE, Id, []).

-spec get_state(pid()) -> relay_state().
get_state(Pid) ->
    gen_server:call(Pid, get_state).

-spec set_parent(pid(), pid() | undefined) -> ok.
set_parent(Pid, ParentPid) ->
    gen_server:call(Pid, {set_parent, ParentPid}).

-spec add_child(pid(), pid()) -> ok.
add_child(Pid, ChildPid) ->
    gen_server:call(Pid, {add_child, ChildPid}).

-spec remove_child(pid(), pid()) -> ok.
remove_child(Pid, ChildPid) ->
    gen_server:call(Pid, {remove_child, ChildPid}).

-spec add_client(pid(), pid()) -> ok | {error, term()}.
add_client(Pid, ClientPid) ->
    gen_server:call(Pid, {add_client, ClientPid}).

-spec remove_client(pid(), pid()) -> ok.
remove_client(Pid, ClientPid) ->
    gen_server:call(Pid, {remove_client, ClientPid}).

-spec broadcast(pid(), term()) -> ok.
broadcast(Pid, Message) ->
    gen_server:cast(Pid, {broadcast, Message}).

-spec init(term()) -> {ok, relay_state()}.
init(Id) ->
    State = #{
        id => Id,
        parent => undefined,
        children => [],
        clients => #{}
    },

    {ok, State}.

-spec handle_call(term(), term(), relay_state()) ->
    {reply, term(), relay_state()}.
handle_call(get_state, _From, State) ->
    {reply, State, State};

handle_call({set_parent, ParentPid}, _From, State) ->
    NewState = State#{
        parent => ParentPid
    },

    {reply, ok, NewState};

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

-spec handle_cast(term(), relay_state()) -> {noreply, relay_state()}.
handle_cast({broadcast, Message}, State) ->
    forward_message(Message, State),

    {noreply, State};

handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(term(), relay_state()) -> {noreply, relay_state()}.
handle_info({broadcast, Message}, State) ->
    forward_message(Message, State),

    {noreply, State};

handle_info(
    {'DOWN', MonitorRef, process, ClientPid, Reason},
    State
) ->

    Clients = maps:get(clients, State),

    case maps:find(ClientPid, Clients) of
        {ok, MonitorRef} ->
            logger:info(
                "Client ~p disconnected. Reason: ~p",
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

-spec forward_message(term(), relay_state()) -> ok.
forward_message(Message, State) ->

    RelayId = maps:get(id, State),
    Children = maps:get(children, State),
    Clients = maps:get(clients, State),

    logger:debug(
        "Relay ~p processing message: ~p",
        [RelayId, Message]
    ),

    maps:foreach(
        fun(ClientPid, _MonitorRef) ->
            ClientPid ! {comment, Message}
        end,
        Clients
    ),

    lists:foreach(
        fun(ChildPid) ->
            ChildPid ! {broadcast, Message}
        end,
        Children
    ),

    ok.

-spec terminate(term(), relay_state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), relay_state(), term()) -> {ok, relay_state()}.
code_change(_OldVersion, State, _Extra) ->
    {ok, State}.
