-module(relay_tree).

-behaviour(gen_server).

%% Owns the relay-tree lifecycle and coordinates client subscriptions and
%% broadcasts. Tree construction and relay recovery are delegated to focused
%% internal modules.

-export([
    start_link/0,
    create_tree/1,
    subscribe/1,
    get_root/0,
    get_leaves/0,
    get_relays/0,
    broadcast/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-type relay_info() :: #{
    pid := pid(),
    id := term(),
    level := non_neg_integer(),
    parent := pid() | undefined,
    children := [pid()],
    monitor := reference()
}.
-type client_info() :: #{leaf := pid(), monitor := reference()}.
-type state() :: #{
    root := pid() | undefined,
    leaves := [pid()],
    relays := #{pid() => relay_info()},
    relay_monitors := #{reference() => pid()},
    clients := #{pid() => client_info()},
    client_monitors := #{reference() => pid()},
    next_leaf := non_neg_integer()
}.

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create_tree(integer()) -> {ok, [pid()]} | {error, term()}.
create_tree(Layers) ->
    gen_server:call(?MODULE, {create_tree, Layers}, infinity).

-spec subscribe(pid()) -> {ok, pid()} | {error, term()}.
subscribe(ClientPid) ->
    gen_server:call(?MODULE, {subscribe, ClientPid}).

-spec get_root() -> pid() | undefined.
get_root() ->
    gen_server:call(?MODULE, get_root).

-spec get_leaves() -> [pid()].
get_leaves() ->
    gen_server:call(?MODULE, get_leaves).

-spec get_relays() -> #{pid() => relay_info()}.
get_relays() ->
    gen_server:call(?MODULE, get_relays).

-spec broadcast(term()) -> ok | {error, term()}.
broadcast(Message) ->
    gen_server:call(?MODULE, {broadcast, Message}).

-spec init([]) -> {ok, state()}.
init([]) ->
    {ok, #{
        root => undefined,
        leaves => [],
        relays => #{},
        relay_monitors => #{},
        clients => #{},
        client_monitors => #{},
        next_leaf => 0
    }}.

-spec handle_call(term(), term(), state()) ->
    {reply, term(), state()}.
handle_call({create_tree, Layers}, _From, State)
    when is_integer(Layers), Layers >= 0, Layers =< 10 ->
    stop_existing_tree(State),
    {Root, Leaves, Relays, RelayMonitors} = relay_tree_builder:build(Layers),
    NewState = State#{
        root => Root,
        leaves => Leaves,
        relays => Relays,
        relay_monitors => RelayMonitors,
        clients => #{},
        client_monitors => #{},
        next_leaf => 0
    },

    logger:info(
        "Created relay tree. Relays: ~p, leaves: ~p",
        [map_size(Relays), length(Leaves)]
    ),
    {reply, {ok, Leaves}, NewState};

handle_call({create_tree, _Layers}, _From, State) ->
    {reply, {error, invalid_layers}, State};

handle_call({subscribe, ClientPid}, _From, State)
    when is_pid(ClientPid) ->
    subscribe_client(ClientPid, State);

handle_call({subscribe, _ClientPid}, _From, State) ->
    {reply, {error, invalid_client}, State};

handle_call(get_root, _From, State) ->
    {reply, maps:get(root, State), State};

handle_call(get_leaves, _From, State) ->
    {reply, maps:get(leaves, State), State};

handle_call(get_relays, _From, State) ->
    {reply, maps:get(relays, State), State};

handle_call({broadcast, Message}, _From, State) ->
    case maps:get(root, State) of
        undefined ->
            {reply, {error, no_tree}, State};
        Root ->
            case is_process_alive(Root) of
                true ->
                    relay_server:broadcast(Root, Message),
                    {reply, ok, State};
                false ->
                    {reply, {error, root_unavailable}, State}
            end
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info({'DOWN', Ref, process, Pid, Reason}, State) ->
    case maps:find(Ref, maps:get(relay_monitors, State)) of
        {ok, Pid} ->
            logger:error(
                "Relay ~p failed: ~p. Recovering subtree",
                [Pid, Reason]
            ),
            {noreply, relay_tree_recovery:recover(Pid, State)};
        _ ->
            handle_client_down(Ref, Pid, State)
    end;
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

subscribe_client(ClientPid, State) ->
    Clients = maps:get(clients, State),
    case maps:find(ClientPid, Clients) of
        {ok, ClientInfo} ->
            {reply, {ok, maps:get(leaf, ClientInfo)}, State};
        error ->
            subscribe_new_client(ClientPid, State)
    end.

subscribe_new_client(ClientPid, State) ->
    case maps:get(leaves, State) of
        [] ->
            {reply, {error, no_tree}, State};
        Leaves ->
            %% Distribute new clients evenly across the current leaves.
            Index = maps:get(next_leaf, State),
            Leaf = lists:nth((Index rem length(Leaves)) + 1, Leaves),
            add_client_to_leaf(ClientPid, Leaf, Index, State)
    end.

add_client_to_leaf(ClientPid, Leaf, Index, State) ->
    try relay_server:add_client(Leaf, ClientPid) of
        ok ->
            add_client_record(ClientPid, Leaf, Index, State);
        {error, Reason} ->
            logger:error(
                "Could not subscribe client ~p to leaf ~p: ~p",
                [ClientPid, Leaf, Reason]
            ),
            {reply, {error, leaf_unavailable}, State}
    catch
        exit:Reason ->
            logger:error(
                "Leaf ~p failed while subscribing client ~p: ~p",
                [Leaf, ClientPid, Reason]
            ),
            {reply, {error, leaf_unavailable}, State}
    end.

add_client_record(ClientPid, Leaf, Index, State) ->
    ClientRef = erlang:monitor(process, ClientPid),
    ClientInfo = #{leaf => Leaf, monitor => ClientRef},
    Clients = maps:get(clients, State),
    ClientMonitors = maps:get(client_monitors, State),
    NewState = State#{
        clients => Clients#{ClientPid => ClientInfo},
        client_monitors => ClientMonitors#{ClientRef => ClientPid},
        next_leaf => Index + 1
    },
    {reply, {ok, Leaf}, NewState}.

handle_client_down(Ref, Pid, State) ->
    case maps:find(Ref, maps:get(client_monitors, State)) of
        {ok, Pid} ->
            logger:info("Tree manager detected client disconnect: ~p", [Pid]),
            {noreply, remove_client_record(Pid, Ref, State)};
        _ ->
            {noreply, State}
    end.

remove_client_record(ClientPid, Ref, State) ->
    Clients = maps:get(clients, State),
    ClientMonitors = maps:get(client_monitors, State),

    case maps:find(ClientPid, Clients) of
        {ok, ClientInfo} ->
            Leaf = maps:get(leaf, ClientInfo),
            case is_process_alive(Leaf) of
                true ->
                    _ = catch relay_server:remove_client(Leaf, ClientPid),
                    ok;
                false ->
                    ok
            end;
        error ->
            ok
    end,

    State#{
        clients => maps:remove(ClientPid, Clients),
        client_monitors => maps:remove(Ref, ClientMonitors)
    }.

%% Stop old relay workers and discard their monitors before replacing a tree.
stop_existing_tree(State) ->
    maps:foreach(
        fun(_Pid, Info) ->
            erlang:demonitor(maps:get(monitor, Info), [flush])
        end,
        maps:get(relays, State)
    ),
    maps:foreach(
        fun(_Pid, Info) ->
            erlang:demonitor(maps:get(monitor, Info), [flush])
        end,
        maps:get(clients, State)
    ),
    maps:foreach(
        fun(Pid, _Info) ->
            _ = catch gen_server:stop(Pid)
        end,
        maps:get(relays, State)
    ),
    ok.
