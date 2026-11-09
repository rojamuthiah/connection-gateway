
-module(relay_tree).

-behaviour(gen_server).

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

%% Public API

start_link() ->
    gen_server:start_link(
        {local, ?MODULE},
        ?MODULE,
        [],
        []
    ).

create_tree(Layers) ->
    gen_server:call(
        ?MODULE,
        {create_tree, Layers},
        infinity
    ).

subscribe(ClientPid) ->
    gen_server:call(
        ?MODULE,
        {subscribe, ClientPid}
    ).

get_root() ->
    gen_server:call(?MODULE, get_root).

get_leaves() ->
    gen_server:call(?MODULE, get_leaves).

get_relays() ->
    gen_server:call(?MODULE, get_relays).

broadcast(Message) ->
    gen_server:call(
        ?MODULE,
        {broadcast, Message}
    ).

%% Initial state

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

%% Create tree

handle_call({create_tree, Layers}, _From, State)
    when is_integer(Layers), Layers >= 0, Layers =< 10 ->

    stop_existing_tree(State),

    {Root, Leaves, RelayInfos} =
        build_tree(0, Layers, undefined, 1),

    RelayMap = maps:from_list([
        {maps:get(pid, Info), Info}
        || Info <- RelayInfos
    ]),

    MonitorMap = maps:from_list([
        {maps:get(monitor, Info), maps:get(pid, Info)}
        || Info <- RelayInfos
    ]),

    NewState = State#{
        root => Root,
        leaves => Leaves,
        relays => RelayMap,
        relay_monitors => MonitorMap,
        clients => #{},
        client_monitors => #{},
        next_leaf => 0
    },

    io:format(
        "Created relay tree. Relays: ~p, leaves: ~p~n",
        [map_size(RelayMap), length(Leaves)]
    ),

    {reply, {ok, Leaves}, NewState};

handle_call({create_tree, _Layers}, _From, State) ->
    {reply, {error, invalid_layers}, State};

%% Subscribe WebSocket process to a leaf

handle_call({subscribe, ClientPid}, _From, State)
    when is_pid(ClientPid) ->

    Clients = maps:get(clients, State),

    case maps:is_key(ClientPid, Clients) of
        true ->
            Existing = maps:get(ClientPid, Clients),
            {reply, {ok, maps:get(leaf, Existing)}, State};

        false ->
            Leaves = maps:get(leaves, State),

            case Leaves of
                [] ->
                    {reply, {error, no_tree}, State};

                _ ->
                    Index = maps:get(next_leaf, State),
                    Leaf = lists:nth(
                        (Index rem length(Leaves)) + 1,
                        Leaves
                    ),

                    case catch relay_server:add_client(
                        Leaf,
                        ClientPid
                    ) of
                        ok ->
                            ClientRef = erlang:monitor(
                                process,
                                ClientPid
                            ),

                            ClientInfo = #{
                                leaf => Leaf,
                                monitor => ClientRef
                            },

                            NewState = State#{
                                clients => Clients#{
                                    ClientPid => ClientInfo
                                },
                                client_monitors =>
                                    (maps:get(client_monitors, State))#{
                                        ClientRef => ClientPid
                                    },
                                next_leaf => Index + 1
                            },

                            {reply, {ok, Leaf}, NewState};

                        _ ->
                            {reply, {error, leaf_unavailable}, State}
                    end
            end
    end;

handle_call({subscribe, _}, _From, State) ->
    {reply, {error, invalid_client}, State};

%% Get root

handle_call(get_root, _From, State) ->
    {reply, maps:get(root, State), State};

%% Get leaves

handle_call(get_leaves, _From, State) ->
    {reply, maps:get(leaves, State), State};

%% Get relay information

handle_call(get_relays, _From, State) ->
    {reply, maps:get(relays, State), State};

%% Broadcast from root

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

handle_cast(_Request, State) ->
    {noreply, State}.

%% Detect relay or client failure

handle_info(
    {'DOWN', Ref, process, Pid, Reason},
    State
) ->
    RelayMonitors = maps:get(relay_monitors, State),
    ClientMonitors = maps:get(client_monitors, State),

    case maps:find(Ref, RelayMonitors) of
        {ok, Pid} ->
            io:format(
                "Relay ~p failed: ~p. Recovering subtree...~n",
                [Pid, Reason]
            ),

            NewState = recover_relay(Pid, State),
            {noreply, NewState};

        _ ->
            case maps:find(Ref, ClientMonitors) of
                {ok, Pid} ->
                    io:format(
                        "Tree manager detected client disconnect: ~p~n",
                        [Pid]
                    ),

                    NewState = remove_client_record(Pid, Ref, State),
                    {noreply, NewState};

                _ ->
                    {noreply, State}
            end
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% Build the binary tree recursively

build_tree(Level, MaxLevel, ParentPid, Index) ->
    Id = {relay, Level, Index},

    {ok, Pid} = relay_server:start(Id),
    Ref = erlang:monitor(process, Pid),

    case ParentPid of
        undefined ->
            ok;

        _ ->
            ok = relay_server:set_parent(Pid, ParentPid),
            ok = relay_server:add_child(ParentPid, Pid)
    end,

    case Level =:= MaxLevel of
        true ->
            Info = #{
                pid => Pid,
                id => Id,
                level => Level,
                parent => ParentPid,
                children => [],
                monitor => Ref
            },

            {Pid, [Pid], [Info]};

        false ->
            {_LeftPid, LeftLeaves, LeftInfos} =
                build_tree(
                    Level + 1,
                    MaxLevel,
                    Pid,
                    Index * 2 - 1
                ),

            {_RightPid, RightLeaves, RightInfos} =
                build_tree(
                    Level + 1,
                    MaxLevel,
                    Pid,
                    Index * 2
                ),

            ChildPids = [
                maps:get(pid, Info)
                || Info <- LeftInfos ++ RightInfos,
                   maps:get(parent, Info) =:= Pid
            ],

            Info = #{
                pid => Pid,
                id => Id,
                level => Level,
                parent => ParentPid,
                children => ChildPids,
                monitor => Ref
            },

            {
                Pid,
                LeftLeaves ++ RightLeaves,
                [Info | LeftInfos ++ RightInfos]
            }
    end.

%% Recover one failed relay and reconnect its surviving children

recover_relay(FailedPid, State) ->
    Relays = maps:get(relays, State),

    case maps:find(FailedPid, Relays) of
        error ->
            State;

        {ok, FailedInfo} ->
            ParentPid = maps:get(parent, FailedInfo),
            ChildPids = maps:get(children, FailedInfo),
            Id = maps:get(id, FailedInfo),
            Level = maps:get(level, FailedInfo),

            %% Start replacement relay with same logical ID
            case relay_server:start(Id) of
                {ok, NewPid} ->
                    NewRef = erlang:monitor(process, NewPid),

                    %% Reconnect replacement to its parent
                    case ParentPid of
                        undefined ->
                            ok;

                        _ ->
                            case is_process_alive(ParentPid) of
                                true ->
                                    catch relay_server:remove_child(
                                        ParentPid,
                                        FailedPid
                                    ),
                                    catch relay_server:add_child(
                                        ParentPid,
                                        NewPid
                                    ),
                                    ok = relay_server:set_parent(
                                        NewPid,
                                        ParentPid
                                    );

                                false ->
                                    ok
                            end
                    end,

                    %% Reconnect surviving children
                    SurvivingChildren = reconnect_children(
                        NewPid,
                        ChildPids
                    ),

                    %% Update replacement metadata
                    NewInfo = FailedInfo#{
                        pid => NewPid,
                        children => SurvivingChildren,
                        monitor => NewRef
                    },

                    RelaysWithoutOld = maps:remove(
                        FailedPid,
                        Relays
                    ),

                    NewRelays = RelaysWithoutOld#{
                        NewPid => NewInfo
                    },

                    %% Update child metadata with new parent
                    UpdatedRelays = lists:foldl(
                        fun(ChildPid, Acc) ->
                            case maps:find(ChildPid, Acc) of
                                {ok, ChildInfo} ->
                                    Acc#{
                                        ChildPid => ChildInfo#{
                                            parent => NewPid
                                        }
                                    };

                                error ->
                                    Acc
                            end
                        end,
                        NewRelays,
                        SurvivingChildren
                    ),

                    OldMonitor = maps:get(monitor, FailedInfo),

                    NewMonitorMap = maps:remove(
                        OldMonitor,
                        maps:get(relay_monitors, State)
                    ),

                    NewMonitorMap2 = NewMonitorMap#{
                        NewRef => NewPid
                    },

                    %% Replace leaf PID if the failed relay was a leaf
                    NewLeaves = replace_leaf(
                        FailedPid,
                        NewPid,
                        maps:get(leaves, State)
                    ),

                    %% Reattach clients that were on the failed leaf
                    NewClients = reattach_clients(
                        FailedPid,
                        NewPid,
                        maps:get(clients, State)
                    ),

                    io:format(
                        "Recovered relay ~p as ~p at level ~p~n",
                        [FailedPid, NewPid, Level]
                    ),

                    NewRoot =
                        case maps:get(root, State) of
                            FailedPid -> NewPid;
                            ExistingRoot -> ExistingRoot
                        end,

                    State#{
                        root => NewRoot,
                        leaves => NewLeaves,
                        relays => UpdatedRelays,
                        relay_monitors => NewMonitorMap2,
                        clients => NewClients
                    };

                {error, Reason} ->
                    io:format(
                        "Could not recover relay ~p: ~p~n",
                        [FailedPid, Reason]
                    ),
                    State
            end
    end.

%% Reconnect surviving children to replacement

reconnect_children(NewParentPid, ChildPids) ->
    lists:filter(
        fun(ChildPid) ->
            case is_process_alive(ChildPid) of
                true ->
                    case catch relay_server:set_parent(
                        ChildPid,
                        NewParentPid
                    ) of
                        ok ->
                            case catch relay_server:add_child(
                                NewParentPid,
                                ChildPid
                            ) of
                                ok -> true;
                                _ -> false
                            end;

                        _ ->
                            false
                    end;

                false ->
                    false
            end
        end,
        ChildPids
    ).

%% Replace failed leaf PID in leaves list

replace_leaf(OldPid, NewPid, Leaves) ->
    [
        case Pid of
            OldPid -> NewPid;
            _ -> Pid
        end
        || Pid <- Leaves
    ].

%% Reattach clients from a failed leaf to its replacement

reattach_clients(OldLeaf, NewLeaf, Clients) ->
    maps:map(
        fun(ClientPid, ClientInfo) ->
            case maps:get(leaf, ClientInfo) of
                OldLeaf ->
                    case is_process_alive(ClientPid) of
                        true ->
                            case catch relay_server:add_client(
                                NewLeaf,
                                ClientPid
                            ) of
                                ok ->
                                    ClientInfo#{leaf => NewLeaf};

                                _ ->
                                    ClientInfo
                            end;

                        false ->
                            ClientInfo
                    end;

                _ ->
                    ClientInfo
            end
        end,
        Clients
    ).

%% Remove disconnected client from manager records

remove_client_record(ClientPid, Ref, State) ->
    Clients = maps:get(clients, State),
    ClientMonitors = maps:get(client_monitors, State),

    case maps:find(ClientPid, Clients) of
        {ok, ClientInfo} ->
            Leaf = maps:get(leaf, ClientInfo),

            case is_process_alive(Leaf) of
                true ->
                    catch relay_server:remove_client(
                        Leaf,
                        ClientPid
                    );

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

%% Stop previous tree

stop_existing_tree(State) ->
    maps:foreach(
        fun(_Pid, Info) ->
            erlang:demonitor(
                maps:get(monitor, Info),
                [flush]
            )
        end,
        maps:get(relays, State)
    ),

    maps:foreach(
        fun(_Pid, Info) ->
            erlang:demonitor(
                maps:get(monitor, Info),
                [flush]
            )
        end,
        maps:get(clients, State)
    ),

    maps:foreach(
        fun(Pid, _Info) ->
            catch gen_server:stop(Pid)
        end,
        maps:get(relays, State)
    ),

    ok.
