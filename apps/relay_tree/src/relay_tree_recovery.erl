-module(relay_tree_recovery).

%% Replaces a failed relay and reconnects its surviving children and clients.

-export([recover/2]).

-spec recover(pid(), map()) -> map().
recover(FailedPid, State) ->
    Relays = maps:get(relays, State),

    case maps:find(FailedPid, Relays) of
        error ->
            State;
        {ok, FailedInfo} ->
            recover_relay(FailedPid, FailedInfo, State)
    end.

recover_relay(FailedPid, FailedInfo, State) ->
    ParentPid = maps:get(parent, FailedInfo),
    ChildPids = maps:get(children, FailedInfo),
    Id = maps:get(id, FailedInfo),
    Level = maps:get(level, FailedInfo),

    case relay_server:start(Id) of
        {ok, NewPid} ->
            NewRef = erlang:monitor(process, NewPid),
            reconnect_parent(ParentPid, FailedPid, NewPid),
            SurvivingChildren = reconnect_children(NewPid, ChildPids),

            NewInfo = FailedInfo#{
                pid => NewPid,
                children => SurvivingChildren,
                monitor => NewRef
            },

            RelaysWithoutOld = maps:remove(
                FailedPid,
                maps:get(relays, State)
            ),
            NewRelays = RelaysWithoutOld#{NewPid => NewInfo},
            UpdatedRelays = update_child_parents(
                NewPid,
                SurvivingChildren,
                NewRelays
            ),

            OldMonitor = maps:get(monitor, FailedInfo),
            RelayMonitors = maps:remove(
                OldMonitor,
                maps:get(relay_monitors, State)
            ),
            NewRelayMonitors = RelayMonitors#{NewRef => NewPid},

            NewLeaves = replace_leaf(
                FailedPid,
                NewPid,
                maps:get(leaves, State)
            ),
            NewClients = reattach_clients(
                FailedPid,
                NewPid,
                maps:get(clients, State),
                NewLeaves
            ),

            logger:info(
                "Recovered relay ~p as ~p at level ~p",
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
                relay_monitors => NewRelayMonitors,
                clients => NewClients
            };
        {error, Reason} ->
            logger:error(
                "Could not recover relay ~p: ~p",
                [FailedPid, Reason]
            ),
            State
    end.

reconnect_parent(undefined, _FailedPid, _NewPid) ->
    ok;
reconnect_parent(ParentPid, FailedPid, NewPid) ->
    case is_process_alive(ParentPid) of
        true ->
            _ = catch relay_server:remove_child(ParentPid, FailedPid),
            _ = catch relay_server:add_child(ParentPid, NewPid),
            ok = relay_server:set_parent(NewPid, ParentPid);
        false ->
            ok
    end.

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

update_child_parents(NewParentPid, ChildPids, Relays) ->
    lists:foldl(
        fun(ChildPid, Acc) ->
            case maps:find(ChildPid, Acc) of
                {ok, ChildInfo} ->
                    Acc#{ChildPid => ChildInfo#{parent => NewParentPid}};
                error ->
                    Acc
            end
        end,
        Relays,
        ChildPids
    ).

replace_leaf(OldPid, NewPid, Leaves) ->
    [
        case Pid of
            OldPid -> NewPid;
            _ -> Pid
        end
        || Pid <- Leaves
    ].

reattach_clients(OldLeaf, NewLeaf, Clients, Leaves) ->
    Loads = client_loads(OldLeaf, Leaves, Clients),
    {UpdatedClients, _UpdatedLoads} = maps:fold(
        fun(ClientPid, ClientInfo, {ClientsAcc, LoadsAcc}) ->
            case maps:get(leaf, ClientInfo) =:= OldLeaf
                andalso is_process_alive(ClientPid)
            of
                true ->
                    reattach_client(
                        ClientPid,
                        ClientInfo,
                        NewLeaf,
                        Leaves,
                        ClientsAcc,
                        LoadsAcc
                    );
                false ->
                    {ClientsAcc, LoadsAcc}
            end
        end,
        {Clients, Loads},
        Clients
    ),
    UpdatedClients.

reattach_client(ClientPid, ClientInfo, NewLeaf, Leaves, Clients, Loads) ->
    case add_client_with_retries(NewLeaf, ClientPid, 3) of
        ok ->
            update_client_leaf(ClientPid, ClientInfo, NewLeaf, Clients, Loads);
        {error, Reason} ->
            logger:warning(
                "Could not reattach client ~p to replacement leaf ~p after 3 attempts: ~p",
                [ClientPid, NewLeaf, Reason]
            ),
            fallback_client(
                ClientPid,
                ClientInfo,
                NewLeaf,
                Leaves,
                Clients,
                Loads
            )
    end.

add_client_with_retries(_Leaf, _ClientPid, 0) ->
    {error, retries_exhausted};
add_client_with_retries(Leaf, ClientPid, Attempts) ->
    case add_client(Leaf, ClientPid) of
        ok ->
            ok;
        {error, _Reason} = Error when Attempts =:= 1 ->
            Error;
        {error, _Reason} ->
            add_client_with_retries(Leaf, ClientPid, Attempts - 1)
    end.

fallback_client(ClientPid, ClientInfo, UnavailableLeaf, Leaves, Clients, Loads) ->
    Candidates = least_loaded_leaves(UnavailableLeaf, Leaves, Loads),
    case attach_to_first_available(Candidates, ClientPid, []) of
        {ok, Leaf} ->
            logger:info(
                "Reattached client ~p to fallback leaf ~p",
                [ClientPid, Leaf]
            ),
            update_client_leaf(ClientPid, ClientInfo, Leaf, Clients, Loads);
        {error, Reason} ->
            logger:error(
                "Could not reattach client ~p to any healthy leaf: ~p",
                [ClientPid, Reason]
            ),
            {Clients, Loads}
    end.

add_client(Leaf, ClientPid) ->
    try relay_server:add_client(Leaf, ClientPid) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason};
        Other ->
            {error, {unexpected_reply, Other}}
    catch
        exit:Reason ->
            {error, Reason}
    end.

attach_to_first_available([], _ClientPid, []) ->
    {error, no_healthy_leaf};
attach_to_first_available([], _ClientPid, Errors) ->
    {error, {all_candidate_leaves_unavailable, lists:reverse(Errors)}};
attach_to_first_available([Leaf | Remaining], ClientPid, Errors) ->
    case add_client(Leaf, ClientPid) of
        ok ->
            {ok, Leaf};
        {error, Reason} ->
            attach_to_first_available(
                Remaining,
                ClientPid,
                [{Leaf, Reason} | Errors]
            )
    end.

least_loaded_leaves(UnavailableLeaf, Leaves, Loads) ->
    HealthyLeaves = [
        Leaf
        || Leaf <- Leaves,
           Leaf =/= UnavailableLeaf,
           is_process_alive(Leaf)
    ],
    lists:sort(
        fun(Left, Right) ->
            {maps:get(Left, Loads, 0), Left}
                < {maps:get(Right, Loads, 0), Right}
        end,
        HealthyLeaves
    ).

client_loads(OldLeaf, Leaves, Clients) ->
    InitialLoads = maps:from_list([{Leaf, 0} || Leaf <- Leaves]),
    maps:fold(
        fun(ClientPid, ClientInfo, Loads) ->
            Leaf = maps:get(leaf, ClientInfo),
            case Leaf =/= OldLeaf
                andalso is_process_alive(ClientPid)
                andalso is_process_alive(Leaf)
            of
                true ->
                    maps:update_with(Leaf, fun(Count) -> Count + 1 end, 1, Loads);
                false ->
                    Loads
            end
        end,
        InitialLoads,
        Clients
    ).

update_client_leaf(ClientPid, ClientInfo, Leaf, Clients, Loads) ->
    UpdatedClients = Clients#{ClientPid => ClientInfo#{leaf => Leaf}},
    UpdatedLoads = maps:update_with(
        Leaf,
        fun(Count) -> Count + 1 end,
        1,
        Loads
    ),
    {UpdatedClients, UpdatedLoads}.
