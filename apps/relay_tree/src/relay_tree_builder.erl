-module(relay_tree_builder).

%% Creates the relay processes and the metadata used by the tree manager.

-export([build/1]).

-spec build(non_neg_integer()) -> {pid(), [pid()], map(), map()}.
build(MaxLevel) ->
    {Root, Leaves, RelayInfos} = build_tree(0, MaxLevel, undefined, 1),

    RelayMap = maps:from_list([
        {maps:get(pid, Info), Info}
        || Info <- RelayInfos
    ]),

    MonitorMap = maps:from_list([
        {maps:get(monitor, Info), maps:get(pid, Info)}
        || Info <- RelayInfos
    ]),

    {Root, Leaves, RelayMap, MonitorMap}.

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
            Info = relay_info(Pid, Id, Level, ParentPid, [], Ref),
            {Pid, [Pid], [Info]};
        false ->
            %% Use heap-style indices so each relay has a stable logical ID.
            {_LeftPid, LeftLeaves, LeftInfos} =
                build_tree(Level + 1, MaxLevel, Pid, Index * 2 - 1),
            {_RightPid, RightLeaves, RightInfos} =
                build_tree(Level + 1, MaxLevel, Pid, Index * 2),

            ChildPids = [
                maps:get(pid, Info)
                || Info <- LeftInfos ++ RightInfos,
                   maps:get(parent, Info) =:= Pid
            ],

            Info = relay_info(Pid, Id, Level, ParentPid, ChildPids, Ref),
            {Pid, LeftLeaves ++ RightLeaves, [Info | LeftInfos ++ RightInfos]}
    end.

relay_info(Pid, Id, Level, ParentPid, ChildPids, MonitorRef) ->
    #{
        pid => Pid,
        id => Id,
        level => Level,
        parent => ParentPid,
        children => ChildPids,
        monitor => MonitorRef
    }.
