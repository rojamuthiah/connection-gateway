
-module(relay_tree).

-behaviour(gen_server).

-export([
    start_link/0,
    create_tree/1,
    subscribe/1,
    get_leaves/0,
    get_root/0,
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

get_leaves() ->
    gen_server:call(?MODULE, get_leaves).

get_root() ->
    gen_server:call(?MODULE, get_root).

broadcast(Message) ->
    gen_server:call(?MODULE, {broadcast, Message}).

%% Initial state

init([]) ->
    State = #{
        root => undefined,
        leaves => [],
        relays => [],
        next_leaf => 0
    },

    {ok, State}.

%% Create tree

handle_call({create_tree, Layers}, _From, State)
    when is_integer(Layers), Layers >= 0, Layers =< 10 ->

    %% Stop old tree
    lists:foreach(
        fun(Pid) ->
            catch gen_server:stop(Pid)
        end,
        maps:get(relays, State)
    ),

    %% Build new tree
    {Root, Leaves, Relays} =
        build_tree(0, Layers, undefined, 1),

    NewState = #{
        root => Root,
        leaves => Leaves,
        relays => Relays,
        next_leaf => 0
    },

    {reply, {ok, Leaves}, NewState};

handle_call({create_tree, _Layers}, _From, State) ->
    {reply, {error, invalid_layers}, State};

%% Subscribe client

handle_call({subscribe, ClientPid}, _From, State)
    when is_pid(ClientPid) ->

    Leaves = maps:get(leaves, State),

    case Leaves of
        [] ->
            {reply, {error, no_tree}, State};

        _ ->
            Index = maps:get(next_leaf, State),

            LeafPid = lists:nth(
                (Index rem length(Leaves)) + 1,
                Leaves
            ),

            case catch relay_server:add_client(
                LeafPid,
                ClientPid
            ) of
                ok ->
                    NewState = State#{
                        next_leaf => Index + 1
                    },

                    {reply, {ok, LeafPid}, NewState};

                _ ->
                    {reply, {error, leaf_unavailable}, State}
            end
    end;

handle_call({subscribe, _ClientPid}, _From, State) ->
    {reply, {error, invalid_client}, State};

%% Get leaves

handle_call(get_leaves, _From, State) ->
    {reply, maps:get(leaves, State), State};

%% Get root

handle_call(get_root, _From, State) ->
    {reply, maps:get(root, State), State};

%% Broadcast

handle_call({broadcast, Message}, _From, State) ->
    case maps:get(root, State) of
        undefined ->
            {reply, {error, no_tree}, State};

        Root ->
            relay_server:broadcast(Root, Message),
            {reply, ok, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% Recursive tree creation

build_tree(Level, MaxLevel, ParentPid, Index) ->

    RelayId = {relay, Level, Index},

    {ok, Pid} = relay_server:start(RelayId),

    %% Automatically establish parent-child relationship
    case ParentPid of
        undefined ->
            ok;

        _ ->
            ok = relay_server:set_parent(Pid, ParentPid),
            ok = relay_server:add_child(ParentPid, Pid)
    end,

    case Level =:= MaxLevel of
        true ->
            %% This is a leaf
            {Pid, [Pid], [Pid]};

        false ->
            %% Create left child subtree
            {_Left, LeftLeaves, LeftRelays} =
                build_tree(
                    Level + 1,
                    MaxLevel,
                    Pid,
                    Index * 2 - 1
                ),

            %% Create right child subtree
            {_Right, RightLeaves, RightRelays} =
                build_tree(
                    Level + 1,
                    MaxLevel,
                    Pid,
                    Index * 2
                ),

            AllLeaves = LeftLeaves ++ RightLeaves,

            AllRelays = [
                Pid | LeftRelays ++ RightRelays
            ],

            {Pid, AllLeaves, AllRelays}
    end.
