-module(lock_server_state_SUITE).
-include("antidote.hrl").

% run with:
% rebar3 ct --suite lock_server_state_SUITE --dir test/singledc

-export([all/0, explore/1, suite/0, explore/0]).

all() -> [explore].

suite() ->
    [{timetrap, {minutes, 5}}].

-record(pid_state, {
    spec :: antidote_locks:lock_spec(),
    requested_time :: integer(),
    status :: waiting | held
}).

-record(replica_state, {
    lock_server_state :: antidote_lock_server_state:state(),
    pid_states = #{} :: #{pid() => #pid_state{}},
    time = 0 :: integer(),
    snapshot_time = #{} :: snapshot_time()
}).

-type future_action() :: {Time :: integer(), Dc :: dcid(), Action :: antidote_lock_server_state:action()}.

-record(state, {
    replica_states :: #{dcid() => #replica_state{}},
    time = 0 :: integer(),
    future_actions = [] :: [future_action()],
    max_pid = 0 :: integer(),
    crdt_effects = [] :: [{snapshot_time(), [{bound_object(), antidote_crdt:effect()}]}],
    cmds = [] :: [any()]
}).


explore() ->
    [{timetrap, {minutes, 15}}].


explore(_Config) ->
    dorer:check(#{max_shrink_time => {300, second}, n => 1000}, fun() ->
        State = my_run_commands(initial_state()),
        log_commands(State),
        dorer:log("Final State: ~n ~p", [print_state(State)]),
        check_liveness(lists:reverse(State#state.cmds))
    end).

log_commands(State) ->
    lists:foreach(fun(Cmd) ->
        dorer:log("RUN ~w", [Cmd])
    end, lists:reverse(State#state.cmds)).

print_state(State) ->
    #{
        time => State#state.time,
        future_actions => State#state.future_actions,
        replica_states => maps:map(
            fun(_K, V) ->
                #{
                    lock_server_state => antidote_lock_server_state:print_state(V#replica_state.lock_server_state),
                    pid_states => V#replica_state.pid_states,
                    time => V#replica_state.time,
                    snapshot_time => V#replica_state.snapshot_time
                }
            end,
            State#state.replica_states),
        crdt_states => maps:map(fun(_K, V) ->
            Snapshot = V#replica_state.snapshot_time,
            Objects = [O || {_S, Effs} <- State#state.crdt_effects, {O, Eff} <- Effs],
            CrdtStates = calculate_crdt_states(Snapshot, Objects, State),
            ReadResults = [{Key, antidote_crdt:value(Type, CrdtState)} || {{Key, Type, _}, CrdtState} <- CrdtStates],
            maps:from_list(ReadResults)
        end, State#state.replica_states)
    }.



my_command_names(Cmds) ->
    [my_command_name(C) || C <- Cmds].

my_command_name({action, {_Time, T}}) when is_tuple(T) ->
    {action, element(1, T)};
my_command_name(T) when is_tuple(T) ->
    element(1, T);
my_command_name(_) ->
    other.

my_run_commands(State) ->
    case dorer:gen(command, dorer_generators:has_more()) of
        false -> State;
        true ->
            Cmd = gen_command(State),
            dorer:log("Time ~p: RUN ~w", [State#state.time, Cmd]),
            NextState = next_state(State#state{cmds = [Cmd | State#state.cmds]}, Cmd),
            dorer:log("State:~n ~p", [print_state(NextState)]),
%%            dorer:log("Active requests: ~p", [maps:map(fun(Dc, RS) ->
%%                maps:keys(RS#replica_state.pid_states) end, NextState#state.replica_states)]),
            check_invariant(NextState),
            my_run_commands(NextState)
    end.





replicas() -> [r1, r2, r3].

replica() -> dorer_generators:no_shrink(dorer_generators:oneof(replicas())).

%% Initial model value at system start. Should be deterministic.
initial_state() ->
    #state{
        replica_states = maps:from_list([{R, initial_state(R)} || R <- replicas()])
    }.

initial_state(R) ->
    #replica_state{
        lock_server_state = antidote_lock_server_state:initial(R, replicas(), 10, 100, 5)
    }.

gen_command(State) ->
    NeedsTick = [R || R <- replicas(), RS <- [maps:get(R, State#state.replica_states)], RS#replica_state.time + 50 < State#state.time],
    NeedsActions = needs_action(State),
    if
        NeedsActions /= [] ->
            hd(NeedsActions);
%%            dorer:gen([command, needs_action],
%%                dorer_generators:oneof(NeedsActions));
        NeedsTick /= [] ->
            {tick, hd(NeedsTick), 0};
%%            dorer:gen([command, needs_tick],
%%                dorer_generators:no_shrink(dorer_generators:oneof([{tick, R, 0} || R <- NeedsTick])));
        true ->
            P1 = if
                State#state.time < 100 -> 20;
                true -> 0
            end,
            FutureActions = filter_future_actions(State#state.future_actions),
            dorer:gen([command, normal],
                dorer_generators:frequency_gen([
                    {P1, {request, State#state.max_pid + 1, replica(), lock_spec()}},
                    {20, {tick, replica(), dorer_generators:oneof([100, 50, 20, 10, 5, 1])}},
                    {10 * length(FutureActions), dorer_generators:oneof([{action, A} || A <- FutureActions])}
                ]))
    end.

needs_action(State) ->
    FutureActions = filter_future_actions(State#state.future_actions),

    lists:map(fun(A) -> {action, A} end,
        lists:filter(fun({T, _Dc, _A}) ->
            T + 50 < State#state.time
        end, FutureActions)).

% only execute reads, if there is no update action planned for the same DC
-spec filter_future_actions([future_action()]) -> [future_action()].
filter_future_actions(Actions) ->
    lists:filter(fun
        ({_, Dc, {read_crdt_state, SnapshotTime, Objects, Data}}) ->
            % check if there is no update scheduled on the same DC
            not lists:any(fun
                ({_, Dc2, {update_crdt_state, SnapshotTime2, Updates, Data2}}) ->
                    Dc == Dc2;
                (_) ->
                    false
            end, Actions);
        (_) ->
            true
    end, Actions).



lock_spec() ->
    N = 3,
    Locks = [list_to_atom("lock" ++ integer_to_list(I)) || I <- lists:seq(1, N)],
    dorer_generators:transform(
        dorer_generators:map(dorer_generators:oneof(Locks), lock_level()),
        fun(M) ->
            case maps:to_list(M) of
                [] -> [{lock1, shared}];
                L -> L
            end
        end).


lock_level() ->
    dorer_generators:oneof([shared, exclusive]).



check_invariant(State) ->
    HeldLocks = lists:flatmap(
        fun(R) ->
            [{{R, P}, {L, K}} ||
                {P, S} <- maps:to_list((maps:get(R, State#state.replica_states))#replica_state.pid_states),
                S#pid_state.status == held,
                {L, K} <- S#pid_state.spec]
        end,
        replicas()),
    lists:foreach(fun
        (Lock1 = {Id, {L, exclusive}}) ->
            lists:foreach(fun(Lock2 = {Id2, {L2, _K2}}) ->
                case Id2 /= Id andalso L == L2 of
                    false -> ok;
                    true ->
                        log_commands(State),
                        dorer:log("Invariant violation in state:~n ~p", [print_state(State)]),
                        throw({'Safety violation: Lock held by two processes: ', Lock1, Lock2})
                end

            end, HeldLocks);
        (_) ->
            ok
    end, HeldLocks).


%% Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(State, {request, Pid, Replica, LockSpec}) ->
    Rs = maps:get(Replica, State#state.replica_states),
    Requester = {Pid, tag},
    {Actions, NewRs} = antidote_lock_server_state:new_request(Requester, State#state.time, Rs#replica_state.snapshot_time, LockSpec, Rs#replica_state.lock_server_state),
    State2 = State#state{
        replica_states = maps:put(Replica, Rs#replica_state{
            lock_server_state = NewRs,
            pid_states = maps:put(Pid, #pid_state{
                status = waiting,
                requested_time = State#state.time,
                spec = LockSpec
            }, Rs#replica_state.pid_states)
        }, State#state.replica_states),
        max_pid = max(Pid, State#state.max_pid)
    },
    State3 = add_actions(State2, Replica, Actions),
    State3;
next_state(State, {tick, Replica, DeltaTime}) ->
    Rs = maps:get(Replica, State#state.replica_states),
    NewTime = State#state.time + DeltaTime,
    {Actions, NewRs} = antidote_lock_server_state:timer_tick(Rs#replica_state.lock_server_state, NewTime),
    State2 = State#state{
        time = NewTime,
        replica_states = maps:put(Replica, Rs#replica_state{
            lock_server_state = NewRs,
            time = NewTime
        }, State#state.replica_states)
    },
    add_actions(State2, Replica, Actions);
next_state(State, {action, {T1, Dc1, Action1}}) ->
    case pick_most_similar({T1, Dc1, Action1}, State#state.future_actions) of
        error ->
            State;
        {ok, {T, Dc, Action}} ->
            State2 = State#state{
                future_actions = State#state.future_actions -- [{T, Dc, Action}]
            },
            run_action(State2, Dc, Action)
    end;
next_state(_, Other) ->
    throw({unhandled_next_state_action, Other}).



run_action(State, Dc, {read_crdt_state, SnapshotTime, Objects, Data}) ->
    ReplicaState = maps:get(Dc, State#state.replica_states),

    % TODO it is not necessary to read exactly from snapshottime
    ReadSnapshot = vectorclock:max([SnapshotTime, ReplicaState#replica_state.snapshot_time]),

    % collect all updates <= SnapshotTime
    CrdtStates = calculate_crdt_states(ReadSnapshot, Objects, State),
    ReadResults = [antidote_crdt:value(Type, CrdtState) || {{_, Type, _}, CrdtState} <- CrdtStates],

    LockServerState = ReplicaState#replica_state.lock_server_state,
    dorer:log("ReadSnapshot = ~p", [ReadSnapshot]),
    dorer:log("ReadResults = ~p", [ReadResults]),
    {Actions, LockServerState2} = antidote_lock_server_state:on_read_crdt_state(State#state.time, Data, ReadSnapshot, ReadResults, LockServerState),
    ReplicaState2 = ReplicaState#replica_state{
        lock_server_state = LockServerState2,
        snapshot_time = ReadSnapshot
    },
    State2 = State#state{
        replica_states = maps:put(Dc, ReplicaState2, State#state.replica_states)
    },
    add_actions(State2, Dc, Actions);
run_action(State, Dc, {send_inter_dc_message, Receiver, Message}) ->
    % deliver interdc message
    ReplicaState = maps:get(Receiver, State#state.replica_states),
    LockServerState = ReplicaState#replica_state.lock_server_state,
    {Actions, LockServerState2} = antidote_lock_server_state:on_receive_inter_dc_message(State#state.time, Dc, Message, LockServerState),
    ReplicaState2 = ReplicaState#replica_state{
        lock_server_state = LockServerState2
    },
    State2 = State#state{
        replica_states = maps:put(Receiver, ReplicaState2, State#state.replica_states)
    },
    add_actions(State2, Receiver, Actions);
run_action(State, Dc, {update_crdt_state, SnapshotTime, Updates, Data}) ->
    % [{bound_object(), op_name(), op_param()}]
    UpdatedObjects = [O || {O, _, _} <- Updates],
    case lists:usort(UpdatedObjects) == lists:sort(UpdatedObjects) of
        true -> ok;
        false -> throw({'List of updates contains duplicates', UpdatedObjects, Updates})
    end,

    ReplicaState = maps:get(Dc, State#state.replica_states),
    UpdateSnapshot = vectorclock:max([ReplicaState#replica_state.snapshot_time, SnapshotTime]),

    CrdtStates = calculate_crdt_states(UpdateSnapshot, UpdatedObjects, State),
    Effects = lists:map(
        fun({{_Key, CrdtState}, {Key, Op, Args}}) ->
            {_, Type, _} = Key,
            {ok, Effect} = antidote_crdt:downstream(Type, {Op, Args}, CrdtState),
            {Key, Effect}
        end,
        lists:zip(CrdtStates, Updates)
    ),
    NewUpdateSnapshot = vectorclock:set(Dc, vectorclock:get(Dc, UpdateSnapshot) + 1, UpdateSnapshot),
    LockServerState = ReplicaState#replica_state.lock_server_state,
    {Actions, NewLockServerState} = antidote_lock_server_state:on_complete_crdt_update(State#state.time, Data, NewUpdateSnapshot, LockServerState),

    NewReplicaState = ReplicaState#replica_state{
        snapshot_time = NewUpdateSnapshot,
        lock_server_state = NewLockServerState
    },
    State2 = State#state{
        crdt_effects = [{NewUpdateSnapshot, Effects} | State#state.crdt_effects],
        replica_states = maps:put(Dc, NewReplicaState, State#state.replica_states)
    },

    add_actions(State2, Dc, Actions);
run_action(State, Dc, {accept_request, {Pid, _Tag}}) ->
    ReplicaState = maps:get(Dc, State#state.replica_states),
    PidState = maps:get(Pid, ReplicaState#replica_state.pid_states),
    NewPidState = PidState#pid_state{
        status = held
    },
    NewReplicaState = ReplicaState#replica_state{
        pid_states = maps:put(Pid, NewPidState, ReplicaState#replica_state.pid_states)
    },

    State2 = State#state{
        replica_states = maps:put(Dc, NewReplicaState, State#state.replica_states)
    },
    Actions = [
        {release_locks, Pid}
    ],
    add_actions(State2, Dc, Actions);
run_action(State, Dc, {release_locks, Pid}) ->
    ReplicaState = maps:get(Dc, State#state.replica_states),

    NewSnapshotTime = vectorclock:set(Dc, vectorclock:get(Dc, ReplicaState#replica_state.snapshot_time), ReplicaState#replica_state.snapshot_time),
    {Actions, NewLockServerState} = antidote_lock_server_state:remove_locks(State#state.time, Pid, NewSnapshotTime, ReplicaState#replica_state.lock_server_state),


    NewReplicaState2 = ReplicaState#replica_state{
        lock_server_state = NewLockServerState,
        pid_states = maps:remove(Pid, ReplicaState#replica_state.pid_states),
        snapshot_time = NewSnapshotTime
    },

    State2 = State#state{
        replica_states = maps:put(Dc, NewReplicaState2, State#state.replica_states)
    },
    add_actions(State2, Dc, Actions).

calculate_crdt_states(ReadSnapshot, Objects, State) ->
    Effects = lists:filter(fun({Clock, _}) -> vectorclock:le(Clock, ReadSnapshot) end, State#state.crdt_effects),
    OrderedEffects = antidote_list_utils:topsort(fun({C1, _}, {C2, _}) -> vectorclock:lt(C1, C2) end, Effects),
    CrdtStates = lists:map(fun(Key = {_, Type, _}) ->
        Initial = antidote_crdt:new(Type),
        CrdtState = lists:foldl(fun(Eff, Acc) ->
            {ok, NewAcc} = antidote_crdt:update(Type, Eff, Acc),
            NewAcc
        end, Initial, [Eff || {_, Effs} <- OrderedEffects, {K, Eff} <- Effs, K == Key]),
        {Key, CrdtState}
    end, Objects),
    CrdtStates.


%%make_lock_update(exclusive, From, To, Crdt) ->
%%    maps:from_list([{D, To} || D <- replicas(), maps:get(D, Crdt, D) == From]);
%%make_lock_update(shared, From, To, Crdt) ->
%%    maps:from_list([{D, To} || D <- [To], maps:get(D, Crdt, D) == From]).
%%
%%apply_lock_updates(CrdtStates, []) ->
%%    CrdtStates;
%%apply_lock_updates(CrdtStates, [{L, Upd} | Rest]) ->
%%    CrdtStates2 = maps:update_with(L, fun(V) -> maps:merge(V, Upd) end, Upd, CrdtStates),
%%    apply_lock_updates(CrdtStates2, Rest).

add_actions(State, Dc, Actions) ->
    T = State#state.time,
    NewActions = [{T, Dc, A} || A <- Actions],
    State#state{
        future_actions = State#state.future_actions ++ NewActions
    }.

%%-record(actions, {
%%    % locks to send to other DCs
%%    hand_over = #{} :: #{dcid() => antidote_locks:lock_spec()},
%%    % new lock requests to send to other DCs
%%    lock_request = #{} :: lock_request_actions()
%% #{dcid() => lock_request_actions_for_dc()}.,
%% #{{dcid(), antidote_locks:lock()} => {MaxKind :: antidote_locks:lock_kind(), MinRequestTime :: milliseconds()}}
%%    % local requesting processes to reply to
%%    replies = [] :: [requester()],
%%    % acknowledge that all parts of a lock have been received
%%    ack_locks = [] :: [antidote_locks:lock_spec_item()]
%%}).



check_liveness([]) ->
    true;
check_liveness([Req = {request, Pid, _R, _Locks} | Rest]) ->
    case find_reply(Pid, Rest, 2000) of
        true -> ok;
        false ->
            throw({'liveness violation, no response for request ', Req})
    end,
    check_liveness(Rest);
check_liveness([_ | Rest]) ->
    check_liveness(Rest).

find_reply(_, _, Time) when Time < 0 ->
    false;
find_reply(_, [], _) ->
    true;
find_reply(Pid, [{action, {_T, _Dc, Action}} | Rest], Time) ->
    case Action of
        {accept_request, {Pid, _}} ->
            true;
        _ ->
            find_reply(Pid, Rest, Time)
    end;
find_reply(Pid, [{tick, _R, T} | Rest], Time) ->
    find_reply(Pid, Rest, Time - T);
find_reply(Pid, [Other | Rest], Time) ->
    % sanity check
    case Other of
        {request, _, _, _} -> ok;
        Other -> throw({unhandled_case2, Other})
    end,
    find_reply(Pid, Rest, Time).


pick_most_similar(_Elem, []) -> error;
pick_most_similar(Elem, List) ->
    WithSimilarity = [{similarity(Elem, X), X} || X <- List],
    {_, Res} = lists:max(WithSimilarity),
    {ok, Res}.


similarity(X, X) -> 1;
similarity(X, Y) when is_atom(X) andalso is_atom(Y) ->
    0.1;
similarity(X, Y) when is_list(X) andalso is_list(Y) ->
    case {X, Y} of
        {[], _} -> 0.1;
        {_, []} -> 0.1;
        {[A | As], [A | Bs]} ->
            L = max(length(As), length(Bs)),
            1 / (1 + L) + similarity(As, Bs) * L / (1 + L);
        {[A | As], Xs} ->
            L = max(1 + length(As), length(Xs)),
            {ok, Sim} = pick_most_similar(A, Xs),
            similarity(A, Sim) / L + similarity(As, Xs -- [Sim]) * (L - 1) / L
    end;
similarity(X, Y) when is_tuple(X) andalso is_tuple(Y) ->
    0.5 + similarity(tuple_to_list(X), tuple_to_list(Y)) * 0.5;
similarity(X, Y) when is_map(X) andalso is_map(Y) ->
    0.5 + similarity(maps:to_list(X), maps:to_list(Y)) * 0.5;
similarity(_, _) -> 0.
