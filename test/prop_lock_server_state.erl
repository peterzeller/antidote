-module(prop_lock_server_state).
-include("antidote.hrl").
-include_lib("proper/include/proper.hrl").


-record(pid_state, {
    spec :: antidote_locks:lock_spec(),
    requested_time :: integer(),
    status :: waiting | held
}).

-record(replica_state, {
    lock_server_state :: antidote_lock_server_state:state(),
    pid_states = #{} :: #{pid() => #pid_state{}},
    crdt_states = #{} :: #{antidote_locks:lock() => antidote_lock_crdt:value()},
    time = 0 :: integer(),
    snapshot_time = #{} :: snapshot_time()
}).

-record(state, {
    replica_states :: #{dcid() => #replica_state{}},
    time = 0 :: integer(),
    future_actions = [] :: [{Time :: integer(), Dc :: dcid(), Action :: antidote_lock_server_state:action()}],
    max_pid = 0 :: integer(),
    crdt_effects = [] :: [{snapshot_time(), [{bound_object(), antidote_crdt:effect()}]}]
}).

prop_test() ->
    ?FORALL(Cmds, my_commands(),
        begin
%%            io:format("~nRUN ~p~n", [length(Cmds)]),
            case satisfies_preconditions(Cmds) of
                false -> throw('input does not satisfy preconditions');
                true ->
                    {State, Tests} = my_run_commands(Cmds, initial_state(), []),
                    Tests2 = Tests ++ [{liveness, liveness(Cmds)}],
                    ?WHENFAIL(begin
                        io:format("Commands~n"),
                        lists:foreach(fun(C) -> io:format("  ~p~n", [C]) end, Cmds),
                        io:format("State: ~p~nResult: ~p\n", [
                            print_state(State),
                            Tests2
                        ])
                    end,

                        aggregate(my_command_names(Cmds), conjunction(Tests2)))
            end
        end).

print_state(State) ->
    #{
        time => State#state.time,
        future_actions => State#state.future_actions,
        replica_states => maps:map(
            fun(_K, V) ->
                V#replica_state{
                    lock_server_state = antidote_lock_server_state:print_state(V#replica_state.lock_server_state)
                }
            end,
            State#state.replica_states)
    }.

my_commands() ->
    ?LET(InitialState, ?LAZY(initial_state()),
        ?LET(List,
            ?SIZED(Size,
                begin
                    proper_types:noshrink(
                        my_commands(20 * Size, InitialState, 1))
                end
            ),
            ?SUCHTHAT(
                S,
                proper_types:shrink_list(List),
                satisfies_preconditions(S)))).

satisfies_preconditions(Cmds) ->
    Res = satisfies_preconditions(Cmds, initial_state()),
    io:format("satisfies_preconditions -> ~p~n", [Res]),
    Res.

satisfies_preconditions([], _) -> true;
satisfies_preconditions([Cmd | Cmds], State) ->
    precondition(State, Cmd) andalso satisfies_preconditions(Cmds, next_state(State, ignore, Cmd)).

%%my_commands() ->
%%    ?LET(InitialState, ?LAZY(initial_state()),
%%        ?SIZED(Size,
%%            begin
%%                my_commands(10*Size, InitialState, 1)
%%            end
%%        )).

my_commands(Size, State, Count) when is_integer(Size) ->
    ?LAZY(
        frequency([
            {1, []},
            {Size, ?LET(Cmd,
                ?SUCHTHAT(X, command(State),
                    precondition(State, X)),
                begin
%%                    io:format(user, "  Running Command ~p~n", [Cmd]),
                    NextState = next_state(State, ignore, Cmd),
                    ?LET(
                        Cmds,
                        my_commands(Size - 1, NextState, Count + 1),
                        [Cmd | Cmds])
                end)}
        ])).





my_command_names(Cmds) ->
    [my_command_name(C) || C <- Cmds].

my_command_name({action, {_Time, T}}) when is_tuple(T) ->
    {action, element(1, T)};
my_command_name(T) when is_tuple(T) ->
    element(1, T);
my_command_name(_) ->
    other.

my_run_commands([], State, Tests) ->
    {State, lists:reverse(Tests)};
my_run_commands([Cmd | RestCmds], State, Tests) ->
    NextState = next_state(State, ignore, Cmd),
    InvTest = invariant(NextState),
    my_run_commands(RestCmds, NextState, [{length(Tests), InvTest} | Tests]).





replicas() -> [r1, r2, r3].

replica() -> oneof(replicas()).

%% Initial model value at system start. Should be deterministic.
initial_state() ->
    #state{
        replica_states = maps:from_list([{R, initial_state(R)} || R <- replicas()])
    }.

initial_state(R) ->
    #replica_state{
        lock_server_state = antidote_lock_server_state:initial(R, replicas(), 10, 100, 5)
    }.

command(State) ->
%%    io:format("Future actions at ~p: ~p~n", [State#state.time, State#state.future_actions]),
    NeedsTick = [R || R <- replicas(), RS <- [maps:get(R, State#state.replica_states)], RS#replica_state.time + 50 < State#state.time],
    NeedsActions = needs_action(State),
    if
        NeedsActions /= [] ->
            oneof(NeedsActions);
        NeedsTick /= [] ->
            oneof([{tick, R, 0} || R <- NeedsTick]);
        true ->
            frequency(
                if
                    State#state.time < 100 ->
                        [{20, {request, State#state.max_pid + 1, replica(), lock_spec()}}];
                    true ->
                        []
                end
                ++ [{20, {tick, replica(), range(1, 100)}}]
                    ++ [{30, {action, A}} || A <- State#state.future_actions])
    end.

needs_action(State) ->
    [{action, {T, A}} || {T, A} <- State#state.future_actions, T + 50 < State#state.time].




lock_spec() ->
    N = 3,
    ?SUCHTHAT(
        L,
        ?LET(T,
            tuple([lock_level() || _ <- lists:seq(1, N)]),
            [{L, K} || {L, K} <- [{list_to_atom("lock" ++ integer_to_list(I)), element(I, T)} || I <- lists:seq(1, N)], K /= none]),
        L /= []).

%%lock() ->
%%    oneof([lock1, lock2, lock3]).

lock_level() ->
    oneof([none, exclusive]).


%% Picks whether a command should be valid under the current state.
precondition(State, {tick, _R, _T}) ->
    Res = needs_action(State) == [],
    io:format("precondition tick -> ~p~n", [Res]),
    Res;
%%precondition(State, {action, A}) ->
%%    Res = lists:member(A, State#state.future_actions),
%%    io:format("precondition action -> ~p~n Action = ~p~n FutAct = ~p~n", [Res, A, State#state.future_actions]),
%%    Res;
precondition(#state{}, _Action) ->
    true.

%% Given the state `State' *prior* to the call `{call, Mod, Fun, Args}',
%% determine whether the result `Res' (coming from the actual system)
%% makes sense.
postcondition(_State, _Action, _Res) ->
    true.

invariant(State) ->
    HeldLocks = lists:flatmap(
        fun(R) ->
            [{{R, P}, {L, K}} ||
                {P, S} <- maps:to_list((maps:get(R, State#state.replica_states))#replica_state.pid_states),
                S#pid_state.status == held,
                {L, K} <- S#pid_state.spec]
        end,
        replicas()),
    conjunction([
        % if one process has an exclusive lock, no other has any lock:
        {safety,
            conjunction(
                [{{exclusive_held, Id, L},
                    lists:all(
                        fun({Id2, {L2, _K2}}) ->
                            Id2 == Id orelse L /= L2
                        end,
                        HeldLocks)}
                    || {Id, {L, exclusive}} <- HeldLocks]
            )
        }
    ]).


%% Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(State, _Res, {request, Pid, Replica, LockSpec}) ->
    Rs = maps:get(Replica, State#state.replica_states),
    Requester = {Pid, tag},
    {Actions, NewRs} = antidote_lock_server_state:new_request(Requester, State#state.time, Rs#replica_state.snapshot_time, LockSpec, Rs#replica_state.lock_server_state),
    State2 = State#state{
        replica_states = maps:put(Replica, Rs#replica_state{
            lock_server_state = NewRs,
            pid_states        = maps:put(Pid, #pid_state{
                status         = waiting,
                requested_time = State#state.time,
                spec           = LockSpec
            }, Rs#replica_state.pid_states)
        }, State#state.replica_states),
        max_pid        = max(Pid, State#state.max_pid)
    },
    State3 = add_actions(State2, Replica, Actions),
    State3;
next_state(State, _Res, {tick, Replica, DeltaTime}) ->
    Rs = maps:get(Replica, State#state.replica_states),
    NewTime = State#state.time + DeltaTime,
    {Actions, NewRs} = antidote_lock_server_state:timer_tick(Rs#replica_state.lock_server_state, NewTime),
    State2 = State#state{
        time           = NewTime,
        replica_states = maps:put(Replica, Rs#replica_state{
            lock_server_state = NewRs,
            time              = NewTime
        }, State#state.replica_states)
    },
    add_actions(State2, Replica, Actions);
next_state(State, _Res, {action, {T1, Dc1, Action1}}) ->
    case pick_most_similar({T1, Dc1, Action1}, State#state.future_actions) of
        error ->
            State;
        {ok, {T, Dc, Action}} ->
            State2 = State#state{
                future_actions = State#state.future_actions -- [{T, Dc, Action}]
            },
            io:format("run_action(~p)~n", [Action]),
            run_action(State2, Dc, Action)
    end.



run_action(State, Dc, {read_crdt_state, SnapshotTime, Objects, Data}) ->
    io:format("read_crdt_state ~p~n", [Objects]),
    % TODO it is not necessary to read exactly from snapshottime
    ReadSnapshot = SnapshotTime,
    % collect all updates <= SnapshotTime
    CrdtStates = calculate_crdt_states(ReadSnapshot, Objects, State),
    io:format("CrdtStates = ~p~n", [CrdtStates]),
    ReadResults = [antidote_crdt:value(Type, CrdtState) || {{_, Type, _}, CrdtState} <- CrdtStates],
    ReplicaState = maps:get(Dc, State#state.replica_states),
    LockServerState = ReplicaState#replica_state.lock_server_state,
    {Actions, LockServerState2} = antidote_lock_server_state:on_read_crdt_state(State#state.time, Data, ReadSnapshot, ReadResults, LockServerState),
    ReplicaState2 = ReplicaState#replica_state{
        lock_server_state = LockServerState2
    },
    State2 = State#state{
        replica_states = maps:put(Dc, ReplicaState2, State#state.replica_states)
    },
    io:format("read_crdt_state Actions = ~p~n", [Actions]),
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
        replica_states = maps:put(Dc, ReplicaState2, State#state.replica_states)
    },
    add_actions(State2, Dc, Actions);
run_action(State, Dc, {update_crdt_state, SnapshotTime, Updates, Data}) ->
    % [{bound_object(), op_name(), op_param()}]
    ReplicaState = maps:get(Dc, State#state.replica_states),
    UpdateSnapshot = vectorclock:max([ReplicaState#replica_state.snapshot_time, SnapshotTime]),
    CrdtStates = calculate_crdt_states(UpdateSnapshot, [O || {O, _, _} <- Updates], State),
    Effects = lists:map(
        fun({{Key, CrdtState}, {Key, Op, Args}}) ->
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
        snapshot_time     = NewUpdateSnapshot,
        lock_server_state = NewLockServerState
    },
    State2 = State#state{
        crdt_effects   = [{NewUpdateSnapshot, Effects} | State#state.crdt_effects],
        replica_states = maps:put(Dc, NewReplicaState, State#state.replica_states)
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


make_lock_update(exclusive, From, To, Crdt) ->
    maps:from_list([{D, To} || D <- replicas(), maps:get(D, Crdt, D) == From]);
make_lock_update(shared, From, To, Crdt) ->
    maps:from_list([{D, To} || D <- [To], maps:get(D, Crdt, D) == From]).

apply_lock_updates(CrdtStates, []) ->
    CrdtStates;
apply_lock_updates(CrdtStates, [{L, Upd} | Rest]) ->
    CrdtStates2 = maps:update_with(L, fun(V) -> maps:merge(V, Upd) end, Upd, CrdtStates),
    apply_lock_updates(CrdtStates2, Rest).

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


liveness(Cmds) ->
    % every request should have a reply within 500ms
    check_liveness(Cmds).

check_liveness([]) ->
    true;
check_liveness([{request, Pid, _R, _Locks} | Rest]) ->
    find_reply(Pid, Rest, 1000)
        andalso check_liveness(Rest);
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
