-module(prop_lock_server_state).
-include("antidote.hrl").
-include_lib("proper/include/proper.hrl").

-export([command/1, initial_state/0, next_state/3,
    precondition/2, postcondition/3]).

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
    future_actions = [] :: [{Time :: integer(), Action :: any()}],
    max_pid = 0 :: integer()
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
                        my_commands(10*Size, InitialState, 1))
                end
            ),
            ?SUCHTHAT(
                S,
                proper_types:shrink_list(List),
                satisfies_preconditions(S)))).

satisfies_preconditions(Cmds) -> satisfies_preconditions(Cmds, initial_state()).

satisfies_preconditions([], _) -> true;
satisfies_preconditions([Cmd|Cmds], State) ->
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
        lock_server_state = antidote_lock_server_state:initial(R, 10, 100, 5)
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
    ?LET(X, non_empty(list(lock_spec_item())), orddict:from_list(X)).

lock_spec_item() ->
    {lock(), lock_level()}.

lock() ->
    oneof([lock1, lock2, lock3]).

lock_level() ->
    oneof([exclusive]).


%% Picks whether a command should be valid under the current state.
precondition(State, {tick, _R, _T}) ->
    Res = needs_action(State) == [],
%%    io:format("precondition tick ~p: ~p~n State = ~p~n", [T, Res, print_state(State)]),
    Res;
precondition(State, {action, A}) ->
    lists:member(A, State#state.future_actions);
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
    LockEntries = [{{L, K}, maps:get(L, Rs#replica_state.crdt_states, #{})} || {L, K} <- LockSpec],
    {Actions, NewRs} = antidote_lock_server_state:new_request(Requester, State#state.time, Rs#replica_state.snapshot_time, replicas(), LockEntries, Rs#replica_state.lock_server_state),
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
next_state(State, _Res, {action, {T, Action}}) ->
    State2 = State#state{
        future_actions = State#state.future_actions -- [{T, Action}]
    },
    run_action(State2, Action).

run_action(State, {reply, Dc, {Pid, _Tag}}) ->
    Rs = maps:get(Dc, State#state.replica_states),
    State#state{
        replica_states = maps:put(Dc, Rs#replica_state{
            pid_states = maps:update_with(
                Pid,
                fun(PidState) ->
%%                    io:format("Acquired ~p~n", [PidState#pid_state.spec]),
                    PidState#pid_state{
                        status = held
                    }
                end,
                Rs#replica_state.pid_states
            )
        }, State#state.replica_states),
        future_actions = [{State#state.time, {release, Dc, Pid}} | State#state.future_actions]
    };
run_action(State, {release, Dc, Pid}) ->
    Rs = maps:get(Dc, State#state.replica_states),

    {Actions, NewRs} = antidote_lock_server_state:remove_locks(State#state.time, Pid, Rs#replica_state.snapshot_time, Rs#replica_state.lock_server_state),

    State2 = State#state{
        replica_states = maps:put(Dc, Rs#replica_state{
            pid_states        = maps:remove(
                Pid,
                Rs#replica_state.pid_states
            ),
            lock_server_state = NewRs
        }, State#state.replica_states)
    },
    add_actions(State2, Dc, Actions);
run_action(State, {lock_request, _From, To, LockRequestActionsForDc}) ->
    Rs = maps:get(To, State#state.replica_states),
    {Actions, NewRs} = antidote_lock_server_state:new_remote_request(State#state.time, LockRequestActionsForDc, Rs#replica_state.lock_server_state),
    State2 = State#state{
        replica_states = maps:put(To, Rs#replica_state{
            lock_server_state = NewRs
        }, State#state.replica_states)
    },
    State3 = add_actions(State2, To, Actions),
    State3;
run_action(State, {hand_over, From, To, Locks}) ->
    Rs = maps:get(From, State#state.replica_states),

    LockUpdates = [{Lock, make_lock_update(Level, From, To, maps:get(Lock, Rs#replica_state.crdt_states, #{}))} || {Lock, Level} <- Locks],

    NewRs = Rs#replica_state{
        crdt_states = apply_lock_updates(Rs#replica_state.crdt_states, LockUpdates)
    },
    State#state{
        replica_states = maps:put(From, NewRs, State#state.replica_states),
        future_actions = [{State#state.time, {hand_over2, NewRs#replica_state.snapshot_time, To, Locks, LockUpdates}} | State#state.future_actions]
    };
run_action(State, {hand_over2, SnapshotTime, To, LockSpec, LockUpdates}) ->
    Rs = maps:get(To, State#state.replica_states),


    Rs2 = Rs#replica_state{
        crdt_states   = apply_lock_updates(Rs#replica_state.crdt_states, LockUpdates),
        snapshot_time = vectorclock:max([Rs#replica_state.snapshot_time, SnapshotTime])
    },

%%
%%    io:format("Old crdt_states at ~p = ~p~n", [To, Rs#replica_state.crdt_states]),
%%    io:format("Updates         at ~p = ~p~n", [To, LockUpdates]),
%%    io:format("New crdt_states at ~p = ~p~n", [To, Rs2#replica_state.crdt_states]),

    LockEntries = [{{L, K}, maps:get(L, Rs2#replica_state.crdt_states, #{})} || {L, K} <- LockSpec],

    {Actions, NewRs} = antidote_lock_server_state:on_remote_locks_received(State#state.time, Rs2#replica_state.snapshot_time, replicas(), LockEntries, Rs2#replica_state.lock_server_state),
    State2 = State#state{
        replica_states = maps:put(To, Rs2#replica_state{
            lock_server_state = NewRs
        }, State#state.replica_states)
    },
    add_actions(State2, To, Actions);
run_action(State, {ack, From, To, Locks}) ->
    Rs = maps:get(To, State#state.replica_states),
    {Actions, NewRs} = antidote_lock_server_state:ack_remote_requests(State#state.time,
        [{From, L} || L <- Locks], Rs#replica_state.lock_server_state),

    State2 = State#state{
        replica_states = maps:put(To, Rs#replica_state{
            lock_server_state = NewRs
        }, State#state.replica_states)
    },
    add_actions(State2, To, Actions).


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
    {actions, HandOverActions, LockRequestActions, Replies, Acks} = Actions,
    T = State#state.time,
    NewActions =
        [{T, {hand_over, Dc, D, Locks}} || {D, Locks} <- maps:to_list(HandOverActions)]
        ++ [{T, {lock_request, Dc, D, Locks}} || {D, Locks} <- maps:to_list(LockRequestActions)]
        ++ [{T, {reply, Dc, R}} || R <- Replies]
        ++ [{T, {ack, Dc, To, Acks}} || To <- replicas(), Acks /= []],
    State#state{
        future_actions = NewActions ++ State#state.future_actions
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
find_reply(Pid, [{action, {_T, Action}} | Rest], Time) ->
    case Action of
        {reply, _, {Pid, _}} ->
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
