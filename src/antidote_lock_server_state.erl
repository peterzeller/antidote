%% -------------------------------------------------------------------
%%
%% Copyright <2013-2018> <
%%  Technische Universität Kaiserslautern, Germany
%%  Université Pierre et Marie Curie / Sorbonne-Université, France
%%  Universidade NOVA de Lisboa, Portugal
%%  Université catholique de Louvain (UCL), Belgique
%%  INESC TEC, Portugal
%% >
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either expressed or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% List of the contributors to the development of Antidote: see AUTHORS file.
%% Description and complete License: see LICENSE file.
%% -------------------------------------------------------------------

%% @doc This encapsulates the state of the antidote_lock_server
-module(antidote_lock_server_state).
%%
-include("antidote.hrl").
-include("antidote_message_types.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([state/0]).

-export([
    initial/1,
    my_dc_id/1,
    new_request/6, new_remote_request/5, on_remote_locks_received/4, remove_locks/3, check_release_locks/2, get_snapshot_time/1, set_timer_active/2, get_timer_active/1, retries_for_waiting_remote/4]).


% state invariants:
% - if a process has an exclusive lock, no other process has a lock (held state)
% - if a local process is in held or waiting state:
%         for exclusive locks: all locks are locally available
%         for shared locks: the processes own lock is locally available

% Liveness:
% - if a process holds a lock, it is not waiting for any locks that are smaller (=> avoiding deadlocks)
% - each request has a request-time. Requests with lower request time are considered first so that
%   each process should eventually get its lock


-record(pid_state, {
    locks :: orddict:orddict(antidote_locks:lock(), lock_state_with_kind()),
    request_time :: integer(),
    is_remote :: {yes, dcid()} | no,
    requester :: requester()
}).

-record(state, {
    %% own datacenter id
    dc_id :: dcid(),
    %% latest snapshot,
    snapshot_time = vectorclock:new() :: snapshot_time(),
    by_pid = #{} :: #{pid() => #pid_state{}},
    timer_Active = false :: boolean()
}).

-opaque state() :: #state{}.



-type requester() :: {pid(), Tag :: term()}.

-type lock_state() :: waiting | held | waiting_remote.

-type lock_state_with_kind() ::
    {lock_state(), antidote_locks:lock_kind()}.

-type lock_request_actions() :: #{dcid() => antidote_locks:lock_spec()}.

-type actions() :: {
    HandOverActions :: [{antidote_locks:lock_spec(), dcid(), requester()}],
    lock_request_actions(),
    Replies :: [requester()]}.

%% Public API:

-spec initial(dcid()) -> state().
initial(MyDcId) -> #state{
    dc_id = MyDcId,
    by_pid = #{}
}.

-spec my_dc_id(state()) -> dcid().
my_dc_id(State) ->
    State#state.dc_id.



-spec new_request(requester(), integer(), snapshot_time(), [dcid()], ordsets:ordset({antidote_locks:lock_spec_item(), antidote_lock_crdt:value()}), state()) -> {actions(), state()}.
new_request(Requester, RequestTime, SnapshotTime, AllDcIds, LockEntries, State) ->
    {RequesterPid, _} = Requester,
    MyDcId = my_dc_id(State),
    Locks = [L || {L, _} <- LockEntries],
    State1 = add_process(Requester, RequestTime, no, Locks, State),
    State2 = set_snapshot_time(SnapshotTime, State1),
    RequestsByDc = missing_locks_by_dc(AllDcIds, MyDcId, LockEntries),
    logger:info("missing_locks_by_dc = ~p", [RequestsByDc]),
    case maps:size(RequestsByDc) of
        0 ->
            % we have all locks locally
            next_actions(State2);
        _ ->
            % tell other data centers that we need locks
            % for shared locks, ask to get own lock back
            % for exclusive locks, ask everyone to give their lock

            WaitingLocks = ordsets:from_list([L || {L, _} <- lists:flatmap(fun(X) -> X end, maps:values(RequestsByDc))]),
            State3 = set_lock_waiting_state(RequesterPid, WaitingLocks, State2, waiting, waiting_remote),
            {Actions, State4} = next_actions(State3),

            {merge_actions({[], RequestsByDc, []}, Actions), State4}
    end.


-spec new_remote_request(requester(), integer(), antidote_locks:lock_spec(), dcid(), state()) -> {actions(), state()}.
new_remote_request(From, Timestamp, Locks, RequesterDcId, State) ->
    NewState = add_process(From, Timestamp, {yes, RequesterDcId}, Locks, State),
    next_actions(NewState).

-spec on_remote_locks_received(snapshot_time(), [dcid()], ordsets:ordset({antidote_locks:lock_spec_item(), antidote_lock_crdt:value()}), state()) -> {actions(), state()}.
on_remote_locks_received(ReadClock, AllDcs, LockEntries, State) ->
    logger:info("on_remote_locks_received~n  LockEntries = ~p~n  ReadClock = ~p", [LockEntries, ReadClock]),
    LockStates = maps:from_list([{L, S} || {{L, _K}, S} <- LockEntries]),
    State2 = update_waiting_remote(AllDcs, LockStates, State),
    State3 = set_snapshot_time(ReadClock, State2),
    next_actions(State3).

% checks that we still have access to all the locks the transaction needed.
% This can only be violated if the lock server crashed, so it is sufficient
% to check that we still have an entry for the given process.
-spec check_release_locks(pid(), state()) -> ok | {error, Reason :: any()}.
check_release_locks(FromPid, State) ->
    case maps:find(FromPid, State#state.by_pid) of
        {ok, _} -> ok;
        error -> {error, 'locks no longer available'}
    end.

% Removes the locks for the given pid and determines who can
% get the locks now.
% Returns the respective actions to perform.
-spec remove_locks(pid(), snapshot_time(), state()) -> {actions(), state()}.
remove_locks(FromPid, CommitTime, State) ->
    case maps:find(FromPid, State#state.by_pid) of
        error ->
            % Pid entry does not exist -> do nothing
            {{[], #{}, []}, State};
        {ok, _PidState} ->
            StateWithoutPid = State#state{
                by_pid = maps:remove(FromPid, State#state.by_pid),
                snapshot_time = merge_snapshot_time(CommitTime, State#state.snapshot_time)
            },
            next_actions(StateWithoutPid)
    end.

-spec get_snapshot_time(state()) -> snapshot_time().
get_snapshot_time(State) ->
    State#state.snapshot_time.

-spec set_timer_active(boolean(), state()) -> state().
set_timer_active(B, S) -> S#state{timer_Active = B}.

-spec get_timer_active(state()) -> boolean().
get_timer_active(#state{timer_Active = B}) -> B.


-spec retries_for_waiting_remote(integer(), integer(), [dcid()], state()) -> {boolean(), lock_request_actions()}.
retries_for_waiting_remote(Time, RetryDelta, OtherDcs, State) ->
        WaitingRemote = [{PidState#pid_state.request_time, {L, K}} ||
        PidState <- maps:values(State#state.by_pid),
        {L, {waiting_remote, K}} <- PidState#pid_state.locks],
        WaitingRemoteLong = [{L, K} || {T, {L, K}} <- WaitingRemote, T + RetryDelta =< Time],
        ByLock = maps:map(fun(_, Ks) -> max_lock_kind(Ks) end, group_by_first(WaitingRemoteLong)),
        LockSpec = orddict:from_list(maps:to_list(ByLock)),
        LockRequests = maps:from_list([{Dc, LockSpec} || LockSpec /= [], Dc <- OtherDcs]),
    {WaitingRemote /= [], LockRequests}.


%% Internal functions


%% Adds a new process to the state.
%% The process is initially in the waiting state for all locks.
-spec add_process(requester(), integer(), {yes, dcid()} | no, antidote_locks:lock_spec(), state()) -> state().
add_process(Requester, RequestTime, IsRemote, Locks, State) ->
    {Pid, _} = Requester,
    State#state{
        by_pid = maps:put(Pid, #pid_state{
            locks = [{Lock, {waiting, Kind}} || {Lock, Kind} <- Locks],
            request_time = RequestTime,
            is_remote = IsRemote,
            requester = Requester
        }, State#state.by_pid)
    }.



%% Tries to acquire the locks for the given Pid
%% Res is true iff all locks were newly acquired.
%% Precondition: all required locks are available locally
-spec try_acquire_locks(pid(), state()) -> {boolean(), state()}.
try_acquire_locks(Pid, State) ->
    {ok, PidState} = maps:find(Pid, State#state.by_pid),
    LockStates = PidState#pid_state.locks,
    IsRemote = PidState#pid_state.is_remote,
    try_acquire_lock_list(Pid, LockStates, IsRemote, false, State).

-spec try_acquire_lock_list(pid(), orddict:orddict(antidote_locks:lock(), lock_state_with_kind()), {yes, dcid()} | no, boolean(), state()) -> {boolean(), state()}.
try_acquire_lock_list(Pid, LockStates, IsRemote, Changed, State) ->
    case LockStates of
        [] -> {Changed, State};
        [{Lock, {LockState, Kind}}| LockStatesRest] ->
            case LockState of
                held ->
                    try_acquire_lock_list(Pid, LockStatesRest, IsRemote, Changed, State);
                waiting ->
                    LS = lock_held_state(Lock, State),
                    case LS == none orelse (Kind == shared andalso LS == shared) of
                        true ->
                            % acquire lock
                            NewState = update_lock_state(State, Pid, Lock, {held, Kind}),
                            try_acquire_lock_list(Pid, LockStatesRest, IsRemote, true, NewState);
                        false ->
                            {false, State}
                    end;
                waiting_remote ->
                    {false, State}
            end
    end.

update_lock_state(State, Pid, Lock, LockState) ->
    State#state{
        by_pid = maps:update_with(Pid, fun(S) ->
            S#pid_state{
                locks = orddict:store(Lock, LockState, S#pid_state.locks)
            }
        end, State#state.by_pid)
    }.



-spec lock_held_state(antidote_locks:lock(), state()) -> shared | exclusive | none.
lock_held_state(Lock, State) ->
    lock_held_state_iter(Lock, maps:iterator(State#state.by_pid)).

lock_held_state_iter(Lock, Iter) ->
    case maps:next(Iter) of
        none -> none;
        {_Pid, PidState, NextIterator} ->
            case orddict:find(Lock, PidState#pid_state.locks) of
                {ok, {held, LockKind}} ->
                    % we can return the first entry we find, since the two kinds exclude each other
                    LockKind;
                _ ->
                    lock_held_state_iter(Lock, NextIterator)
            end
    end.



% calculates for which data centers there are still missing locks
-spec missing_locks_by_dc(list(dcid()), dcid(), [{antidote_locks:lock_spec(), antidote_lock_server:lock_crdt_value()}]) -> #{dcid() => antidote_locks:lock_spec()}.
missing_locks_by_dc(AllDcIds, MyDcId, Locks) ->
    Missing = lists:flatmap(fun({{_Lock,Kind}=I,LockValue}) -> [{Dc, I} || Dc <- missing_locks(AllDcIds, MyDcId, Kind, LockValue)] end, Locks),
    case Missing of
        [] -> maps:new();
        _ ->
            group_by_first(Missing)
    end.

% computes which locks are still missing
% returns a list of current owners for this lock
-spec missing_locks(list(dcid()), dcid(), antidote_locks:lock_kind(), antidote_lock_server:lock_crdt_value()) -> [dcid()].
missing_locks(AllDcIds, MyDcId, Kind, LockValue) ->
    Res = case Kind of
        shared ->
            %check that we own our own entry
            case maps:find(MyDcId, LockValue) of
                {ok, MyDcId} ->
                    % if we own our own entry, we need no further requests
                    [];
                error ->
                    % default value: owned by us
                    [];
                _ ->
                    % otherwise, request to get lock back from current owner:
                    CurrentOwner = maps:get(MyDcId, LockValue),
                    [CurrentOwner]
            end;
        exclusive ->
            % check that we own all datacenters
            case lists:all(fun(Dc) -> maps:get(Dc, LockValue, Dc) == MyDcId end, AllDcIds) of
                true ->
                    % if we own all lock parts, we need no further requests
                    [];
                false ->
                    % otherwise, request all parts from the current owners:
                    CurrentOwners = [maps:get(Dc, LockValue, Dc) || Dc <- AllDcIds],
                    lists:usort(CurrentOwners) -- [MyDcId]
            end
    end,
    logger:info("missing_locks(~p, ~p, ~p, ~p) -> ~p", [AllDcIds, MyDcId, Kind, LockValue, Res]),
    Res.




-spec group_by_first([{K,V}]) -> #{K => [V]}.
group_by_first(List) ->
    M1 = lists:foldl(fun({K,V}, M) ->
        maps:update_with(K, fun(L) -> [V|L] end, [V], M)
    end, maps:new(), List),
    maps:map(fun(_K,V) -> lists:reverse(V) end, M1).




% sets the given locks to the waiting_remote state
-spec set_lock_waiting_state(pid(), ordsets:ordset(antidote_locks:lock()), state(), lock_state(), lock_state()) -> state().
set_lock_waiting_state(Pid, Locks, State, OldS, NewS) ->
    State#state{
        by_pid = maps:update_with(Pid, fun(S) ->
                S#pid_state{
                    locks = set_lock_waiting_remote_list(Locks, S#pid_state.locks, OldS, NewS)
                }
            end, State#state.by_pid)
    }.

-spec set_lock_waiting_remote_list(ordsets:ordset(antidote_locks:lock()), orddict:orddict(antidote_locks:lock(), lock_state_with_kind()), lock_state(), lock_state()) -> orddict:orddict(antidote_locks:lock(), lock_state_with_kind()).
set_lock_waiting_remote_list(LocksToChange, Locks, OldS, NewS) ->
    [{L, case S == OldS andalso ordsets:is_element(L, LocksToChange) of
        true -> {NewS, K};
        false -> {S, K}
    end} || {L,{S,K}} <- Locks].


% Updates the lock state according to the new information about lock values.
% If all locks are received, state is changed from waiting_remote to waiting
-spec update_waiting_remote([dcid()], #{antidote_locks:lock() => antidote_lock_crdt:value()}, state()) -> state().
update_waiting_remote(AllDcs, LockValues, State) ->
    MyDcId = my_dc_id(State),
    State#state{
        by_pid = maps:map(fun(_Pid, S) ->
            S#pid_state{
                locks = update_waiting_remote_list(AllDcs, MyDcId, LockValues, S#pid_state.locks)
            }
        end, State#state.by_pid)
    }.


update_waiting_remote_list(AllDcs, MyDcId, LockValues, Locks) ->
    [{L, case maps:find(L, LockValues) of
        {ok, Value} ->
            NewS = case missing_locks(AllDcs, MyDcId, K, Value) of
                [] ->
                    case S of
                        waiting_remote -> waiting;
                        _ -> S
                    end;
                _ -> waiting_remote
            end,

            {NewS, K};
        error -> {S, K}
    end} || {L,{S,K}} <- Locks].




% determines the next actions to perform
-spec next_actions(state()) -> {actions(), state()}.
next_actions(State) ->
    % sort processes by request time, so that the processes waiting the longest will
    % acquire their locks first
    Pids = [Pid || {Pid, _} <- lists:sort(fun compare_by_request_time/2, maps:to_list(State#state.by_pid))],
    % try to acquire
    lists:foldl(fun(Pid, {{HandOverActions, LockRequestActions, Replies}, S}) ->
        {AllAcquired, S2} = try_acquire_locks(Pid, S),
        case AllAcquired of
            true ->
                PidState = maps:get(Pid, S#state.by_pid),
                NewReplies = [PidState#pid_state.requester | Replies],
                case PidState#pid_state.is_remote of
                    {yes, RequestingDc} ->
                        % send locks to remote data-center
                        LocksToTransfer = [{L,K} || {L, {_, K}} <- PidState#pid_state.locks],
                        HandOverActions2 = [{LocksToTransfer, RequestingDc, PidState#pid_state.requester} | HandOverActions],
                        % if there are still processes waiting for the lock locally, send requests to get them back
                        Locks = [Lock || {Lock, _} <- LocksToTransfer],
                        NewLockRequestActions = filter_waited_for_locks(Locks, S2),
                        % Change all waiting to waiting_remote for the sent locks
                        S3 = change_waiting_locks_to_remote(Locks, S2),
                        MergedLockRequestActions = merge_lock_request_actions(#{RequestingDc => NewLockRequestActions}, LockRequestActions),
                        % remove the lock
                        {Actions, S4} = remove_locks(Pid, vectorclock:new(), S3),
                        MergedActions = merge_actions({HandOverActions2, MergedLockRequestActions, NewReplies}, Actions),
                        {MergedActions, S4};
                    no ->
                        {{HandOverActions, LockRequestActions, NewReplies}, S2}
                end;
            false ->
                {{HandOverActions, LockRequestActions, Replies}, S2}
        end
    end, {{[], #{}, []}, State}, Pids).



compare_by_request_time({Pid1, PidState1}, {Pid2, PidState2}) ->
    {PidState1#pid_state.request_time, Pid1} =< {PidState2#pid_state.request_time, Pid2}.




% changes lock state from waiting to waiting_remote for all given locks
-spec change_waiting_locks_to_remote([antidote_locks:lock()], state()) -> state().
change_waiting_locks_to_remote(Locks, State) ->
    State#state{
        by_pid = maps:map(fun(_Pid, PidState) ->
                PidState#pid_state{
                    locks = [case LockState == waiting andalso lists:member(Lock, Locks) of
                        true -> {Lock, {waiting_remote, LockKind}};
                        false -> L
                    end || L = {Lock, {LockState, LockKind}} <- PidState#pid_state.locks  ]
                }
            end, State#state.by_pid)
    }.

-spec merge_lock_request_actions(#{dcid() => antidote_locks:lock_spec()}, #{dcid() => antidote_locks:lock_spec()}) -> #{dcid() => antidote_locks:lock_spec()}.
merge_lock_request_actions(A, B) ->
    maps:fold(fun
        (_, [], Acc) ->
            Acc;
        (K, V1, Acc) ->
            maps:update_with(K, fun(V2) -> ordsets:union(V1, V2) end, V1, Acc)
    end, B, A).


-spec merge_actions(actions(), actions()) -> actions().
merge_actions({H1, L1, R1}, {H2, L2, R2}) ->
    {H1++H2, merge_lock_request_actions(L1, L2), R1 ++ R2}.

%
-spec filter_waited_for_locks([antidote_locks:lock()], state()) -> antidote_locks:lock_spec().
filter_waited_for_locks(Locks, State) ->
    [{L, Kind} || L <- Locks, (Kind = max_lock_waiting_kind(L, State)) /= none].

% goes through all the processes waiting for the given lock and returns the maximum required lock level
-spec max_lock_waiting_kind(antidote_locks:lock(), state()) -> shared | exclusive | none.
max_lock_waiting_kind(Lock, State) ->
    max_lock_waiting_kind_iter(Lock, maps:iterator(State#state.by_pid), none).

max_lock_waiting_kind_iter(Lock, Iter, Res) ->
    case maps:next(Iter) of
        none -> Res;
        {_Pid, PidState, NextIterator} ->
            case orddict:find(Lock, PidState#pid_state.locks) of
                {ok, {waiting, LockKind}} ->
                    % we can return the first entry we find, since the two kinds exclude each other
                    max_lock_waiting_kind_iter(Lock, NextIterator, max_lock_kind(LockKind, Res));
                _ ->
                    max_lock_waiting_kind_iter(Lock, NextIterator, Res)
            end
    end.


max_lock_kind(Ls) ->
    lists:foldl(fun max_lock_kind/2, none, Ls).

-spec max_lock_kind(shared | exclusive | none, shared | exclusive | none) -> shared | exclusive | none.
max_lock_kind(A, none) -> A;
max_lock_kind(none, A) -> A;
max_lock_kind(exclusive, _) -> exclusive;
max_lock_kind(_, exclusive) -> exclusive;
max_lock_kind(shared, shared) -> shared.


-spec set_snapshot_time(snapshot_time(), state()) -> state().
set_snapshot_time(Time, State) ->
    State#state{snapshot_time = Time}.



-spec merge_snapshot_time(snapshot_time(), snapshot_time()) -> snapshot_time().
merge_snapshot_time(V1, V2) -> vectorclock:max([V1, V2]).


-ifdef(TEST).
% run with: rebar3 eunit --module=antidote_lock_server_state

group_by_first_test() ->
    M = group_by_first([{a, 1}, {b, 2}, {a, 3}, {a, 4}]),
    ?assertEqual(#{a => [1, 3, 4], b => [2]}, M).

max_lock_kind_test() ->
    ?assertEqual(none, max_lock_kind(none, none)),
    ?assertEqual(shared, max_lock_kind(none, shared)),
    ?assertEqual(exclusive, max_lock_kind(none, exclusive)),
    ?assertEqual(shared, max_lock_kind(shared, none)),
    ?assertEqual(shared, max_lock_kind(shared, shared)),
    ?assertEqual(exclusive, max_lock_kind(shared, exclusive)),
    ?assertEqual(exclusive, max_lock_kind(exclusive, none)),
    ?assertEqual(exclusive, max_lock_kind(exclusive, shared)),
    ?assertEqual(exclusive, max_lock_kind(exclusive, exclusive)),
    ok.


shared_lock_ok_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    {Actions, _S2} = new_request(R1, 10, undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}}], S1),
    {HandOverActions, LockRequestActions, Replies} = Actions,
    ?assertEqual([], HandOverActions),
    ?assertEqual(#{}, LockRequestActions),
    ?assertEqual([R1], Replies),
    ok.

shared_lock_fail_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    % data center 3 has our lock, so we need to request it
    {Actions, _S2} = new_request(R1, 10, undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc2, dc3 => dc3}}], S1),
    {HandOverActions, LockRequestActions, Replies} = Actions,
    ?assertEqual([], HandOverActions),
    ?assertEqual(#{dc3 => [{lock1, shared}]}, LockRequestActions),
    ?assertEqual([], Replies),
    ok.

shared_lock_fail2_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    % data center 3 has our lock, so we need to request it
    {Actions, _S2} = new_request(R1, 10, undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc3}}], S1),
    {HandOverActions, LockRequestActions, Replies} = Actions,
    ?assertEqual([], HandOverActions),
    ?assertEqual(#{dc3 => [{lock1, shared}]}, LockRequestActions),
    ?assertEqual([], Replies),
    ok.


shared_lock_missing_local_test() ->
    SInit = initial(dc1),
    R1 = {p1, t1},
    % data center 3 has our lock, so we need to request it
    {Actions1, S1} = new_request(R1, 10, undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc3}}], SInit),
    ?assertEqual({[], #{dc3 => [{lock1, shared}]}, []}, Actions1),

    % later we receive the lock from dc3 and we can get the lock
    {Actions2, _S2} = on_remote_locks_received(time1, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], S1),
    ?assertEqual({[], #{}, [R1]}, Actions2),
    ok.

exclusive_lock_missing_local_test() ->
    SInit = initial(dc1),
    R1 = {p1, t1},
    % data centers 2 and 3 have the locks, so we need to request it
    {Actions1, S1} = new_request(R1, 10, undefined, [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc3, dc2 => dc2, dc3 => dc2}}], SInit),
    ?assertEqual({[], #{dc2 => [{lock1, exclusive}], dc3 => [{lock1, exclusive}]}, []}, Actions1),

    % later we receive the lock from dc2, which is not enough to acquire the lock
    {Actions2, S2} = on_remote_locks_received(undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual({[], #{}, []}, Actions2),

    % when we have all the locks, we can acquire the lock
    {Actions3, _S3} = on_remote_locks_received(undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S2),
    ?assertEqual({[], #{}, [R1]}, Actions3),
    ok.

exclusive_lock_missing_local2_test() ->
    SInit = initial(dc1),
    R1 = {p1, t1},
    % data centers 2 and 3 have the locks, so we need to request it
    Locks = [
        {{lock1, exclusive}, #{dc1 => dc3, dc2 => dc2, dc3 => dc2}},
        {{lock2, exclusive}, #{dc1 => dc3, dc2 => dc2, dc3 => dc2}}
    ],
    {Actions1, S1} = new_request(R1, 10, undefined, [dc1, dc2, dc3], Locks, SInit),
    ?assertEqual({[], #{dc2 => [{lock1, exclusive}, {lock2, exclusive}], dc3 => [{lock1, exclusive}, {lock2, exclusive}]}, []}, Actions1),

    % first we only get lock 2
    {Actions2, S2} = on_remote_locks_received(undefined, [dc1, dc2, dc3], [{{lock2, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual({[], #{}, []}, Actions2),

    % when we have all the locks, we can acquire the lock
    ReceivedLocks = [
        {{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock2, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}],
    {Actions3, _S3} = on_remote_locks_received(undefined, [dc1, dc2, dc3], ReceivedLocks, S2),
    ?assertEqual({[], #{}, [R1]}, Actions3),
    ok.



shared_locks_missing_local_test() ->
    SInit = initial(dc1),
    R1 = {p1, t1},
    % data center 2 has our lock2, so we need to request it
    RLocks = [
        {{lock1, shared}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}},
        {{lock2, shared}, #{dc1 => dc2, dc2 => dc2, dc3 => dc3}},
        {{lock3, shared}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}}
    ],
    {Actions1, S1} = new_request(R1, 10, undefined, [dc1, dc2, dc3], RLocks, SInit),
    ?assertEqual({[], #{dc2 => [{lock2, shared}]}, []}, Actions1),

    % later we receive the lock from dc2 and we can get the lock
    {Actions2, _S2} = on_remote_locks_received(undefined, [dc1, dc2, dc3], [{{lock2, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], S1),
    ?assertEqual({[], #{}, [R1]}, Actions2),
    ok.


snapshot_time_test() ->
    SInit = initial(dc1),
    VC1 = vectorclock:from_list([{dc2, 5}, {dc3, 10}]),
    VC2 = vectorclock:from_list([{dc2, 8}, {dc3, 10}]),
    VC3 = vectorclock:from_list([{dc2, 8}, {dc3, 12}]),
    R1 = {p1, t1},

    ?assertEqual(vectorclock:new(), get_snapshot_time(SInit)),

    % data centers 2 and 3 have the locks, so we need to request it
    {Actions1, S1} = new_request(R1, 10, VC1, [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc3, dc2 => dc2, dc3 => dc2}}], SInit),
    ?assertEqual({[], #{dc2 => [{lock1, exclusive}], dc3 => [{lock1, exclusive}]}, []}, Actions1),

    ?assertEqual(VC1, get_snapshot_time(S1)),

    % later we receive the lock from dc2, which is not enough to acquire the lock
    {Actions2, S2} = on_remote_locks_received(VC2, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual({[], #{}, []}, Actions2),

    ?assertEqual(VC2, get_snapshot_time(S2)),

    % when we have all the locks, we can acquire the lock
    {Actions3, S3} = on_remote_locks_received(VC3, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S2),
    ?assertEqual({[], #{}, [R1]}, Actions3),

    ?assertEqual(VC3, get_snapshot_time(S3)),
    ok.

exclusive_lock_ok_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    {Actions, _S2} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S1),
    {HandOverActions, LockRequestActions, Replies} = Actions,
    ?assertEqual([], HandOverActions),
    ?assertEqual(#{}, LockRequestActions),
    ?assertEqual([R1], Replies),
    ok.


exclusive_lock_fail_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    {Actions, _S2} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc2}}], S1),
    {HandOverActions, LockRequestActions, Replies} = Actions,
    ?assertEqual([], HandOverActions),
    ?assertEqual(#{dc2 => [{lock1, exclusive}]}, LockRequestActions),
    ?assertEqual([], Replies),
    ok.


two_shared_locks_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}}], SInit),
    {HandOverActions1, LockRequestActions1, Replies1} = Actions1,
    ?assertEqual([], HandOverActions1),
    ?assertEqual(#{}, LockRequestActions1),
    ?assertEqual([R1], Replies1),

    % then R2 tries to acquire the same lock
    R2 = {p2, t2},
    {Actions2, _S2} = new_request(R2, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}}], S1),
    {HandOverActions2, LockRequestActions2, Replies2} = Actions2,
    ?assertEqual([], HandOverActions2),
    ?assertEqual(#{}, LockRequestActions2),
    ?assertEqual([R2], Replies2),

    ok.

two_exclusive_locks_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], SInit),
    {HandOverActions1, LockRequestActions1, Replies1} = Actions1,
    ?assertEqual([], HandOverActions1),
    ?assertEqual(#{}, LockRequestActions1),
    ?assertEqual([R1], Replies1),

    % then R2 tries to acquire the same lock, does not get it
    R2 = {p2, t2},
    {Actions2, S2} = new_request(R2, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S1),
    {HandOverActions2, LockRequestActions2, Replies2} = Actions2,
    ?assertEqual([], HandOverActions2),
    ?assertEqual(#{}, LockRequestActions2),
    ?assertEqual([], Replies2),

    % when R1 releases it's lock, R2 gets it
    ?assertEqual(ok, check_release_locks(p1, S2)),
    {Actions3, S3} = remove_locks(p1, vectorclock:new(), S2),
    {HandOverActions3, LockRequestActions3, Replies3} = Actions3,
    ?assertEqual([], HandOverActions3),
    ?assertEqual(#{}, LockRequestActions3),
    ?assertEqual([R2], Replies3),

    % cannot release locks a second time
    ?assertMatch({error, _}, check_release_locks(p1, S3)),

    ?assertEqual({{[], #{}, []}, S3}, remove_locks(p1, vectorclock:new(), S3)),
    ok.

shared_lock_with_remote_shared_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], SInit),
    ?assertEqual({[], #{}, [R1]}, Actions1),

    % then remote DC2 tries to acquire the same lock
    R2 = {p2, t2},
    {Actions2, _S2} = new_remote_request(R2, 20, [{lock1, shared}], dc2, S1),
    ?assertEqual({[{[{lock1,shared}],dc2,{p2,t2}}], #{}, [R2]}, Actions2),
    ok.

shared_lock_with_remote_exclusive_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], SInit),
    ?assertEqual({[], #{}, [R1]}, Actions1),

    % then remote DC2 tries to acquire the same lock
    R2 = {p2, t2},
    {Actions2, S2} = new_remote_request(R2, 20, [{lock1, exclusive}], dc2, S1),
    ?assertEqual({[], #{}, []}, Actions2),

    % when R1 releases it's lock, the remote gets it
    {Actions3, _S3} = remove_locks(p1, vectorclock:new(), S2),
    ?assertEqual({[{[{lock1,exclusive}],dc2,{p2,t2}}], #{}, [R2]}, Actions3),
    ok.


exclusive_lock_with_remote_shared_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], SInit),
    ?assertEqual({[], #{}, [R1]}, Actions1),

    % then remote DC2 tries to acquire the same lock
    R2 = {p2, t2},
    {Actions2, S2} = new_remote_request(R2, 20, [{lock1, shared}], dc2, S1),
    ?assertEqual({[], #{}, []}, Actions2),

    % when R1 releases it's lock, the remote gets it
    {Actions3, _S3} = remove_locks(p1, vectorclock:new(), S2),
    ?assertEqual({[{[{lock1,shared}],dc2,{p2,t2}}], #{}, [R2]}, Actions3),
    ok.

exclusive_lock_with_remote_exclusive_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], SInit),
    ?assertEqual({[], #{}, [R1]}, Actions1),

    % then remote DC2 tries to acquire the same lock
    R2 = {p2, t2},
    {Actions2, S2} = new_remote_request(R2, 20, [{lock1, exclusive}], dc2, S1),
    ?assertEqual({[], #{}, []}, Actions2),

    % when R1 releases it's lock, the remote gets it
    {Actions3, _S3} = remove_locks(p1, vectorclock:new(), S2),
    ?assertEqual({[{[{lock1,exclusive}],dc2,{p2,t2}}], #{}, [R2]}, Actions3),
    ok.


locks_request_again_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], SInit),
    ?assertEqual({[], #{}, [R1]}, Actions1),

    % then R2 tries to acquire the same lock
    R2 = {p2, t2},
    {Actions2, S2} = new_request(R2, 30, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual({[], #{}, []}, Actions2),

    % then a remote request for the lock comes in with an earlier timestamp
    R3 = {p3, t3},
    {Actions3, S3} = new_remote_request(R3, 20, [{lock1, exclusive}], dc2, S2),
    ?assertEqual({[], #{}, []}, Actions3),

    % when R1 releases it's lock, the remote gets it, but we also want it back
    {Actions4, S4} = remove_locks(p1, vectorclock:new(), S3),
    ?assertEqual({[{[{lock1,exclusive}],dc2,{p3,t3}}], #{dc2 => [{lock1, exclusive}]}, [R3]}, Actions4),

    % later we get it back from dc2 and R2 can get the lock
    {Actions5, _S5} = on_remote_locks_received(vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S4),
    ?assertEqual({[], #{}, [R2]}, Actions5),
    ok.

missing_locks_empty_test() ->
    Missing = missing_locks([dc1, dc2, dc3], dc1, exclusive, #{}),
    ?assertEqual([dc2, dc3], Missing),
    ok.

missing_locks_by_dc_empty_test() ->
    Missing = missing_locks_by_dc([dc1, dc2, dc3], dc1, [{{lock1, exclusive}, #{}}]),
    ?assertEqual(#{dc2 => [{lock1, exclusive}], dc3 => [{lock1, exclusive}]}, Missing),
    ok.


locks_missing_local_retry_test() ->
    SInit = initial(dc1),
    R1 = {p1, t1},
    % data center 2 has our lock2, so we need to request it
    RLocks1 = [
        {{lock1, shared}, #{dc1 => dc3, dc2 => dc2, dc3 => dc3}},
        {{lock2, exclusive}, #{dc1 => dc2, dc2 => dc2, dc3 => dc3}},
        {{lock3, shared}, #{dc1 => dc2, dc2 => dc2, dc3 => dc3}}
    ],
    {Actions1, S1} = new_request(R1, 10, undefined, [dc1, dc2, dc3], RLocks1, SInit),
    ?assertEqual({[], #{dc2 => [{lock2, exclusive}, {lock3, shared}], dc3 => [{lock1, shared}, {lock2, exclusive}]}, []}, Actions1),


    R2 = {p2, t2},
    RLocks2 = [
        {{lock1, exclusive}, #{dc1 => dc3, dc2 => dc2, dc3 => dc3}},
        {{lock4, shared}, #{dc1 => dc2, dc2 => dc2, dc3 => dc3}}
    ],
    {Actions2, S2} = new_request(R2, 20, undefined, [dc1, dc2, dc3], RLocks2, S1),
    ?assertEqual({[], #{dc2 => [{lock1,exclusive}, {lock4, shared}], dc3 => [{lock1, exclusive}]}, []}, Actions2),

    % request again later:
    {StillWaiting3, Actions3} = retries_for_waiting_remote(25, 10, [dc2, dc3], S2),
    Missing3 = [{lock1, shared}, {lock2, exclusive}, {lock3, shared}],
    ?assertEqual(#{dc2 => Missing3, dc3 => Missing3}, Actions3),
    ?assertEqual(true, StillWaiting3),

    % request again later:
    {StillWaiting4, Actions4} = retries_for_waiting_remote(35, 10, [dc2, dc3], S2),
    Missing4 = [{lock1, exclusive}, {lock2, exclusive}, {lock3, shared}, {lock4, shared}],
    ?assertEqual(#{dc2 => Missing4, dc3 => Missing4}, Actions4),
    ?assertEqual(true, StillWaiting4),
    ok.


-endif.
