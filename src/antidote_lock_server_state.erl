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
    new_request/5, new_remote_request/5, on_remote_locks_received/4, remove_locks/2, check_release_locks/2, get_snapshot_time/1]).


% state invariants:
% - if a process has an exclusive lock, no other process has a lock (held state)
% - if a process is in held or waiting state, the corresponding lock is locally available
% - NOT if a lock is not held by anyone, then no one is waiting for it (if someone where, he should have taken it)
% - if a process holds a lock, it is not waiting for any locks that are smaller (=> avoiding deadlocks)

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
    snapshot_time = undefined :: snapshot_time(),
    by_pid = #{} :: #{pid() => #pid_state{}}
}).

-opaque state() :: #state{}.



-type requester() :: {pid(), Tag :: term()}.

-type lock_state() :: waiting | held | waiting_remote.

-type lock_state_with_kind() ::
    {lock_state(), antidote_locks:lock_kind()}.

-type actions() :: {
    HandOverActions :: [{antidote_locks:lock_spec(), dcid(), requester()}],
    LockRequestActions :: #{dcid() => antidote_locks:lock_spec()},
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



-spec new_request(requester(), integer(), [dcid()], ordsets:ordset({antidote_locks:lock_spec_item(), antidote_lock_crdt:value()}), state()) -> {actions(), state()}.
new_request(Requester, RequestTime, AllDcIds, LockEntries, State) ->
    {RequesterPid, _} = Requester,
    MyDcId = my_dc_id(State),
    Locks = [L || {L, _} <- LockEntries],
    State2 = add_process(Requester, RequestTime, no, Locks, State),
    RequestsByDc = missing_locks_by_dc(AllDcIds, MyDcId, LockEntries),
    case maps:size(RequestsByDc) of
        0 ->
            % we have all locks locally
            next_actions(State2);
        _ ->
            % tell other data centers that we need locks
            % for shared locks, ask to get own lock back
            % for exclusive locks, ask everyone to give their lock

            WaitingLocks = [L || {L, _} <- lists:concat(maps:values(RequestsByDc))],
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
    LockStates = maps:from_list([{L, S} || {{L, _K}, S} <- LockEntries]),
    State2 = update_waiting_remote(AllDcs, LockStates, State),
    State3 = set_snapshot_time(State2, ReadClock),
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
-spec remove_locks(pid(), state()) -> {actions(), state()}.
remove_locks(FromPid, State) ->
    case maps:find(FromPid, State#state.by_pid) of
        error ->
            % Pid entry does not exist -> do nothing
            {[], #{}, [], State};
        {ok, _PidState} ->
            StateWithoutPid = State#state{
                by_pid = maps:remove(FromPid, State#state.by_pid)
            },
            next_actions(StateWithoutPid)
    end.

-spec get_snapshot_time(state()) -> snapshot_time().
get_snapshot_time(State) ->
    State#state.snapshot_time.

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
%% Res is true iff all locks were acquired.
%% Precondition: all required locks are available locally
-spec try_acquire_locks(pid(), state()) -> {boolean(), state()}.
try_acquire_locks(Pid, State) ->
    {ok, PidState} = maps:find(Pid, State#state.by_pid),
    LockStates = PidState#pid_state.locks,
    IsRemote = PidState#pid_state.is_remote,
    try_acquire_lock_list(Pid, LockStates, IsRemote, State).

-spec try_acquire_lock_list(pid(), orddict:orddict(antidote_locks:lock(), lock_state_with_kind()), {yes, dcid()} | no, state()) -> {boolean(), state()}.
try_acquire_lock_list(Pid, LockStates, IsRemote, State) ->
    case LockStates of
        [] -> {true, State};
        [{Lock, {LockState, Kind}}| LockStatesRest] ->
            case LockState of
                held ->
                    try_acquire_lock_list(Pid, LockStatesRest, IsRemote, State);
                waiting ->
                    LS = lock_held_state(Lock, State),
                    case LS == none orelse (IsRemote == no andalso Kind == shared andalso LS == shared) of
                        true ->
                            % acquire lock
                            NewState = update_lock_state(State, Pid, Lock, {held, Kind}),
                            try_acquire_lock_list(Pid, LockStatesRest, IsRemote, NewState);
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
                    case PidState#pid_state.is_remote of
                        {yes, _} -> exclusive; % remote requests need exclusive access
                        no -> LockKind
                    end;
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
    case Kind of
        shared ->
            %check that we own at least one entry in the map
            case lists:member(MyDcId, maps:values(LockValue)) of
                true ->
                    % if we own one or more entries, we need no further requests
                    [];
                false ->
                    % otherwise, request to get lock back from current owner:
                    CurrentOwner = maps:get(MyDcId, LockValue),
                    [CurrentOwner]
            end;
        exclusive ->
            % check that we own all datacenters
            case lists:all(fun(Dc) -> maps:get(Dc, LockValue, false) == MyDcId end, AllDcIds) of
                true ->
                    % if we own all lock parts, we need no further requests
                    [];
                false ->
                    % otherwise, request all parts from the current owners:
                    lists:usort(maps:values(LockValue)) -- [MyDcId]
            end
    end.




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


% sets the given locks to the waiting_remote state
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

            {L, {NewS, K}};
        error -> {L, {S, K}}
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
                case PidState#pid_state.is_remote of
                    {yes, RequestingDc} ->
                        % send locks to remote data-center
                        LocksToTransfer = PidState#pid_state.locks,
                        HandOverActions2 = [{LocksToTransfer, RequestingDc, PidState#pid_state.requester} | HandOverActions],
                        % if there are still processes waiting for the lock locally, send requests to get them back
                        Locks = [Lock || {Lock, _} <- LocksToTransfer],
                        NewLockRequestActions = filter_waited_for_locks(Locks, S2),
                        % Change all waiting to waiting_remote for the sent locks
                        S3 = change_waiting_locks_to_remote(Locks, S2),
                        {{HandOverActions2, merge_lock_request_actions(#{RequestingDc => NewLockRequestActions}, LockRequestActions), Replies}, S3};
                    no ->
                        NewReplies = [PidState#pid_state.requester | Replies],
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
    maps:fold(fun(K, V1, Acc) ->
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
                    LockKind2 = case PidState#pid_state.is_remote of
                        {yes, _} -> exclusive; % remote requests need exclusive access
                        no -> LockKind
                    end,
                    max_lock_waiting_kind_iter(Lock, NextIterator, max_lock_kind(LockKind2, Res));
                _ ->
                    max_lock_waiting_kind_iter(Lock, NextIterator, Res)
            end
    end.


-spec max_lock_kind(shared | exclusive | none, shared | exclusive | none) -> shared | exclusive | none.
max_lock_kind(A, none) -> A;
max_lock_kind(none, A) -> A;
max_lock_kind(exclusive, _) -> exclusive;
max_lock_kind(_, exclusive) -> exclusive;
max_lock_kind(shared, shared) -> shared.


-spec set_snapshot_time(state(), snapshot_time()) -> state().
set_snapshot_time(State, Time) ->
    State#state{snapshot_time = Time}.



-ifdef(TEST).
% run with: rebar3 eunit --module=antidote_lock_server_state

group_by_first_test() ->
    M = group_by_first([{a, 1}, {b, 2}, {a, 3}, {a, 4}]),
    ?assertEqual(#{a => [1, 3, 4], b => [2]}, M).

shared_lock_ok_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    {Actions, _S2} = new_request(R1, 10, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}}], S1),
    {HandOverActions, LockRequestActions, Replies} = Actions,
    ?assertEqual([], HandOverActions),
    ?assertEqual(#{}, LockRequestActions),
    ?assertEqual([R1], Replies),
    ok.

shared_lock_fail_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    % data center 3 has our lock, so we need to request it
    {Actions, _S2} = new_request(R1, 10, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc2, dc3 => dc3}}], S1),
    {HandOverActions, LockRequestActions, Replies} = Actions,
    ?assertEqual([], HandOverActions),
    ?assertEqual(#{dc3 => [{lock1, shared}]}, LockRequestActions),
    ?assertEqual([], Replies),
    ok.

%%HandOverActions :: [{antidote_locks:lock_spec(), dcid(), requester()}],
%%LockRequestActions :: #{dcid() => antidote_locks:lock_spec()},
%%Replies :: [requester()]}.


-endif.
