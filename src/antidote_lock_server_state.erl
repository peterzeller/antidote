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

-export_type([state/0]).

-export([initial/1, my_dc_id/1, add_process/5, try_acquire_locks/2, missing_locks_by_dc/3, missing_locks/4, set_lock_waiting_remote/3, check_release_locks/2, remove_locks/2, add_lock_waiting/4, filter_waited_for_locks/2]).


% state invariants:
% - if a process has an exclusive lock, no other process has a lock (held state)
% - if a process is in held or waiting state, the corresponding lock is locally available
% - NOT if a lock is not held by anyone, then no one is waiting for it (if someone where, he should have taken it)
% - if a process holds a lock, it is not waiting for any locks that are smaller (=> avoiding deadlocks)

-record(pid_state, {
    locks :: orddict:orddict(antidote_locks:lock(), lock_state()),
    request_time :: integer(),
    is_remote :: {yes, dcid()} | no,
    requester :: requester()
}).

-record(state, {
    %% own datacenter id
    dc_id :: dcid(),
    by_pid :: #{pid() => #pid_state{}}
}).

-opaque state() :: #state{}.



-type requester() :: {pid(), Tag :: term()}.

-type lock_state() ::
    {waiting | held | waiting_remote, antidote_locks:lock_kind()}.


-spec initial(dcid()) -> state().
initial(MyDcId) -> #state{
    dc_id = MyDcId,
    by_pid = #{}
}.

-spec my_dc_id(state()) -> dcid().
my_dc_id(State) ->
    State#state.dc_id.


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

-spec try_acquire_lock_list(pid(), ordsets:ordset(antidote_locks:lock(), lock_state()), {yes, dcid()} | no, state()) -> {boolean(), state()}.
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
                    throw({'precondition violated: all locks must be available locally', Lock})
            end
    end.

update_lock_state(State, Pid, Lock, LockState) ->
    State#state{
        by_pid = maps:update_with(Pid, fun(S) ->
            S#pid_state{
                locks = orddict:append(Lock, LockState, S#pid_state.locks)
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
    LockValueOwners = maps:get(owners, LockValue),
    case Kind of
        shared ->
            %check that we own at least one entry in the map
            case lists:member(MyDcId, maps:values(LockValueOwners)) of
                true ->
                    % if we own one or more entries, we need no further requests
                    [];
                false ->
                    % otherwise, request to get lock back from current owner:
                    CurrentOwner = maps:get(MyDcId, LockValueOwners),
                    [CurrentOwner]
            end;
        exclusive ->
            % check that we own all datacenters
            case lists:all(fun(Dc) -> maps:get(Dc, LockValueOwners, false) == MyDcId end, AllDcIds) of
                true ->
                    % if we own all lock parts, we need no further requests
                    [];
                false ->
                    % otherwise, request all parts from the current owners:
                    lists:usort(maps:values(LockValueOwners)) -- [MyDcId]
            end
    end.




-spec group_by_first([{K,V}]) -> #{K => [V]}.
group_by_first(List) ->
    M1 = lists:foldl(fun({K,V}, M) ->
        maps:update_with(K, fun(L) -> [V|L] end, [], M)
    end, maps:new(), List),
    maps:map(fun(_K,V) -> lists:reverse(V) end, M1).


%% Adds a new process to the state.
%% The process is initially in the waiting state.
-spec add_lock_waiting(requester(), integer(), antidote_locks:lock_spec(), antidote_lock_server_state:state()) -> antidote_lock_server_state:state().
add_lock_waiting(Requester, RequestTime, Locks, State) ->
    {RequesterPid, _} = Requester,
    PidState = #pid_state{
        locks = [{Lock, {waiting, Kind}} || {Lock, Kind} <- Locks],
        request_time = RequestTime,
        is_remote = no,
        requester = Requester
    },
    State#state{
        by_pid = maps:put(RequesterPid, PidState, State#state.by_pid)
    }.

% sets the given locks to the waiting_remote state
-spec set_lock_waiting_remote(pid(), ordsets:ordset(antidote_locks:lock()), antidote_lock_server_state:state()) -> antidote_lock_server_state:state().
set_lock_waiting_remote(Pid, Locks, State) ->
    State#state{
        by_pid = maps:update_with(Pid, fun(S) ->
                S#pid_state{
                    locks = set_lock_waiting_remote_list(Locks, S#pid_state.locks)
                }
            end, State#state.by_pid)
    }.

set_lock_waiting_remote_list(LocksToChange, Locks) ->
    [{L, case ordsets:is_element(L, LocksToChange) of
        true -> {L, {waiting_remote, K}};
        false -> {L, {S, K}}
    end} || {L,{S,K}} <- Locks].
% optimized version:
%%set_lock_waiting_remote_list([], Locks) -> Locks;
%%set_lock_waiting_remote_list(_, []) -> [];
%%set_lock_waiting_remote_list([X|Xs], [{L,S}|R]) when X < L ->
%%    [{L,S}| set_lock_waiting_remote_list([X|Xs], R)];
%%set_lock_waiting_remote_list([X|Xs], [{L,{_,K}}|R]) when X == L ->
%%    [{L,{waiting_remote,K}}| set_lock_waiting_remote_list(Xs, R)];
%%set_lock_waiting_remote_list([X|Xs], Locks) ->
%%    set_lock_waiting_remote_list(Xs, Locks).


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
-spec remove_locks(pid(), state()) -> {HandOverActions, LockRequestActions, Replies, state()}
    when HandOverActions :: [{antidote_locks:lock_spec(), dcid()}],
    LockRequestActions :: #{dcid() => antidote_locks:lock_spec()},
    Replies :: [requester()].
remove_locks(FromPid, State) ->
    case maps:find(FromPid, State#state.by_pid) of
        error ->
            % Pid entry does not exist -> do nothing
            {[], #{}, [], State};
        {ok, PidState} ->
            StateWithoutPid = State#state{
                by_pid = maps:remove(FromPid, State#state.by_pid)
            },
            % sort processes by request time, so that the processes waiting the longest will
            % acquire their locks first
            Pids = [Pid || {Pid, _} <- lists:sort(fun compare_by_request_time/2, maps:to_list(StateWithoutPid#state.by_pid))],
            % try to acquire
            lists:foldl(fun(Pid, {HandOverActions, LockRequestActions, Replies, S}) ->
                {AllAcquired, S2} = try_acquire_locks(Pid, S),
                case AllAcquired of
                    true ->
                        PidState = maps:get(Pid, S#state.by_pid),
                        NewReplies = [PidState#pid_state.requester | Replies],
                        case PidState#pid_state.is_remote of
                            {yes, RequestingDc} ->
                                % send locks to remote data-center
                                LocksToTransfer = PidState#pid_state.locks,
                                HandOverActions2 = [{LocksToTransfer, RequestingDc} | HandOverActions],
                                % if there are still processes waiting for the lock locally, send requests to get them back
                                Locks = [Lock || {Lock, _} <- LocksToTransfer],
                                NewLockRequestActions = filter_waited_for_locks(Locks, S2),
                                % Change all waiting to waiting_remote for the sent locks
                                S3 = change_waiting_locks_to_remote(Locks, S2),
                                {HandOverActions2, merge_lock_request_actions(#{RequestingDc => NewLockRequestActions}, LockRequestActions), NewReplies, S3};
                            no ->
                                {HandOverActions, LockRequestActions, NewReplies, S2}
                        end;
                    false ->
                        {HandOverActions, LockRequestActions, Replies, S2}
                end
            end, {[], #{}, [], StateWithoutPid}, Pids)
    end.

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
        maps:update_with(K, fun(V2) -> ordsets:union(V1, V2) end, [], Acc)
    end, B, A).


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
