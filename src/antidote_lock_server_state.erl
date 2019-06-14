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

-export([initial/0, my_dc_id/1, add_process/5, try_acquire_locks/2, try_acquire_remote_locks/5, missing_locks_by_dc/3, missing_locks/4, set_lock_waiting_remote/3, check_release_locks/3, remove_locks/2, add_lock_waiting/4]).

-opaque state() :: #state{}.

% state invariants:
% - if a process has an exclusive lock, no other process has a lock (held state)
% - if a process is in held or waiting state, the corresponding lock is locally available
% - NOT if a lock is not held by anyone, then no one is waiting for it (if someone where, he should have taken it)
% - if a process holds a lock, it is not waiting for any locks that are smaller (=> avoiding deadlocks)
-record(state, {
    %% own datacenter id
    dc_id :: dcid(),
    by_pid :: #{pid() => #pid_state{}}
}).

-record(pid_state, {
    locks :: orddict:orddict(antidote_locks:lock(), lock_state()),
    request_time :: integer(),
    is_remote :: boolean(),
    requester :: requester()
}).

-type requester() :: {pid(), Tag :: term()}.

-type lock_state() ::
    {waiting | held | waiting_remote, antidote_locks:lock_kind()}.


initial() -> #state{}.

-spec my_dc_id(state()) -> dcid().
my_dc_id(State) ->
    State#state.dc_id.


-spec add_process(requester(), integer(), boolean(), antidote_locks:lock_spec(), state()) -> state().
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
%% Precondition: all remote locks are already available locally
-spec try_acquire_locks(pid(), state()) -> {boolean(), state()}.
try_acquire_locks(Pid, State) ->
    {ok, PidState} = maps:find(Pid, State#state.by_pid),
    LockStates = PidState#pid_state.locks,
    try_acquire_lock_list(Pid, LockStates, State).

-spec try_acquire_lock_list(pid(), ordsets:ordset(antidote_locks:lock(), lock_state()), state()) -> {boolean(), state()}.
try_acquire_lock_list(Pid, LockStates, State) ->
    case LockStates of
        [] -> {true, State};
        [{Lock, {LockState, Kind}}| LockStatesRest] ->
            case LockState of
                held ->
                    try_acquire_lock_list(Pid, LockStatesRest, State);
                waiting ->
                    LS = lock_held_state(Lock, State),
                    case LS == none orelse Kind == shared andalso LS == shared  of
                        true ->
                            {false, State};
                        false ->
                            % acquire lock
                            NewState = update_lock_state(State, Pid, Lock, {held, Kind}),
                            try_acquire_lock_list(Pid, LockStatesRest, NewState)
                    end;
                waiting_remote ->
                    throw({'precondition violated: all locks must be available locally', Lock})
            end
    end.

update_lock_state(State, Pid, Lock, LockState) ->
    State#state{
        by_pid = maps:update_with(Pid, fun(S) ->
            S#pid_state{
                locks = orddict:update(Lock, LockState, S#pid_state.locks)
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
-spec add_lock_waiting(pid(), integer(), antidote_locks:lock_spec(), antidote_lock_server_state:state()) -> antidote_lock_server_state:state().
add_lock_waiting(Requester, RequestTime, Locks, State) ->
    {RequesterPid, _} = Requester,
    PidState = #pid_state{
        locks = [{Lock, {waiting, Kind}} || {Lock, Kind} <- Locks],
        request_time = RequestTime,
        is_remote = false,
        requester = Requester
    },
    State#state{
        by_pid = maps:put(RequesterPid, PidState, State)
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


% Tries to acquire the given locks for a remote dc.
% Strategy used:
%   When lock is in use: do not send
%   Otherwise: send
% Precondition:
%   When a lock is not in use, no process is waiting for it.
% Returns:
%   HandOff: the locks to be sent to the remote dc
%   RemoteRequests: the locks that have to be requested back because local processes are still waiting for them.
-spec try_acquire_remote_locks(antidote_locks:lock_spec(), integer(), dcid(), pid(), state()) -> {HandOff, RemoteRequests, state()}
    when HandOff :: antidote_locks:lock_spec(), %
    RemoteRequests :: antidote_locks:lock_spec_item().
try_acquire_remote_locks(Locks, Timestamp, DcId, RequesterPid, State) ->
    todo.



-spec check_release_locks(pid(), antidote_locks:lock_spec(), state()) -> ok | {error, Reason :: any()}.
check_release_locks(FromPid, Locks, State) ->
    todo.

% removes the locks for the given pid
-spec remove_locks(pid(), state()) -> {HandOverActions, LockRequestActions, Replies, state()}
    when HandOverActions :: [{antidote_locks:lock_spec(), dcid()}],
    LockRequestActions :: #{dcid() => antidote_locks:lock_spec()},
    Replies :: [requester()].
remove_locks(FromPid, State) ->
    todo.


