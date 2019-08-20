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

-export_type([state/0, lock_request_actions/0, actions/0]).

-export([
    initial/4,
    my_dc_id/1,
    new_request/6, new_remote_request/3, on_remote_locks_received/5, remove_locks/4, check_release_locks/2, get_snapshot_time/1, set_timer_active/2, get_timer_active/1, retries_for_waiting_remote/5, print_state/1, print_systemtime/1, print_vc/1, print_actions/1, print_lock_request_actions/1, is_lock_process/2, get_remote_waiting_locks/1, next_actions/2]).


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
    requester :: requester()
}).


-type milliseconds() :: integer().

-record(state, {
    %% own datacenter id
    dc_id :: dcid(),
    %% latest snapshot,
    snapshot_time = vectorclock:new() :: snapshot_time(),
    % for each local lock request (requester-pid) the corresponding state
    by_pid = #{} :: #{pid() => #pid_state{}},
    % for each lock: remote data centers who want this lock
    remote_requests = #{} :: lock_request_actions_for_dc(),
    % is the retry timer is active or not?
    timer_Active = false :: boolean(),
    % for each exclusive lock we have: the time when we acquired it
    time_acquired = #{} :: #{antidote_locks:lock() => milliseconds()},
    % for each lock we have: the last time that we used it
    last_used = #{} :: #{antidote_locks:lock() => milliseconds()},
    %%%%%%%%%%%%%%
    % configuration parameters:
    %%%%%%%%%%%%%%
    % minimal time (in ms) for holding an exclusive lock after the last use
    % (this should be enough to start another transaction using the same locks
    %  and thus avoid the need to request the same locks again)
    min_exclusive_lock_duration :: milliseconds(),
    % maximum time for holding a lock after first use
    % if own DC and others request the lock, the own DC is preferred for this duration
    % (higher value increases throughput and latency)
    max_lock_hold_duration :: milliseconds(),
    % artificial request-time delay when remote locks are required
    % (this is to avoid races and to make all DCs give up their locks at about the same time)
    remote_request_delay :: milliseconds()
}).

-opaque state() :: #state{}.



-type requester() :: {pid(), Tag :: term()}.

-type lock_state() :: waiting | held | waiting_remote.

-type lock_state_with_kind() ::
{lock_state(), antidote_locks:lock_kind()}.

% Key: pair consisting of requesting DC and lock
% Value: pair consisting of lock kind and request time
-type lock_request_actions_for_dc() :: #{{dcid(), antidote_locks:lock()} => {MaxKind :: antidote_locks:lock_kind(), MinRequestTime :: milliseconds()}}.

% Key: From which DC should locks be requested
% Value: Which locks to request
-type lock_request_actions() :: #{dcid() => lock_request_actions_for_dc()}.


-record(actions, {
    hand_over = #{} :: #{dcid() => antidote_locks:lock_spec()},
    lock_request = #{} :: lock_request_actions(),
    replies = [] :: [requester()]
}).

-type actions() :: #actions{}.

%-----------------
% Public API:
%-----------------


%% The initial state
-spec initial(dcid(), milliseconds(), milliseconds(), milliseconds()) -> state().
initial(MyDcId, MinExclusiveLockDuration, MaxLockHoldDuration, RemoteRequestDelay) -> #state{
    dc_id = MyDcId,
    min_exclusive_lock_duration = MinExclusiveLockDuration,
    max_lock_hold_duration = MaxLockHoldDuration,
    remote_request_delay = RemoteRequestDelay
}.


%% returns the own data center id
-spec my_dc_id(state()) -> dcid().
my_dc_id(State) ->
    State#state.dc_id.


%% Adds a new request to the state.
%% Requester: The process requesting the lock
%% RequestTime: The current time (in ms)
%% SnapshotTime: The database snapshot time this request is based on
%% AllDcIds: List of all data centers in the system
%% LockEntries: The requested locks and the current CRDT value of these locks
-spec new_request(requester(), milliseconds(), snapshot_time(), [dcid()], ordsets:ordset({antidote_locks:lock_spec_item(), antidote_lock_crdt:value()}), state()) -> {actions(), state()}.
new_request(Requester, RequestTime, SnapshotTime, AllDcIds, LockEntries, State) ->
    {RequesterPid, _} = Requester,
    MyDcId = my_dc_id(State),
    Locks = [L || {L, _} <- LockEntries],
    RequestsByDc = missing_locks_by_dc(AllDcIds, MyDcId, LockEntries),
    % if remote requests are necessary, wait longer
    RequestTime2 = case maps:size(RequestsByDc) of
        0 -> RequestTime;
        _ -> RequestTime + State#state.remote_request_delay
    end,
    % add time and dc to remote requests
    RequestsByDc2 = maps:map(fun(_Dc, V) -> maps:from_list([{{MyDcId, L}, {K, RequestTime2}} || {L, K} <- V]) end, RequestsByDc),
    State1 = add_process(Requester, RequestTime2, Locks, State),
    State2 = set_snapshot_time(SnapshotTime, State1),
    case maps:size(RequestsByDc) of
        0 ->
            % we have all locks locally
            next_actions(State2, RequestTime);
        _ ->
            % tell other data centers that we need locks
            % for shared locks, ask to get own lock back
            % for exclusive locks, ask everyone to give their lock

            WaitingLocks = ordsets:from_list([L || {{_, L}, _} <- lists:flatmap(fun maps:to_list/1, maps:values(RequestsByDc2))]),
            State3 = set_lock_waiting_state(RequesterPid, WaitingLocks, State2, waiting, waiting_remote),
            {Actions, State4} = next_actions(State3, RequestTime),

            Actions2 = merge_actions(#actions{lock_request = RequestsByDc2}, Actions),
            {Actions2, State4}
    end.

%% Adds new remote requests to the state
%% CurrentTime: The current time (in ms)
%% LockRequestActions: The new lock requests
-spec new_remote_request(milliseconds(), lock_request_actions_for_dc(), state()) -> {actions(), state()}.
new_remote_request(CurrentTime, LockRequestActions, State) ->
    NewState = State#state{
        remote_requests = merge_lock_request_actions_for_dc(LockRequestActions, State#state.remote_requests)
    },
    next_actions(NewState, CurrentTime).


%% Called when we receive locks from a remote DC
%% CurrentTime: The current time (in ms)
%% ReadClock: The snapshot time where the remote locks where read
%% AllDcs: List of all data centers
%% LockEntries: new CRDT values of received locks
%%
%% Note: this should usually be called together with new_remote_request
-spec on_remote_locks_received(integer(), snapshot_time(), [dcid()], ordsets:ordset({antidote_locks:lock_spec_item(), antidote_lock_crdt:value()}), state()) -> {actions(), state()}.
on_remote_locks_received(CurrentTime, ReadClock, AllDcs, LockEntries, State) ->
    LockStates = maps:from_list([{L, S} || {{L, _K}, S} <- LockEntries]),
    State2 = update_waiting_remote(AllDcs, LockStates, State),
    State3 = set_snapshot_time(ReadClock, State2),
    next_actions(State3, CurrentTime).

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
-spec remove_locks(integer(), pid(), snapshot_time(), state()) -> {actions(), state()}.
remove_locks(CurrentTime, FromPid, CommitTime, State) ->
    case maps:find(FromPid, State#state.by_pid) of
        error ->
            % Pid entry does not exist -> do nothing
            {#actions{}, State};
        {ok, PidState} ->
            StateWithoutPid = State#state{
                by_pid = maps:remove(FromPid, State#state.by_pid),
                snapshot_time = merge_snapshot_time(CommitTime, State#state.snapshot_time),
                last_used = update_last_used(CurrentTime, PidState, State#state.last_used)
            },
            next_actions(StateWithoutPid, CurrentTime)
    end.

-spec get_snapshot_time(state()) -> snapshot_time().
get_snapshot_time(State) ->
    State#state.snapshot_time.

-spec set_timer_active(boolean(), state()) -> state().
set_timer_active(B, S) -> S#state{timer_Active = B}.

-spec get_timer_active(state()) -> boolean().
get_timer_active(#state{timer_Active = B}) -> B.


%% Determines, if there are any locks that need to be requested again
%% Time: The current time (in ms)
%% RetryDelta: only considers requests that are waiting for more than this amount of ms
%% OtherDcs: List of all other data centers
-spec retries_for_waiting_remote(milliseconds(), milliseconds(), dcid(), [dcid()], state()) -> {boolean(), lock_request_actions()}.
retries_for_waiting_remote(Time, RetryDelta, MyDc, OtherDcs, State) ->
    WaitingRemote = [{L, {K, PidState#pid_state.request_time}} ||
        PidState <- maps:values(State#state.by_pid),
        {L, {waiting_remote, K}} <- PidState#pid_state.locks],
    WaitingRemoteLong = [{{MyDc, L}, {K, T}} || {L, {K, T}} <- WaitingRemote, T + RetryDelta =< Time],
    ByLock = maps:map(fun(_, Ls) -> {max_lock_kind([K || {K, _} <- Ls]), lists:min([T || {_, T} <- Ls])} end, antidote_list_utils:group_by_first(WaitingRemoteLong)),
    LockRequests = maps:from_list([{Dc, ByLock} || ByLock /= #{}, Dc <- OtherDcs]),
    {WaitingRemote /= [], LockRequests}.


%% For debugging: Transforms data into a form that is
%% more readable when printed by Erlang.
-spec print_state(state()) -> any().
print_state(State) ->
    #{
        dc_id => State#state.dc_id,
        snapshot_time => print_vc(State#state.snapshot_time),
        by_pid => maps:map(fun(_K,V) -> print_pid_state(V) end, State#state.by_pid),
        remote_requests => maps:map(fun(_, {K, T}) -> {K, print_systemtime(T)} end, State#state.remote_requests),
        timer_Active => State#state.timer_Active,
        time_acquired => print_map_to_time(State#state.time_acquired),
        last_used => print_map_to_time(State#state.last_used)
    }.

print_map_to_time(M) ->
    maps:map(fun(_, V) -> print_systemtime(V) end, M).

print_pid_state(PidState) ->
    #{
        lock => maps:from_list(PidState#pid_state.locks),
        request_time => print_systemtime(PidState#pid_state.request_time)
    }.

print_vc(undefined) ->
    #{};
print_vc(Vc) ->
    maps:from_list([{print_dc(K), print_systemtime(V)} || {K,V} <- vectorclock:to_list(Vc)]).

print_dc({Dc, _}) when is_atom(Dc) ->
    list_to_atom(lists:sublist(atom_to_list(Dc), 4));
print_dc(Dc) ->
    Dc.


print_systemtime(Ms) when is_integer(Ms) ->
    {{_Year, _Month, _Day}, {Hour, Minute, Second}} = calendar:system_time_to_local_time(Ms, millisecond),
    lists:flatten(io_lib:format("~p:~p:~p.~p", [Hour,Minute, Second, Ms rem 1000]));
print_systemtime(Other) -> Other.


-spec print_actions(actions()) -> any().
print_actions(#actions{hand_over = HandOverActions, lock_request = LockRequestActions, replies =  Replies}) ->
    H = [#{' handover_lock' => LK, to => print_dc(Dc)} || {Dc, LK} <- maps:to_list(HandOverActions)],
    L = print_lock_request_actions(LockRequestActions),
    R = [#{reply_to => R} || R <- Replies],
    H ++ L ++ R.

-spec print_lock_request_actions(lock_request_actions()) -> any().
print_lock_request_actions(LockRequestActions) ->
    try [#{' lock_request_to' => print_dc(Dc), for_dc => print_dc(RDc), lock => L, kind => K, time => print_systemtime(T)} ||
        {Dc, As} <- maps:to_list(LockRequestActions),
        {{RDc, L}, {K, T}} <- maps:to_list(As)]
    catch
        A:B:T ->
            [{'ERROR in print_lock_request_actions', LockRequestActions, {A, B, T}}]
    end.

%% Checks if the given PID is waiting for a lock
-spec is_lock_process(pid(), state()) -> boolean().
is_lock_process(Pid, State) ->
    maps:is_key(Pid, State#state.by_pid).

%% Internal functions


%% Adds a new process to the state.
%% The process is initially in the waiting state for all locks.
-spec add_process(requester(), integer(), antidote_locks:lock_spec(), state()) -> state().
add_process(Requester, RequestTime, Locks, State) ->
    {Pid, _} = Requester,
    State#state{
        by_pid = maps:put(Pid, #pid_state{
            locks = [{Lock, {waiting, Kind}} || {Lock, Kind} <- Locks],
            request_time = RequestTime,
            requester = Requester
        }, State#state.by_pid)
    }.

-spec merge_lock_request_actions_for_dc(lock_request_actions_for_dc(), lock_request_actions_for_dc()) -> lock_request_actions_for_dc().
merge_lock_request_actions_for_dc(A1, A2) ->
    Merge = fun(Key, {K, T}, M) ->
        MergeValues = fun({K2, T2}) ->
            {max_lock_kind(K2, K), min(T2, T)}
        end,
        maps:update_with(Key, MergeValues, {K, T}, M)
    end,
    maps:fold(Merge, A2, A1).



%% Tries to acquire the locks for the given Pid
%% Res is true iff all locks were newly acquired.
%% Precondition: all required locks are available locally
-spec try_acquire_locks(integer(), pid(), state()) -> {boolean(), state()}.
try_acquire_locks(CurrentTime, Pid, State) ->
    case maps:find(Pid, State#state.by_pid) of
        {ok, PidState} ->
            LockStates = PidState#pid_state.locks,
            try_acquire_lock_list(CurrentTime, Pid, LockStates, PidState#pid_state.request_time, false, State);
        error ->
            {false, State}
    end.

-spec try_acquire_lock_list(integer(), pid(), orddict:orddict(antidote_locks:lock(), lock_state_with_kind()), integer(), boolean(), state()) -> {boolean(), state()}.
try_acquire_lock_list(CurrentTime, Pid, LockStates, RequestTime, Changed, State) ->
    case LockStates of
        [] -> {Changed, State};
        [{Lock, {LockState, Kind}}| LockStatesRest] ->
            case LockState of
                held ->
                    try_acquire_lock_list(CurrentTime, Pid, LockStatesRest, RequestTime, Changed, State);
                waiting ->
                    LS = lock_held_state(Lock, State),
                    CanAcquireLock =
                        (LS == none orelse (Kind == shared andalso LS == shared)),
                    case CanAcquireLock of
                        true ->
                            % acquire lock
                            NewState = update_lock_state(State, Pid, Lock, {held, Kind}),
                            try_acquire_lock_list(CurrentTime, Pid, LockStatesRest, RequestTime, true, NewState);
                        false ->
                            {false, State}
                    end;
                waiting_remote ->
                    {false, State}
            end
    end.

%%% checks if there is a local request for an exclusive lock in state waiting_remote with
%%% a request time < than the given request time
%%-spec is_waiting_remote_exclusive(antidote_locks:lock(), integer(), dcid(), state()) -> boolean().
%%is_waiting_remote_exclusive(Lock, OtherRequestTime, OtherDc, State) ->
%%    lists:any(fun(PidState) ->
%%        % adding dc-id in comparison to resolve system-time collisions
%%        {PidState#pid_state.request_time, State#state.dc_id} < {OtherRequestTime, OtherDc}
%%            andalso orddict:find(Lock, PidState#pid_state.locks) == {ok, {waiting_remote, exclusive}}
%%    end, maps:values(State#state.by_pid)).

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
            antidote_list_utils:group_by_first(Missing)
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
    Res.








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
-spec next_actions(state(), integer()) -> {actions(), state()}.
next_actions(State, CurrentTime) ->

    %--logger:notice("next_actions ~p~n ~p", [print_systemtime(CurrentTime), print_state(State)]),

    % First try to serve remote requests:
    {Actions, State2} = handle_remote_requests(State, CurrentTime),

    %--logger:notice("handle_remote_requests actions~n ~p", [print_actions(Actions)]),


    % sort processes by request time, so that the processes waiting the longest will
    % acquire their locks first
    Pids = [Pid || {Pid, _} <- lists:sort(fun compare_by_request_time/2, maps:to_list(State#state.by_pid))],

    % try to acquire locks for local requests
    {Actions2, State3} = lists:foldl(fun(Pid, {AccActions, S}) ->
        {AllAcquired, S2} = try_acquire_locks(CurrentTime, Pid, S),
        case AllAcquired of
            true ->
                PidState = maps:get(Pid, S#state.by_pid),
                NewReplies = [PidState#pid_state.requester | AccActions#actions.replies],
                S3 = set_lock_acquired_time(CurrentTime, PidState#pid_state.locks, S2),
                {AccActions#actions{replies = NewReplies}, S3};
            false ->
                {AccActions, S2}
        end
    end, {Actions, State2}, Pids),

%%    case print_actions(Actions2) of
%%        [] when State3 == State ->
%%            ok;
%%        PrintedActions ->
%%            logger:notice("next_actions ~n PreState  = ~p~n PostState = ~p~n Actions = ~p", [print_state(State), print_state(State3), PrintedActions])
%%    end,

    {Actions2, State3}.

-spec handle_remote_requests(state(), milliseconds()) -> {actions(), state()}.
handle_remote_requests(State, CurrentTime) ->


    % Select the locks which are eligible to be sent to other DC:
    % type: lock_request_actions_for_dc()
    LocksToTransfer = maps:filter(fun({Dc, L}, {K, T}) ->
        % request must not be from the future
        T =< CurrentTime
            andalso
            (
                % last use is some time ago
                maps:get(L, State#state.last_used, 0) =< CurrentTime - State#state.min_exclusive_lock_duration
                    orelse
                    % lock was held for a longer period of time already
                maps:get(L, State#state.time_acquired, 0) =< CurrentTime - State#state.max_lock_hold_duration
            )
        % there is no conflicting local request with a smaller time
            andalso not exists_local_request(L, T, K, State)
        % there is no conflicting remote request with a smaller time
            andalso not exists_conflicting_remote_request(Dc, L, K, T, State)
    end, State#state.remote_requests),


    LocksToTransferL = maps:to_list(LocksToTransfer),
    LocksWithKind = [{L,K} || {{_Dc, L}, {K, _T}} <- LocksToTransferL],
    Locks = [L || {{_Dc, L}, {_K, _T}} <- LocksToTransferL],

    % remove sent locks (shared if shared, all if exclusive)
    NewRemoteLocks = remove_sent_locks_from_remote_requests(LocksWithKind, State),

    RequestAgainRemote = remote_lock_requests_for_sent_locks(LocksToTransferL, State),

    % check local requests and get the locks that must be requested again
    RequestAgainLocal = get_local_waiting_requests(LocksToTransferL, State),

    % change local requests to waiting_remote
    State2 = change_waiting_locks_to_remote(Locks, State),

    State3 = State2#state{
        remote_requests = NewRemoteLocks
    },

    HandoverActions = maps:map(
        fun(_, L) -> ordsets:from_list(L) end,
        antidote_list_utils:group_by_first([{Dc, {L,K}} || {{Dc, L}, {K, _T}} <- LocksToTransferL])),

    Actions = #actions{
        hand_over =  HandoverActions,
        lock_request = merge_lock_request_actions(RequestAgainLocal, RequestAgainRemote)
    },

    {Actions, State3}.

exists_conflicting_remote_request(Dc, L, K, T, State) ->
    lists:any(fun({{Dc2, L2}, {K2, T2}}) ->
        L == L2
            andalso Dc /= Dc2
            andalso not (K == shared andalso K2 == shared)
            andalso {T2, Dc2} =< {T, Dc}
    end, maps:to_list(State#state.remote_requests)).



%% Removes all the transferred locks and all conflicting locks from the
%% set of remote requests and collects them in a list to be sent to the remote DC.
-spec remove_sent_locks_from_remote_requests([{antidote_locks:lock(), antidote_locks:lock_kind()}], state()) -> lock_request_actions_for_dc().
remove_sent_locks_from_remote_requests(LocksWithKind, State) ->
    maps:filter(fun({_Dc, L}, {K, _T}) ->
        lists:all(fun({L2, K2}) ->
            L /= L2 orelse (K == shared andalso K2 == shared)
        end, LocksWithKind)
    end, State#state.remote_requests).

-spec remote_lock_requests_for_sent_locks([{{dcid(), antidote_locks:lock()}, {antidote_locks:lock_kind(), milliseconds()}}], state()) -> lock_request_actions().
remote_lock_requests_for_sent_locks(LocksToTransferL, State) ->
    List = lists:flatmap(fun({{Dc, L}, {K, T}}) ->
        lists:flatmap(fun({{Dc2, L2}, {K2, _T2}}) ->
            if
                L /= L2 -> [];
                Dc == Dc2 -> [];
                K == shared andalso K2 == shared -> [];
                true -> [{Dc2, {{Dc, L}, {K, T}}}]
            end
        end, LocksToTransferL)
    end, maps:to_list(State#state.remote_requests)),
    list_to_lock_request_actions(List).

% checks if there is a conflicting local request for the given lock
% with time <= T or already held
-spec exists_local_request(antidote_locks:lock(), milliseconds(), antidote_locks:lock_kind(), state()) -> boolean().
exists_local_request(Lock, T, Kind, State) ->
    lists:any(
        fun(S) ->
            TimeBefore = S#pid_state.request_time =< T,
            case orddict:find(Lock, S#pid_state.locks) of
                {ok, {St, shared}} -> Kind == exclusive andalso (St == held orelse TimeBefore);
                {ok, {St, exclusive}} -> St == held orelse TimeBefore;
                error -> false
            end
        end,
        maps:values(State#state.by_pid)
    ).




-spec set_lock_acquired_time(milliseconds(), orddict:orddict(antidote_locks:lock(), lock_state_with_kind()), state()) -> state().
set_lock_acquired_time(CurrentTime, PidStatePidStateLocks, State) ->
    EL = orddict:fold(
        fun
            (Key, {_, exclusive}, M) ->
                maps:update_with(Key, fun(X) -> min(X, CurrentTime) end, CurrentTime, M);
            (_, _, M) -> M
        end, State#state.time_acquired, PidStatePidStateLocks),
    State#state{time_acquired = EL}.

-spec update_last_used(milliseconds(), #pid_state{}, #{antidote_locks:lock() => milliseconds()}) -> #{antidote_locks:lock() => milliseconds()}.
update_last_used(CurrentTime, PidState, LastUsed) ->
    orddict:fold(fun(L, _, Acc) ->
        maps:put(L, CurrentTime, Acc)
    end, LastUsed, PidState#pid_state.locks).

%%-spec remove_lock_acquired_time(orddict:orddict(antidote_locks:lock(), lock_state_with_kind()), state()) -> state().
%%remove_lock_acquired_time(PidStatePidStateLocks, State) ->
%%    EL = orddict:fold(
%%        fun(Key, _, M) ->
%%            maps:remove(Key, M)
%%        end, State#state.exclusive_locks, PidStatePidStateLocks),
%%    State#state{exclusive_locks = EL}.

-spec get_remote_waiting_locks(state()) -> ordsets:ordset(antidote_locks:lock()).
get_remote_waiting_locks(State) ->
    PidStates = maps:values(State#state.by_pid),
    Locks = lists:flatmap(fun(P) -> [L || {L, {waiting_remote, _}} <- P#pid_state.locks] end, PidStates),
    ordsets:from_list(Locks).


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


% computes lock request actions for the locally waiting locks
% assuming we send the given locks to other data centers
-spec get_local_waiting_requests([{{dcid(), antidote_locks:lock()}, {antidote_locks:lock_kind(), milliseconds()}}], state())
        -> lock_request_actions().
get_local_waiting_requests(LocksToTransferL, State) ->
    List = lists:flatmap(fun({{Dc, Lock}, {LockKind, _T}}) ->
        lists:filtermap(fun(PS) ->
            case orddict:find(Lock, PS#pid_state.locks) of
                {ok, {_S, K}} when not (LockKind == shared andalso K == shared) ->
                    {true, {Dc, {{State#state.dc_id, Lock}, {K, PS#pid_state.request_time}}}};
                _ ->
                    false
            end
        end, maps:values(State#state.by_pid))
    end, LocksToTransferL),
    list_to_lock_request_actions(List).

-spec list_to_lock_request_actions([{dcid(), {{dcid(), antidote_locks:lock()}, {antidote_locks:lock_kind(), milliseconds()}}}]) -> lock_request_actions().
list_to_lock_request_actions(List) ->
    ByDc = antidote_list_utils:group_by_first(List),
    maps:map(fun(_, IList) ->
        maps:map(fun(_, L) ->
            antidote_list_utils:reduce(fun({K1, T1}, {K2, T2}) -> {max_lock_kind(K1, K2), min(T1, T2)} end, L)
        end, antidote_list_utils:group_by_first(IList))
    end, ByDc).


-spec merge_lock_request_actions(lock_request_actions(), lock_request_actions()) -> lock_request_actions().
merge_lock_request_actions(A, B) ->
    maps:fold(fun
        (_, [], Acc) ->
            Acc;
        (K, V1, Acc) ->
            maps:update_with(K, fun(V2) -> merge_lock_request_actions_for_dc(V1, V2) end, V1, Acc)
    end, B, A).




-spec merge_handover_actions(#{dcid() => antidote_locks:lock_spec()}, #{dcid() => antidote_locks:lock_spec()}) -> #{dcid() => antidote_locks:lock_spec()}.
merge_handover_actions(H1, H2) ->
    maps:fold(fun(Dc, LS1, Acc) ->
        maps:update_with(Dc, fun(LS2) -> ordsets:union(LS1, LS2) end, [], Acc)
    end, H1, H2).

-spec merge_actions(actions(), actions()) -> actions().
merge_actions(A1, A2) ->
    #actions{
        hand_over = merge_handover_actions(A1#actions.hand_over, A2#actions.hand_over),
        lock_request = merge_lock_request_actions(A1#actions.lock_request, A2#actions.lock_request),
        replies = A1#actions.replies ++ A2#actions.replies
    }.

%
%%-spec filter_waited_for_locks([antidote_locks:lock()], state()) -> lock_request_actions_for_dc().
%%filter_waited_for_locks(Locks, State) ->
%%    [{L, {Kind, MinTime}} ||
%%        L <- Locks,
%%        (Kind = max_lock_waiting_kind(L, State)) /= none,
%%        (MinTime = min_lock_request_time(L, State)) < infty].

%%% returns the minimum time of a local request waiting for this lock
%%-spec min_lock_request_time(antidote_locks:lock(), state()) -> integer() | infty.
%%min_lock_request_time(Lock, State) ->
%%    lists:min([infty] ++ [PidState#pid_state.request_time ||
%%        PidState <- maps:values(State#state.by_pid),
%%        {L, {waiting, _}} <- PidState#pid_state.locks,
%%        L == Lock]).


%%% goes through all the processes waiting for the given lock and returns the maximum required lock level
%%-spec max_lock_waiting_kind(antidote_locks:lock(), state()) -> shared | exclusive | none.
%%max_lock_waiting_kind(Lock, State) ->
%%    max_lock_waiting_kind_iter(Lock, maps:iterator(State#state.by_pid), none).

%%max_lock_waiting_kind_iter(Lock, Iter, Res) ->
%%    case maps:next(Iter) of
%%        none -> Res;
%%        {_Pid, PidState, NextIterator} ->
%%            case orddict:find(Lock, PidState#pid_state.locks) of
%%                {ok, {waiting, LockKind}} ->
%%                    % we can return the first entry we find, since the two kinds exclude each other
%%                    max_lock_waiting_kind_iter(Lock, NextIterator, max_lock_kind(LockKind, Res));
%%                _ ->
%%                    max_lock_waiting_kind_iter(Lock, NextIterator, Res)
%%            end
%%    end.


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

initial(Dc) ->
    initial(Dc, 0, 0, 1).

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
    ?assertEqual(#{}, Actions#actions.hand_over),
    ?assertEqual(#{}, Actions#actions.lock_request),
    ?assertEqual([R1], Actions#actions.replies),
    ok.

shared_lock_fail_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    % data center 3 has our lock, so we need to request it
    {Actions, _S2} = new_request(R1, 10, undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc2, dc3 => dc3}}], S1),
    ?assertEqual(#{}, Actions#actions.hand_over),
    ?assertEqual(#{dc3 => #{{dc1, lock1} => {shared, 11}}}, Actions#actions.lock_request),
    ?assertEqual([], Actions#actions.replies),
    ok.

shared_lock_fail2_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    % data center 3 has our lock, so we need to request it
    {Actions, _S2} = new_request(R1, 10, undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc3}}], S1),
    ?assertEqual(#{}, Actions#actions.hand_over),
    ?assertEqual(#{dc3 => #{{dc1, lock1} => {shared, 11}}}, Actions#actions.lock_request),
    ?assertEqual([], Actions#actions.replies),
    ok.


shared_lock_missing_local_test() ->
    SInit = initial(dc1),
    R1 = {p1, t1},
    % data center 3 has our lock, so we need to request it
    {Actions1, S1} = new_request(R1, 10, undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc3}}], SInit),
    ?assertEqual(#actions{lock_request =  #{dc3 => #{{dc1, lock1} => {shared, 11}}}}, Actions1),

    % later we receive the lock from dc3 and we can get the lock
    {Actions2, _S2} = on_remote_locks_received(99, time1, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], S1),
    ?assertEqual(#actions{replies = [R1]}, Actions2),
    ok.

exclusive_lock_missing_local_test() ->
    SInit = initial(dc1),
    R1 = {p1, t1},
    % data centers 2 and 3 have the locks, so we need to request it
    {Actions1, S1} = new_request(R1, 10, undefined, [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc3, dc2 => dc2, dc3 => dc2}}], SInit),
    ?assertEqual(#actions{lock_request = #{dc2 => #{{dc1, lock1} => {exclusive, 11}}, dc3 => #{{dc1, lock1} => {exclusive, 11}}}}, Actions1),

    % later we receive the lock from dc2, which is not enough to acquire the lock
    {Actions2, S2} = on_remote_locks_received(99, undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual(#actions{}, Actions2),

    % when we have all the locks, we can acquire the lock
    {Actions3, _S3} = on_remote_locks_received(99, undefined, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S2),
    ?assertEqual(#actions{replies = [R1]}, Actions3),
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
    ?assertEqual(#actions{lock_request = #{
        dc2 => #{{dc1, lock1} => {exclusive, 11}, {dc1, lock2} => {exclusive, 11}},
        dc3 => #{{dc1, lock1} => {exclusive, 11}, {dc1, lock2} => {exclusive, 11}}}}, Actions1),
    ?assertEqual([lock1, lock2], get_remote_waiting_locks(S1)),

    % first we only get lock 2
    {Actions2, S2} = on_remote_locks_received(99, undefined, [dc1, dc2, dc3], [{{lock2, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual(#actions{}, Actions2),
    ?assertEqual([lock1], get_remote_waiting_locks(S2)),

    % when we have all the locks, we can acquire the lock
    ReceivedLocks = [
        {{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}},
        {{lock2, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}],
    {Actions3, _S3} = on_remote_locks_received(99, undefined, [dc1, dc2, dc3], ReceivedLocks, S2),
    ?assertEqual(#actions{replies = [R1]}, Actions3),
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
    ?assertEqual(#actions{lock_request = #{dc2 => #{{dc1, lock2} => {shared, 11}}}}, Actions1),

    % later we receive the lock from dc2 and we can get the lock
    {Actions2, _S2} = on_remote_locks_received(99, undefined, [dc1, dc2, dc3], [{{lock2, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], S1),
    ?assertEqual(#actions{replies = [R1]}, Actions2),
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
    ?assertEqual(#actions{lock_request = #{dc2 => #{{dc1, lock1} => {exclusive, 11}}, dc3 => #{{dc1, lock1} => {exclusive, 11}}}}, Actions1),

    ?assertEqual(VC1, get_snapshot_time(S1)),

    % later we receive the lock from dc2, which is not enough to acquire the lock
    {Actions2, S2} = on_remote_locks_received(99, VC2, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc3, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual(#actions{}, Actions2),

    ?assertEqual(VC2, get_snapshot_time(S2)),

    % when we have all the locks, we can acquire the lock
    {Actions3, S3} = on_remote_locks_received(99, VC3, [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S2),
    ?assertEqual(#actions{replies = [R1]}, Actions3),

    ?assertEqual(VC3, get_snapshot_time(S3)),
    ok.

exclusive_lock_ok_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    {Actions, _S2} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual(#actions{replies = [R1]}, Actions),
    ok.


exclusive_lock_fail_test() ->
    S1 = initial(dc1),
    R1 = {p1, t1},
    {Actions, _S2} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc2}}], S1),
    ?assertEqual(#{}, Actions#actions.hand_over),
    ?assertEqual(#{dc2 => #{{dc1, lock1} => {exclusive, 11}}}, Actions#actions.lock_request),
    ?assertEqual([], Actions#actions.replies),
    ok.


two_shared_locks_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}}], SInit),
    ?assertEqual(#actions{replies = [R1]}, Actions1),

    % then R2 tries to acquire the same lock
    R2 = {p2, t2},
    {Actions2, _S2} = new_request(R2, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}}], S1),
    ?assertEqual(#actions{replies = [R2]}, Actions2),

    ok.

two_exclusive_locks_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], SInit),
    ?assertEqual(#{}, Actions1#actions.hand_over),
    ?assertEqual(#{}, Actions1#actions.lock_request),
    ?assertEqual([R1], Actions1#actions.replies),

    % then R2 tries to acquire the same lock, does not get it
    R2 = {p2, t2},
    {Actions2, S2} = new_request(R2, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual(#actions{}, Actions2),

    % when R1 releases it's lock, R2 gets it
    ?assertEqual(ok, check_release_locks(p1, S2)),
    {Actions3, S3} = remove_locks(99, p1, vectorclock:new(), S2),
    ?assertEqual(#actions{replies = [R2]}, Actions3),

    % cannot release locks a second time
    ?assertMatch({error, _}, check_release_locks(p1, S3)),

    ?assertEqual({#actions{}, S3}, remove_locks(99, p1, vectorclock:new(), S3)),
    ok.

shared_lock_with_remote_shared_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], SInit),
    ?assertEqual(#actions{replies = [R1]}, Actions1),

    % then remote DC2 tries to acquire the same lock
    {Actions2, _S2} = new_remote_request(99, #{{dc2, lock1} => {shared, 20}}, S1),
    ?assertEqual(#actions{hand_over = #{dc2 => [{lock1,shared}]}}, Actions2),
    ok.

shared_lock_with_remote_exclusive_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], SInit),
    ?assertEqual(#actions{replies = [R1]}, Actions1),

    % then remote DC2 tries to acquire the same lock
    {Actions2, S2} = new_remote_request(99, #{{dc2, lock1} => {exclusive, 20}}, S1),
    ?assertEqual(#actions{}, Actions2),

    % when R1 releases it's lock, the remote gets it
    {Actions3, _S3} = remove_locks(99, p1, vectorclock:new(), S2),
    ?assertEqual(#actions{hand_over = #{dc2 => [{lock1,exclusive}]}}, Actions3),
    ok.

shared_lock_with_remote_exclusive_duplicate_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], SInit),
    ?assertEqual(#actions{replies = [R1]}, Actions1),

    % then remote DC2 tries to acquire the same lock
    {Actions2, S2} = new_remote_request(99, #{{dc2, lock1} => {exclusive, 20}}, S1),
    ?assertEqual(#actions{}, Actions2),

    % and (because of a timeout) it tries to acquire the lock once more
    {Actions3, S3} = new_remote_request(99, #{{dc2, lock1} => {exclusive, 20}}, S2),
    ?assertEqual(#actions{}, Actions3),

    % when R1 releases it's lock, the remote gets it
    {Actions4, _S4} = remove_locks(99, p1, vectorclock:new(), S3),
    ?assertEqual(#actions{hand_over = #{dc2 => [{lock1,exclusive}]}}, Actions4),
    ok.


same_time_remote_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, shared}, #{dc1 => dc1, dc2 => dc1, dc3 => dc3}}], SInit),
    ?assertEqual(#actions{replies = [R1]}, Actions1),

    % then remote DC2 tries to acquire the same lock
    {Actions2, S2} = new_remote_request(99, #{{dc2, lock1} => {exclusive, 20}}, S1),
    ?assertEqual(#actions{}, Actions2),

    % and DC3 tries with the same timestamp
    {Actions3, S3} = new_remote_request(99, #{{dc3, lock1} => {exclusive, 20}}, S2),
    ?assertEqual(#actions{}, Actions3),

    % when R1 releases it's lock, the remote dc2 gets it
    {Actions4, _S4} = remove_locks(99, p1, vectorclock:new(), S3),
    ?assertEqual(#actions{
        hand_over = #{dc2 => [{lock1,exclusive}]},
        lock_request = #{dc2 => #{{dc3,lock1} => {exclusive,20}}}
    }, Actions4),
    ok.


exclusive_lock_with_remote_shared_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], SInit),
    ?assertEqual(#actions{replies = [R1]}, Actions1),

    % then remote DC2 tries to acquire the same lock
    {Actions2, S2} = new_remote_request(99,  #{{dc2, lock1} => {shared, 20}}, S1),
    ?assertEqual(#actions{}, Actions2),

    % when R1 releases it's lock, the remote gets it
    {Actions3, _S3} = remove_locks(99, p1, vectorclock:new(), S2),
    ?assertEqual(#actions{hand_over = #{dc2 => [{lock1,shared}]}}, Actions3),
    ok.


exclusive_lock_future_test() ->
    SInit = initial(dc1),

    % remote DC2 tries to acquire the lock
    {Actions2, S2} = new_remote_request(1, #{{dc2, lock1} => {shared, 20}}, SInit),
    ?assertEqual(#actions{}, Actions2),

    % remote DC3 tries to acquire the lock
    {Actions3, S3} = new_remote_request(2, #{{dc3, lock1} => {shared, 19}}, S2),
    ?assertEqual(#actions{}, Actions3),

    % when some time passes, the lock is sent to both DCs (possible since it is shared)
    {Actions4, _S4} = next_actions(S3, 99),
    ?assertEqual(#actions{ hand_over = #{dc2 => [{lock1,shared}],dc3 => [{lock1,shared}]}}, Actions4),
    ok.

exclusive_lock_with_remote_exclusive_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], SInit),
    ?assertEqual(#actions{replies = [R1]}, Actions1),

    % then remote DC2 tries to acquire the same lock
    {Actions2, S2} = new_remote_request(99, #{{dc2, lock1} => {exclusive, 20}}, S1),
    ?assertEqual(#actions{}, Actions2),

    % when R1 releases it's lock, the remote gets it
    {Actions3, _S3} = remove_locks(99, p1, vectorclock:new(), S2),
    ?assertEqual(#actions{hand_over = #{dc2 => [{lock1,exclusive}]}}, Actions3),
    ok.


exclusive_lock_with_remote_exclusive_later_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1, waiting on remote
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}}], SInit),
    ?assertEqual(#actions{lock_request = #{dc2 => #{{dc1, lock1} => {exclusive, 11}}, dc3 => #{{dc1, lock1} => {exclusive, 11}}}}, Actions1),

    % then remote DC2 tries to acquire the same lock
    {Actions2, _S2} = new_remote_request(99, #{{dc2, lock1} => {exclusive, 20}}, S1),
    % since the remote request has a later timestamp, we do not pass on the lock:
    ?assertEqual(#actions{}, Actions2),

    ok.

exclusive_lock_with_remote_exclusive_earlier_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1, waiting on remote
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 30, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc2, dc3 => dc3}}], SInit),
    ?assertEqual(#actions{lock_request = #{dc2 => #{{dc1, lock1} => {exclusive, 31}}, dc3 => #{{dc1, lock1} => {exclusive, 31}}}}, Actions1),

    % then remote DC2 tries to acquire the same lock
    {Actions2, _S2} = new_remote_request(99, #{{dc2, lock1} => {exclusive, 20}}, S1),
    % since the remote request has an earlier timestamp, we send our locks to DC2
    ?assertEqual(#actions{hand_over = #{dc2 => [{lock1,exclusive}]}, lock_request = #{dc2 => #{{dc1,lock1} => {exclusive,31}}}}, Actions2),

    ok.

locks_request_again_test() ->
    SInit = initial(dc1),
    % first R1 tries to acquire lock1
    R1 = {p1, t1},
    {Actions1, S1} = new_request(R1, 10, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], SInit),
    ?assertEqual(#actions{replies = [R1]}, Actions1),

    % then R2 tries to acquire the same lock
    R2 = {p2, t2},
    {Actions2, S2} = new_request(R2, 30, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S1),
    ?assertEqual(#actions{}, Actions2),

    % then a remote request for the lock comes in with an earlier timestamp
    {Actions3, S3} = new_remote_request(99, #{{dc2, lock1} => {exclusive, 20}}, S2),
    ?assertEqual(#actions{}, Actions3),

    % when R1 releases it's lock, the remote gets it, but we also want it back because R2 needs it
    {Actions4, S4} = remove_locks(99, p1, vectorclock:new(), S3),
    ?assertEqual(#actions{hand_over = #{dc2 => [{lock1,exclusive}]}, lock_request = #{dc2 => #{{dc1,lock1} => {exclusive,30}}}}, Actions4),

    % later we get it back from dc2 and R2 can get the lock
    {Actions5, _S5} = on_remote_locks_received(99, vectorclock:new(), [dc1, dc2, dc3], [{{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}], S4),
    ?assertEqual(#actions{replies = [R2]}, Actions5),
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
    ?assertEqual(#actions{lock_request =
        #{
            dc2 => #{{dc1, lock2} => {exclusive,11},{dc1, lock3} => {shared,11}},
            dc3 => #{{dc1, lock1} => {shared,11},{dc1,lock2} => {exclusive,11}}
        }},
        Actions1),


    R2 = {p2, t2},
    RLocks2 = [
        {{lock1, exclusive}, #{dc1 => dc3, dc2 => dc2, dc3 => dc3}},
        {{lock4, shared}, #{dc1 => dc2, dc2 => dc2, dc3 => dc3}}
    ],
    {Actions2, S2} = new_request(R2, 20, undefined, [dc1, dc2, dc3], RLocks2, S1),
    ?assertEqual(#actions{lock_request = #{
        dc2 => #{{dc1, lock1} => {exclusive,21}, {dc1, lock4} => {shared,21}},
        dc3 => #{{dc1, lock1} => {exclusive,21}}}}, Actions2),

    % request again later:
    {StillWaiting3, Actions3} = retries_for_waiting_remote(25, 12, dc1, [dc2, dc3], S2),
    Missing3 = #{{dc1, lock1} => {shared, 11}, {dc1, lock2} => {exclusive, 11}, {dc1, lock3} => {shared, 11}},
    ?assertEqual(#{dc2 => Missing3, dc3 => Missing3}, Actions3),
    ?assertEqual(true, StillWaiting3),

    % request again later:
    {StillWaiting4, Actions4} = retries_for_waiting_remote(35, 10, dc1, [dc2, dc3], S2),
    Missing4 = #{{dc1, lock1} => {exclusive, 11}, {dc1, lock2} => {exclusive, 11}, {dc1, lock3} => {shared, 11}, {dc1, lock4} => {shared, 21}},
    ?assertEqual(#{dc2 => Missing4, dc3 => Missing4}, Actions4),
    ?assertEqual(true, StillWaiting4),
    ok.




min_max_time_test() ->
    SInit = initial(dc1, 30, 200, 1),
    R1 = {p1, t1},
    % Assume we have exclusive access -> we can immediately get the lock
    RLocks1 = [
        {{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}
    ],
    {Actions1, S1} = new_request(R1, 10, undefined, [dc1, dc2, dc3], RLocks1, SInit),
    ?assertEqual(#actions{replies = [R1]}, Actions1),

    % we finish the transaction and release the lock
    {Actions2, S2} = remove_locks(11, p1, #{}, S1),
    ?assertEqual(#actions{}, Actions2),

    % Now dc2 also wants the lock, but does not get it because of the minimum wait time
    {Actions3, S3} = new_remote_request(12, #{{dc2, lock1} => {exclusive, 10}}, S2),
    ?assertEqual(#actions{}, Actions3),

    % we can continue to use the lock on dc1 for 200ms if we have less than 30ms between requests
    S4 = lists:foldl(fun(Time, AccS) ->
        R = {{p,Time}, {t, Time}},
        {AccActions1, AccS2} = new_request(R, Time, undefined, [dc1, dc2, dc3], RLocks1, AccS),
        ?assertEqual(#actions{replies = [R]}, AccActions1),

        % we finish the transaction and release the lock
        {AccActions2, AccS3} = remove_locks(Time+1, {p, Time}, #{}, AccS2),
        ?assertEqual(#actions{}, AccActions2),
        AccS3

    end, S3, [30, 50, 75, 100, 125, 150, 175, 205]),

    % however, after 200ms, we have to pass the lock:
    R2 = {p2, t2},
    {Actions5, _S5} = new_request(R2, 215, undefined, [dc1, dc2, dc3], RLocks1, S4),
    ?assertEqual(#actions{
        hand_over = #{dc2 => [{lock1,exclusive}]},
        lock_request = #{dc2 => #{{dc1,lock1} => {exclusive,215}}}
    }, Actions5),

    ok.



min_max_time_interrupt_test() ->
    SInit = initial(dc1, 30, 200, 1),
    R1 = {p1, t1},
    % Assume we have exclusive access -> we can immediately get the lock
    RLocks1 = [
        {{lock1, exclusive}, #{dc1 => dc1, dc2 => dc1, dc3 => dc1}}
    ],
    {Actions1, S1} = new_request(R1, 10, undefined, [dc1, dc2, dc3], RLocks1, SInit),
    ?assertEqual(#actions{replies = [R1]}, Actions1),

    % we finish the transaction and release the lock
    {Actions2, S2} = remove_locks(11, p1, #{}, S1),
    ?assertEqual(#actions{}, Actions2),

    % Now dc2 also wants the lock, but does not get it because of the minimum wait time
    {Actions3, S3} = new_remote_request(12, #{{dc2, lock1} => {exclusive, 10}}, S2),
    ?assertEqual(#actions{}, Actions3),

    % we can continue to use the lock on dc1 for 200ms if we have less than 30ms between requests
    S4 = lists:foldl(fun(Time, AccS) ->
        R = {{p,Time}, {t, Time}},
        {AccActions1, AccS2} = new_request(R, Time, undefined, [dc1, dc2, dc3], RLocks1, AccS),
        ?assertEqual(#actions{replies = [R]}, AccActions1),

        % we finish the transaction and release the lock
        {AccActions2, AccS3} = remove_locks(Time+1, {p, Time}, #{}, AccS2),
        ?assertEqual(#actions{}, AccActions2),
        AccS3

    end, S3, [30, 50, 75, 100]),

    % however, if we take more than 30ms we have to pass the lock:
    R2 = {p2, t2},
    {Actions5, _S5} = new_request(R2, 135, undefined, [dc1, dc2, dc3], RLocks1, S4),
    ?assertEqual(#actions{
        hand_over = #{dc2 => [{lock1,exclusive}]},
        lock_request = #{dc2 => #{{dc1,lock1} => {exclusive,135}}}
    }, Actions5),

    ok.

-endif.
