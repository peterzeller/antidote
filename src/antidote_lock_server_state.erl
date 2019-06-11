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

-export([initial/0, my_dc_id/1, add_process/5]).

-opaque state() :: #state{}.

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
    {waiting, antidote_locks:lock_kind()}
  | {held, antidote_locks:lock_kind()}.


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
-spec try_acquire_locks(pid(), state()) -> {boolean(), state()}.
try_acquire_locks(Pid, State) ->

    {RequesterPid, _} = Requester,
    case Locks of
        [] ->
            % all locks acquired
            {true, State};
        [{Lock, Kind} | LocksRest] ->
            case Kind of
                shared ->
                    case is_lock_held_exclusively(Lock, State) of
                        true ->
                            % lock currently used exclusively -> wait
                            add_lock_waiting(Requester, Locks, State);
                        error ->
                            % not used exclusively -> acquire this lock
                            State2 = State#state{
                                locks_held_shared = maps:update_with(Lock, fun(L) ->
                                    [RequesterPid | L] end, [], State#state.locks_held_shared)
                            },
                            try_acquire_locks(Requester, LocksRest, State2)
                    end;
                exclusive ->
                    case {maps:find(Lock, State#state.locks_held_exclusively), maps:find(Lock, State#state.locks_held_shared)} of
                        {error, error} ->
                            % lock neither used exclusively nor shared -> acquire this lock
                            State2 = State#state{
                                locks_held_exclusively = maps:put(Lock, RequesterPid, State#state.locks_held_exclusively)
                            },
                            try_acquire_locks(Requester, LocksRest, State2);
                        _ ->
                            % lock currently used -> wait
                            add_lock_waiting(Requester, Locks, State)
                    end
            end
    end.


-spec is_lock_held_exclusively(antidote_locks:lock(), state()) -> boolean().
is_lock_held_exclusively(Lock, State) ->
    case maps:find(Lock, State#state.locks_held_exclusively) of
        {ok, _} -> true;
        error -> false
    end.
