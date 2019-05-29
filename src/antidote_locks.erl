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

%% @doc This module handles obtaining locks before a transaction
%% and releasing locks after a transaction
-module(antidote_locks).
%%
-include("antidote.hrl").

%% API
-export([obtain_locks/3, release_locks/3]).

-type lock_spec() :: #{
    shared_locks => list(binary()),
    exclusive_locks => list(binary())
}.

%% @doc tries to obtain the given locks
%% Proc: The process that will work with the locks.
%%       We assume that the locks are only needed as long as this process is alive.
%%       However, there can be multiple processes at the same time using the locks.
%% ClientClock: The minimum time for which locks are requested
%% Locks: The locks to aquire
%% Returns:
%%  {ok, SnapshotTime}:
%%      In this case it is guaranteed that the locks are acquired and
%%      protect the state since SnapshotTime.
-spec obtain_locks(pid(), snapshot_time(), lock_spec()) -> {ok, snapshot_time()} | {error, any()}.
obtain_locks(_Proc, ClientClock, Locks) ->
    SharedLocks = maps:get(shared_locks, Locks, []),
    ExclusiveLocks = maps:get(exclusive_locks, Locks, []),
    case SharedLocks == [] andalso ExclusiveLocks == [] of
        true ->
            % no locks required ->
            {ok, ClientClock};
        false ->
            {error, 'locks not implemented yet'}
    end.

%% @doc releases the locks
%% Proc: The process that no longer uses the locks.
%% CommitTime: The last commit timestamp of an operation executed while holding the locks
%% Locks: The locks to release
-spec release_locks(pid(), snapshot_time(), lock_spec()) -> ok.
release_locks(_Proc, _CommitTime, _Locks) -> ok.


