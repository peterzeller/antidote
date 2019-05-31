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

%% @doc A lock server is running on each datacenter
%% it is globally registered under the name 'antidote_lock_server'
%%
%% and manages the locks related to the transactions running on the same shard
-module(antidote_lock_server).
%%
-include("antidote.hrl").
-behavior(gen_server).



%% API
-export([start_link/0, request_locks/2]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).
-define(LOCK_BUCKET, <<"__antidote_lock_bucket">>).

% there is one lock-part per datacenter.
-type lock_crdt_value() :: #{
    % lock-part to current owner
    owners => #{dcid() => dcid()},
    % who is currently waiting to obtain this lock?
    waiting => list({Requester :: dcid(), antidote_locks:lock_kind(), RequestTime :: non_neg_integer()})
}.
-type requester() :: {pid(), Tag :: term()}.

-record(state, {
    %% own datacenter id
    dc_id :: dcid(),
    %% map from locks to the processes using the lock
    locks_held :: maps:map(antidote_locks:lock(), list(pid())),
    %% for each lock: who is waiting for this lock?
    lock_waiting :: maps:map(antidote_locks:lock(), list(requester())),
    %% for each requester: who is it still waiting for?
    requester_waiting :: maps:map(requester(), antidote_locks:lock())
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Starts the server
-spec(start_link() -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    % globally register this server so that we only have one
    % lock manager for the whole data center.
    gen_server:start_link({global, ?SERVER}, ?MODULE, [], []).

-spec request_locks(snapshot_time(), antidote_locks:lock_spec()) -> {ok, snapshot_time()} | {error, any()}.
request_locks(ClientClock, Locks) ->
    request_locks(ClientClock, Locks, 3).


request_locks(ClientClock, Locks, NumTries) ->
    try
        gen_server:call({global, ?SERVER}, {request_locks, ClientClock, Locks})
    catch
        {'EXIT', {noproc, _}} when NumTries > 0 ->
            % if there is no lock server running, start one and try again
            % we register this as a transient process directly under the antidote_sup:
            supervisor:start_child(antidote_sup, #{
                id => lock_server,
                start => {?MODULE, start_link, []},
                % using a transient process, because it will be started on demand and we need
                % to avoid conflicts with other shards who might als try to start a server
                restart => transient
            }),
            request_locks(ClientClock, Locks, NumTries - 1)
    end.




%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    % we want to be notified if a transaction holding locks crashes
    process_flag(trap_exit, true),
    {ok, #state{
        lock_waiting = maps:new(),
        locks_held = maps:new(),
        requester_waiting = maps:new()
    }}.

handle_call({request_locks, ClientClock, Locks}, From, State) ->
    handle_request_locks(ClientClock, Locks, From, State).

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================






-spec handle_request_locks(snapshot_time(), antidote_locks:lock_spec(), requester(), #state{}) -> Result
    when Result :: {reply, {could_not_obtain_logs, any()}, #state{}}
                 | {reply, ok, #state{}}
                 | {noreply, #state{}}.
handle_request_locks(ClientClock, Locks, From, State) ->
    LockObjects = get_lock_objects(Locks),
    AllDcIds = get_all_dc_ids(),
    case cure:read_objects(ClientClock, [], LockObjects) of
        {error, Reason} ->
            % this could happen if the shards containing the locks are down
            % if we cannot read the locks we fail immediately since waiting
            % would probably take too much time
            {reply, {could_not_obtain_logs, Reason}, State};
        {ok, LockValuesRaw, _ReadClock} ->
            % link the requester:
            % if we crash, then the transaction using the locks should crash as well
            % if the transaction crashes, we want to know about that to release the lock
            {FromPid, _} = From,
            link(FromPid),
            LockValues = [parse_lock_value(V) || V <- LockValuesRaw],
            LockEntries = lists:zip(Locks, LockValues),
            {_LockEntriesOwn, LockEntriesRequired} = lists:partition(owns_lock(AllDcIds, State#state.dc_id), LockEntries),
            case LockEntriesRequired of
                [] ->
                    NewState = State#state{
                      locks_held = todo % TODO update locks_held
                    },
                    {reply, ok, NewState};
                _ ->
                    % tell other data centers that we need locks
                    % for shared locks, ask to get own lock back
                    % for exclusive locks, ask everyone to give their lock

                    %inter_dc_query:perform_request(),
                    NewState = todo,

                    % will reply once all locks are acquired
                    {noreply, NewState}
            end
    end.



-spec get_lock_objects(antidote_locks:lock_spec()) -> list(bound_object()).
get_lock_objects(Locks) ->
    [{Key, antidote_crdt_map_rr, ?LOCK_BUCKET} || {Key, _} <- Locks].


-spec owns_lock(list(dcid()), dcid()) -> fun(({antidote_locks:lock_spec_item(), lock_crdt_value()}) -> boolean()).
owns_lock(AllDcIds, DcId) ->
    fun({{_Lock, Kind}, LockValue}) ->
        LockValueOwners = maps:get(owners, LockValue),
        case Kind of
            shared ->
                %check that we own at least one entry in the map
                lists:member(DcId, maps:values(LockValueOwners));
            exclusive ->
                % check that we own all datacenters
                lists:all(fun(Dc) -> maps:get(Dc, LockValueOwners, false) == DcId end, AllDcIds)
        end
    end.

-spec parse_lock_value(antidote_crdt_map_rr:value()) -> lock_crdt_value().
parse_lock_value(RawV) ->
    Locks = orddict_get({<<"owners">>, antidote_crdt_map_rr}, RawV, []),
    Waiting = orddict_get({<<"waiting">>, antidote_crdt_set_aw}, RawV, []),
    #{
        owners => maps:from_list([{binary_to_term(K), binary_to_term(V)} || {{K, _}, V} <- Locks]),
        waiting => [binary_to_term(W) || W <- Waiting]
    }.

-spec orddict_get(K, orddict:orddict(K, V), V) -> V.
orddict_get(Key, Dict, Default) ->
    case orddict:find(Key, Dict) of
        {ok, Val} -> Val;
        error -> Default
    end.


-spec get_all_dc_ids() -> list(dcid()).
get_all_dc_ids() ->
    [].
