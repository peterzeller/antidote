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
-include("antidote_message_types.hrl").
-behavior(gen_server).



%% API
-export([start_link/0, request_locks/2, request_locks_remote/1]).

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
    %% for each exclusively used lock: who is currently using it?
    locks_held_exclusively :: maps:map(antidote_locks:lock(), pid()),
    %% for each shared lock: who is currently using it?
    locks_held_shared :: maps:map(antidote_locks:lock(), list(pid())),
    %% for each lock: who is waiting for this lock?
    lock_waiting :: maps:map(antidote_locks:lock(), queue:queue(requester())),
    %% for each requester: which locks is it still waiting for?
    requester_waiting :: maps:map(requester(), ordsets:ordset(antidote_locks:lock_spec_item()))
}).

-record(request_locks, {
    client_clock :: snapshot_time(),
    locks :: antidote_locks:lock_spec()
}).

-record(request_locks_remote, {
    my_dc_id :: dcid(),
    timestamp :: integer(),
    locks :: antidote_locks:lock_spec()
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
    request(#request_locks{client_clock = ClientClock, locks = Locks}, 3).


% sends a request to the global gen-server instance, starting it if necessary
request(Req, NumTries) ->
    try
        gen_server:call({global, ?SERVER}, Req)
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
            request(Req, NumTries - 1)
    end.

% called in inter_dc_query_response
-spec request_locks_remote(lock_request()) -> ok | {error, Reason}.
request_locks_remote(Req) ->
    request(Req, 3).



%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    % we want to be notified if a transaction holding locks crashes
    process_flag(trap_exit, true),
    {ok, #state{
        lock_waiting = maps:new(),
        locks_held_exclusively = maps:new(),
        locks_held_shared = maps:new(),
        requester_waiting = maps:new()
    }}.

handle_call(#request_locks{client_clock = ClientClock, locks = Locks}, From, State) ->
    handle_request_locks(ClientClock, Locks, From, State);
handle_call(#request_locks_remote{}=Req, From, State) ->
    handle_request_locks_remote(Req, From, State).

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

            AllDcIds = dc_meta_data_utilities:get_dcs(),

            LockValues = [parse_lock_value(V) || V <- LockValuesRaw],
            LockEntries = lists:zip(Locks, LockValues),

            RequestsByDc = requests_for_missing_locks(AllDcIds, tate#state.dc_id, LockEntries),
            case maps:size(RequestsByDc) of
                0 ->
                    % we have all locks locally
                    NewState = try_acquire_locks(From, ordsets:from_list(Locks), State),
                    {noreply, NewState};
                _ ->
                    % tell other data centers that we need locks
                    % for shared locks, ask to get own lock back
                    % for exclusive locks, ask everyone to give their lock

                    lists:foreach(fun({OtherDcID, ReqMsg}) ->
                        {LocalPartition, _} = log_utilities:get_key_partition(locks),
                        PDCID = {OtherDcID, LocalPartition},

                        inter_dc_query:perform_request(?LOCK_SERVER_REQUEST, PDCID, term_to_binary(ReqMsg), fun antidote_lock_server:on_interc_reply/2)
                    end, maps:to_list(RequestsByDc)),

                    % TODO actually, I always need to put it in all places, so that I can make sure not to give
                    % tokens away that I am waiting for myself
                    NewState = add_lock_waiting(From, Locks, State),



                    % will reply once all locks are acquired
                    {noreply, NewState}
            end
    end.

on_interc_reply(_BinaryResp, _RequestCacheEntry) ->
    ok.



-spec get_lock_objects(antidote_locks:lock_spec()) -> list(bound_object()).
get_lock_objects(Locks) ->
    [{Key, antidote_crdt_map_rr, ?LOCK_BUCKET} || {Key, _} <- Locks].




% calculates which inter-dc requests have to be sent out to others
% for requesting all required locks
-spec requests_for_missing_locks(list(dcid()), dcid(), antidote_locks:lock_spec()) -> maps:map(dcid(), #request_locks_remote{}).
requests_for_missing_locks(AllDcIds, MyDcId, Locks) ->
    InterDcRequests = lists:flatmap(requests_for_missing_locks(AllDcIds, MyDcId), Locks),
    case InterDcRequests of
        [] -> maps:new();
        _ ->
            Time = system_time(),
            RequestsByDc = group_by_first(InterDcRequests),
            maps:map(fun(_Dc, Locks) ->
                #request_locks_remote{locks = Locks, my_dc_id = MyDcId, timestamp = Time}
            end, RequestsByDc)
    end.


-spec requests_for_missing_locks(list(dcid()), dcid()) -> fun(({antidote_locks:lock_spec_item(), lock_crdt_value()}) -> list({dcid(), antidote_locks:lock_spec_item()})).
requests_for_missing_locks(AllDcIds, MyDcId) ->
    fun({{Lock, Kind}=LockItem, LockValue}) ->
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
                        [{CurrentOwner, LockItem}]
                end;
            exclusive ->
                % check that we own all datacenters
                case lists:all(fun(Dc) -> maps:get(Dc, LockValueOwners, false) == MyDcId end, AllDcIds) of
                    true ->
                        % if we own all lock parts, we need no further requests
                        [];
                    false ->
                        % otherwise, request all parts from the current owners:
                        [{Owner, LockItem} || Owner <- lists:usort(maps:values(LockValueOwners))]
                end
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



-spec try_acquire_locks(requester(), ordsets:ordset(antidote_locks:lock_spec_item()), #state{}) -> #state{}.
try_acquire_locks(Requester, Locks, State) ->
    {RequesterPid, _} = Requester,
    case Locks of
        [] ->
            % all locks acquired
            gen_server:reply(Requester, ok),
            State;
        [{Lock, Kind}|LocksRest] ->
            case Kind of
                shared ->
                    case maps:find(Lock, State#state.locks_held_exclusively) of
                        {ok, _} ->
                            % lock currently used exclusively -> wait
                            add_lock_waiting(Requester, Locks, State);
                        error ->
                            % not used exclusively -> acquire this lock
                            State2 = State#state{
                                locks_held_shared = maps:update_with(Lock, fun(L) -> [RequesterPid|L] end, [], State#state.locks_held_shared)
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


-spec group_by_first([{K,V}]) -> maps:map(K, [V]).
group_by_first(List) ->
    M1 = lists:foldl(fun({K,V}, M) ->
        maps:update_with(K, fun(L) -> [V|L] end, [], M)
        end, maps:new(), List),
    maps:map(fun(_K,V) -> lists:reverse(V) end, M1).


-spec add_lock_waiting(requester(), antidote_locks:lock_spec(), #state{}) -> #state{}.
add_lock_waiting(From, Locks, State) ->
    OrdLocks = ordsets:from_list(Locks),
    NewLocksWaiting = lists:foldl(fun(L) ->
        maps:update_with(L, fun(Q) -> queue:in(From, Q) end, queue:new(), State#state.lock_waiting)
    end, State#state.lock_waiting, OrdLocks),
    State#state{
        lock_waiting = NewLocksWaiting,
        requester_waiting = maps:put(From, OrdLocks , State#state.requester_waiting)
    }.


handle_request_locks_remote(#request_locks_remote{}, From, State) ->

    erlang:error(not_implemented).
