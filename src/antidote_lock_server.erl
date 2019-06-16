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
-export([start_link/0, request_locks/2, request_locks_remote/1, release_locks/2, on_interdc_reply/2]).

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
% the map stores lock-part to current owner
-export_type([lock_crdt_value/0]).

-type lock_crdt_value() :: #{dcid() => dcid()}.
-type requester() :: {pid(), Tag :: term()}.


-record(request_locks, {
    client_clock :: snapshot_time(),
    locks :: antidote_locks:lock_spec()
}).

-record(release_locks, {
    commit_time :: snapshot_time(),
    locks :: antidote_locks:lock_spec()
}).

-record(request_locks_remote, {
    timestamp :: integer(),
    my_dc_id :: dcid(),
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

-spec release_locks(snapshot_time(), antidote_locks:lock_spec()) -> ok | {error, any()}.
release_locks(CommitTime, Locks) ->
    request(#release_locks{commit_time = CommitTime, locks = Locks}, 0).


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
            request(Req, NumTries - 1);
        Reason ->
            logger:error("Could not handle antidote_lock_server request ~p:~n~p", [Req, Reason]),
            {error, Reason}
    end.

% called in inter_dc_query_response
-spec request_locks_remote(#request_locks_remote{}) -> ok | {error, Reason :: any()}.
request_locks_remote(Req) ->
    request(Req, 3).



%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    % we want to be notified if a transaction holding locks crashes
    process_flag(trap_exit, true),
    {ok, antidote_lock_server_state:initial()}.

handle_call(#request_locks{client_clock = ClientClock, locks = Locks}, From, State) ->
    handle_request_locks(ClientClock, Locks, From, State);
handle_call(#release_locks{}, From, State) ->
    {FromPid, _Tag} = From,
    handle_release_locks(FromPid, State);
handle_call(#request_locks_remote{}=Req, From, State) ->
    handle_request_locks_remote(Req, From, State).

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', FromPid, _Reason}, State) ->
    % when a process crashes, it's locks are released
    {reply, _, NewState} = handle_release_locks(FromPid, State),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================






-spec handle_request_locks(snapshot_time(), antidote_locks:lock_spec(), requester(), antidote_lock_server_state:state()) -> Result
    when Result :: {reply, {could_not_obtain_logs, any()}, antidote_lock_server_state:state()}
| {reply, ok, antidote_lock_server_state:state()}
| {noreply, antidote_lock_server_state:state()}.
handle_request_locks(ClientClock, Locks, From, State) ->
    LockObjects = get_lock_objects(Locks),
    case antidote:read_objects(ClientClock, [], LockObjects) of
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

            MyDcId = antidote_lock_server_state:my_dc_id(State),
            RequestsByDc = antidote_lock_server_state:missing_locks_by_dc(AllDcIds, MyDcId, LockEntries),
            case maps:size(RequestsByDc) of
                0 ->
                    % we have all locks locally
                    NewState = try_acquire_locks(From, Locks, State),
                    {noreply, NewState};
                _ ->
                    % tell other data centers that we need locks
                    % for shared locks, ask to get own lock back
                    % for exclusive locks, ask everyone to give their lock

                    SystemTime = erlang:system_time(),
                    send_interdc_lock_requests(RequestsByDc, SystemTime, MyDcId),

                    NewState1 = antidote_lock_server_state:add_lock_waiting(FromPid, SystemTime, Locks, State),
                    NewState2 = maps:fold(fun(_Dc, #request_locks_remote{locks = Ls}, S) ->
                        antidote_lock_server_state:set_lock_waiting_remote(From, Ls, S)
                    end, NewState1, RequestsByDc),

                    % will reply once all locks are acquired
                    {noreply, NewState2}
            end
    end.

-spec send_interdc_lock_requests(#{dcid() => antidote_locks:lock_spec()}, integer(), dcid()) -> ok.
send_interdc_lock_requests(RequestsByDc, Time, MyDcID) ->
    maps:fold(fun(OtherDcID, RequestedLocks, ok) ->
        ReqMsg = #request_locks_remote{timestamp = Time, locks = RequestedLocks, my_dc_id = MyDcID},
        send_interdc_lock_request(OtherDcID, ReqMsg)
    end, ok, RequestsByDc).

-spec send_interdc_lock_request(dcid(), #request_locks_remote{}) -> ok.
send_interdc_lock_request(OtherDcID, ReqMsg) ->
    {LocalPartition, _} = log_utilities:get_key_partition(locks),
    PDCID = {OtherDcID, LocalPartition},
    ok = inter_dc_query:perform_request(?LOCK_SERVER_REQUEST, PDCID, term_to_binary(ReqMsg), fun antidote_lock_server:on_interdc_reply/2).

on_interdc_reply(_BinaryResp, _RequestCacheEntry) ->
    ok.


%% Lock CRDT, stored under Lock, antidote_crdt_map_rr, ?LOCK_BUCKET}
%% In the map: Lock-part to current lock holder
%%   keys: {DcId, antidote_crdt_register_mv}
%%   values: DcId

-spec get_lock_objects(antidote_locks:lock_spec()) -> list(bound_object()).
get_lock_objects(Locks) ->
    [get_lock_object(Key) || {Key, _} <- Locks].


-spec get_lock_object(antidote_locks:lock()) -> bound_object().
get_lock_object(Lock) ->
    {Lock, antidote_crdt_map_rr, ?LOCK_BUCKET}.

-spec parse_lock_value(antidote_crdt_map_rr:value()) -> lock_crdt_value().
parse_lock_value(RawV) ->
    maps:from_list([{binary_to_term(K), binary_to_term(V)} || {{K, _}, V} <- RawV]).

-spec make_lock_updates(antidote_locks:lock(), [{dcid(), dcid()}]) -> [{bound_object(), {op_name(), op_param()}}].
make_lock_updates(_Lock, []) -> [];
make_lock_updates(Lock, Updates) ->
    [{get_lock_object(Lock), {update,
        [{{K, antidote_crdt_register_mv}, {assign, V}} || {K,V} <- Updates]}}].

% End of CRDT functions







-spec try_acquire_locks(requester(), antidote_locks:lock_spec(), antidote_lock_server_state:state()) -> antidote_lock_server_state:state().
try_acquire_locks(Requester, _Locks, State) ->
    {RequesterPid, _} = Requester,
    {AllAcquired, NewState} = antidote_lock_server_state:try_acquire_locks(RequesterPid, State),
    case AllAcquired of
        true ->
            gen_server:reply(Requester, ok);
        false ->
            ok
    end,
    NewState.




handle_request_locks_remote(#request_locks_remote{locks = Locks, timestamp = Timestamp, my_dc_id = RequesterDcId}, From, State) ->
    NewState = antidote_lock_server_state:add_process(From, Timestamp, true, Locks, State),

    {AllLocksAcquired, NewState2} = antidote_lock_server_state:try_acquire_locks(RequesterDcId, NewState),

    MyDcId = antidote_lock_server_state:my_dc_id(State),

    case AllLocksAcquired of
        true ->
            % send locks to other data centers
            handoff_locks_to_other_dcs_async(Locks, RequesterDcId, MyDcId),


            % send requests again for the locks we gave away and still need:
            RequestAgain = antidote_lock_server_state:filter_waited_for_locks([L || {L, _} <- Locks], NewState2),
            send_interdc_lock_requests(#{RequesterDcId => RequestAgain}, erlang:system_time(), MyDcId),

            complete_request(From, ok, NewState2),
            {noreply, NewState2};
        false ->
            {noreply, NewState2}
    end.

-spec handoff_locks_to_other_dcs_async(antidote_locks:lock_spec(), dcid(), dcid()) -> pid().
handoff_locks_to_other_dcs_async(Locks, RequesterDcId, MyDcId) ->
    spawn_link(fun() ->
        {ok, TxId} = antidote:start_transaction(ignore, []),
        LockObjects = get_lock_objects(Locks),
        {ok, LockValuesRaw} = antidote:read_objects(LockObjects, TxId),
        LockValues = [parse_lock_value(L) || L <- LockValuesRaw],
        Updates = lists:flatmap(fun({{Lock, Kind}, LockValue}) ->
            case Kind of
                shared ->
                    case maps:find(RequesterDcId, LockValue) of
                        {ok, MyDcId} ->
                            make_lock_updates(Lock, [{RequesterDcId, RequesterDcId}]);
                        _ ->
                            []
                    end;
                exclusive ->
                    make_lock_updates(Lock, [{K, RequesterDcId} || {K, V} <- maps:to_list(LockValue), V == MyDcId])
            end
        end, lists:zip(Locks, LockValues)),
        antidote:update_objects(Updates, TxId),
        antidote:commit_transaction(TxId)
    end).


-spec handle_release_locks(requester(), antidote_lock_server_state:state()) ->
    {reply, ok, antidote_lock_server_state:state()}.
handle_release_locks(FromPid, State) ->

    CheckResult = antidote_lock_server_state:check_release_locks(FromPid, State),
    {HandOverActions, LockRequestActions, Replies, NewState} = antidote_lock_server_state:remove_locks(FromPid, State),

    MyDcId = antidote_lock_server_state:my_dc_id(State),

    % send to other data centers
    lists:foreach(fun({HandOver, RequesterDcId}) ->
        handoff_locks_to_other_dcs_async(HandOver, RequesterDcId, MyDcId)
    end, HandOverActions),

    % send requests again
    send_interdc_lock_requests(LockRequestActions, erlang:system_time(), MyDcId),

    % reply to acquired locks
    lists:foreach(fun(R) -> gen_server:reply(R, ok) end, Replies),

    case CheckResult of
        {error, Reason} ->
            {reply, {lock_error, Reason}, NewState};
        ok ->
            {reply, ok, NewState}
    end.


complete_request(From, Reply, State) ->
    {Pid, _} = From,
    gen_server:reply(From, Reply),
    antidote_lock_server_state:remove_locks(Pid, State).
