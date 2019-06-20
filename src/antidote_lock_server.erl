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

% how long (in milliseconds) can a local server prefer local requests over remote requests?
% (higher values should give higher throughput but also higher latency if remote lock requests are necessary)
-define(INTER_DC_LOCK_REQUEST_DELAY, 500).

% how long (in milliseconds) may a transaction take to acquire the necessary locks?
-define(LOCK_REQUEST_TIMEOUT, 20000).
-define(LOCK_REQUEST_RETRIES, 3).

% how long (in milliseconds) can a transaction be alive when it is holding locks?
-define(MAX_TRANSACTION_TIME, 20000).

-define(SERVER, ?MODULE).

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

-record(on_receive_remote_locks, {
    locks :: antidote_locks:lock_spec(),
    snapshot_time :: snapshot_time()
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
    request(#request_locks{client_clock = ClientClock, locks = Locks}, ?LOCK_REQUEST_TIMEOUT, ?LOCK_REQUEST_RETRIES).

-spec release_locks(snapshot_time(), antidote_locks:lock_spec()) -> ok | {error, any()}.
release_locks(CommitTime, Locks) ->
    request(#release_locks{commit_time = CommitTime, locks = Locks}, infinity, 0).

-spec on_receive_remote_locks(antidote_locks:lock_spec(), snapshot_time()) -> ok | {error, any()}.
on_receive_remote_locks(Locks, SnapshotTime) ->
    request(#on_receive_remote_locks{snapshot_time = SnapshotTime, locks = Locks}, infinity, 0).


% sends a request to the global gen-server instance, starting it if necessary
request(Req, Timeout, NumTries) ->
    try
        gen_server:call({global, ?SERVER}, Req, Timeout)
    catch
        exit:{noproc, _} when NumTries > 0 ->
            % if there is no lock server running, start one and try again
            % we register this as a transient process directly under the antidote_sup:
            supervisor:start_child(antidote_sup, #{
                id => lock_server,
                start => {?MODULE, start_link, []},
                % using a transient process, because it will be started on demand and we need
                % to avoid conflicts with other shards who might als try to start a server
                restart => transient
            }),
            request(Req, Timeout, NumTries - 1);
        Reason ->
            logger:error("Could not handle antidote_lock_server request ~p:~n~p", [Req, Reason]),
            case NumTries > 0 of
                true -> request(Req, Timeout, NumTries - 1);
                false -> {error, Reason}
            end
    end.

% called in inter_dc_query_response
-spec request_locks_remote(#request_locks_remote{}) -> ok | {error, Reason :: any()}.
request_locks_remote(Req) ->
    request(Req, infinity, 3).



%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    % we want to be notified if a transaction holding locks crashes
    process_flag(trap_exit, true),
    MyDcId = dc_meta_data_utilities:get_my_dc_id(),
    {ok, antidote_lock_server_state:initial(MyDcId)}.

handle_call(#request_locks{client_clock = ClientClock, locks = Locks}, From, State) ->
    handle_request_locks(ClientClock, Locks, From, State);
handle_call(#release_locks{commit_time = CommitTime}, From, State) ->
    {FromPid, _Tag} = From,
    handle_release_locks(FromPid, CommitTime, State);
handle_call(#request_locks_remote{}=Req, From, State) ->
    handle_request_locks_remote(Req, From, State);
handle_call(#on_receive_remote_locks{snapshot_time = SnapshotTime, locks = Locks}, _From, State) ->
    handle_on_receive_remote_locks(Locks, SnapshotTime, State).

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', FromPid, _Reason}, State) ->
    % when a process crashes, its locks are released
    {reply, _, NewState} = handle_release_locks(FromPid, vectorclock:new(), State),
    {noreply, NewState};
handle_info({transaction_timeout, FromPid}, State) ->
    {reply, Res, NewState} = handle_release_locks(FromPid, vectorclock:new(), State),
    case Res of
        ok ->
            % kill the transaction process if it still has the locks
            erlang:exit(FromPid, kill);
        _ -> ok
    end,
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
    when Result :: {reply, Resp, antidote_lock_server_state:state()} | {noreply, antidote_lock_server_state:state()},
    Resp :: {could_not_obtain_logs, any()}.
handle_request_locks(ClientClock, Locks, From, State) ->
    logger:info("handle_request_locks ~p~n  ~p", [Locks, ClientClock]),
    LockObjects = antidote_lock_crdt:get_lock_objects(Locks),
    case antidote:read_objects(ClientClock, [], LockObjects) of
        {error, Reason} ->
            % this could happen if the shards containing the locks are down
            % if we cannot read the locks we fail immediately since waiting
            % would probably take too much time
            {reply, {could_not_obtain_logs, Reason}, State};
        {ok, LockValuesRaw, ReadClock} ->
            % link the requester:
            % if we crash, then the transaction using the locks should crash as well
            % if the transaction crashes, we want to know about that to release the lock
            {FromPid, _} = From,
            link(FromPid),
            % send a message so that we can kill the transaction if it takes too long
            timer:send_after(?MAX_TRANSACTION_TIME, {transaction_timeout, FromPid}),

            AllDcIds = dc_meta_data_utilities:get_dcs(),

            LockValues = [antidote_lock_crdt:parse_lock_value(V) || V <- LockValuesRaw],
            LockEntries = lists:zip(Locks, LockValues),
            logger:info("handle_request_locks~n AllDcIds = ~p~nLockEntries=~n  ~p", [AllDcIds, LockEntries]),

            {Actions, NewState} = antidote_lock_server_state:new_request(From, erlang:system_time(millisecond), ReadClock, AllDcIds, LockEntries, State),
            run_actions(Actions, NewState),
            {noreply, NewState}
    end.

-spec send_interdc_lock_requests(#{dcid() => antidote_locks:lock_spec()}, integer(), dcid()) -> ok.
send_interdc_lock_requests(RequestsByDc, Time, MyDcID) ->
    maps:fold(fun(OtherDcID, RequestedLocks, ok) ->
        logger:notice("antidote_lock_server: requesting locks from ~p~n  RequestedLocks = ~p", [OtherDcID, RequestedLocks]),
        ReqMsg = #request_locks_remote{timestamp = Time, locks = RequestedLocks, my_dc_id = MyDcID},
        send_interdc_lock_request(OtherDcID, ReqMsg)
    end, ok, RequestsByDc).

-spec send_interdc_lock_request(dcid(), #request_locks_remote{}) -> ok.
send_interdc_lock_request(OtherDcID, ReqMsg) ->
    {LocalPartition, _} = log_utilities:get_key_partition(locks),
    PDCID = {OtherDcID, LocalPartition},
    ok = inter_dc_query:perform_request(?LOCK_SERVER_REQUEST, PDCID, term_to_binary(ReqMsg), fun antidote_lock_server:on_interdc_reply/2).

on_interdc_reply(BinaryResp, _RequestCacheEntry) ->
    {ok, Locks, SnapshotTime} = binary_to_term(BinaryResp),
    on_receive_remote_locks(Locks, SnapshotTime),
    ok.


handle_request_locks_remote(#request_locks_remote{locks = Locks, timestamp = Timestamp, my_dc_id = RequesterDcId}, From, State) ->
    logger:info("handle_request_locks_remote~n  Locks = ~p~n  Timestamp = ~p~n  RequesterDcId = ~p", [Locks, Timestamp, RequesterDcId]),
    {Actions, NewState} = antidote_lock_server_state:new_remote_request(From, Timestamp, Locks, RequesterDcId, State),
    run_actions(Actions, NewState),
    {noreply, NewState}.


-spec handoff_locks_to_other_dcs(antidote_locks:lock_spec(), dcid(), dcid(), snapshot_time()) -> {ok, snapshot_time()} | {error, reason()}.
handoff_locks_to_other_dcs(Locks, RequesterDcId, MyDcId, Snapshot) ->
    case antidote:start_transaction(Snapshot, []) of
        {error, Reason} ->
            logger:error("Could not start transaction:~n  ~p", [Reason]);
        {ok, TxId} ->
            LockObjects = antidote_lock_crdt:get_lock_objects(Locks),
            {ok, LockValuesRaw} = antidote:read_objects(LockObjects, TxId),
            LockValues = [antidote_lock_crdt:parse_lock_value(L) || L <- LockValuesRaw],
            Updates = lists:flatmap(fun({{Lock, Kind}, LockValue1}) ->
                LockValue = maps:merge(#{MyDcId => MyDcId}, LockValue1),
                case Kind of
                    shared ->
                        case maps:find(RequesterDcId, LockValue) of
                            {ok, MyDcId} ->
                                antidote_lock_crdt:make_lock_updates(Lock, [{RequesterDcId, RequesterDcId}]);
                            _ ->
                                []
                        end;
                    exclusive ->
                        antidote_lock_crdt:make_lock_updates(Lock, [{K, RequesterDcId} || {K, V} <- maps:to_list(LockValue), V == MyDcId])
                end
            end, lists:zip(Locks, LockValues)),
            logger:info("handoff_locks_to_other_dcs~n  Updates = ~p", [Updates]),
            antidote:update_objects(Updates, TxId),
            {ok, LockValuesRaw2} = antidote:read_objects(LockObjects, TxId),
            logger:info("handoff_locks_to_other_dcs~n  LockValuesRaw2 = ~p", [LockValuesRaw2]),
            antidote:commit_transaction(TxId)
    end.


-spec handle_release_locks(pid(), snapshot_time(), antidote_lock_server_state:state()) ->
    {reply, Resp, antidote_lock_server_state:state()}
    when Resp :: ok | {lock_error, reason()}.
handle_release_locks(FromPid, CommitTime, State) ->
    logger:info("handle_release_locks ~p", [CommitTime]),

    CheckResult = antidote_lock_server_state:check_release_locks(FromPid, State),
    {Actions, NewState} = antidote_lock_server_state:remove_locks(FromPid, CommitTime, State),

    run_actions(Actions, NewState),

    case CheckResult of
        {error, Reason} ->
            {reply, {lock_error, Reason}, NewState};
        ok ->
            {reply, ok, NewState}
    end.

-spec run_actions(antidote_lock_server_state:actions(), antidote_lock_server_state:state()) -> ok.
run_actions(Actions, State) ->
    {HandOverActions, LockRequestActions, Replies} = Actions,
    MyDcId = antidote_lock_server_state:my_dc_id(State),

    % send to other data centers
    lists:foreach(fun({HandOver, RequesterDcId, Requester}) ->
        {ok, SnapshotTime} = handoff_locks_to_other_dcs(HandOver, RequesterDcId, MyDcId, antidote_lock_server_state:get_snapshot_time(State)),
        logger:notice("antidote_lock_server: handover locks to ~p~n  HandOver = ~p~n SnapshotTime = ~p", [RequesterDcId, HandOver, SnapshotTime]),
        gen_server:reply(Requester, {ok, HandOver, SnapshotTime})
    end, HandOverActions),

    % send requests
    send_interdc_lock_requests(LockRequestActions, erlang:system_time(millisecond) + ?INTER_DC_LOCK_REQUEST_DELAY, MyDcId),

    % reply to acquired locks
    lists:foreach(fun(R) ->
        Reply = {ok, antidote_lock_server_state:get_snapshot_time(State)},
        logger:notice("antidote_lock_server: replying to ~p:~n  ~p", [R, Reply]),
        gen_server:reply(R, Reply)
    end, Replies),
    ok.


handle_on_receive_remote_locks(Locks, SnapshotTime, State) ->
    logger:info("handle_on_receive_remote_locks~n  Locks = ~p~n  SnapshotTime = ~p", [Locks, SnapshotTime]),
    LockObjects = antidote_lock_crdt:get_lock_objects(Locks),
    case antidote:read_objects(SnapshotTime, [], LockObjects) of
        {error, _Reason} ->
            % ignore
            {reply, ok, State};
        {ok, LockValuesRaw, ReadClock} ->
            logger:info("handle_on_receive_remote_locks~n  LockValuesRaw = ~p", [LockValuesRaw]),
            AllDcIds = dc_meta_data_utilities:get_dcs(),
            LockValues = [antidote_lock_crdt:parse_lock_value(V) || V <- LockValuesRaw],
            LockEntries = lists:zip(Locks, LockValues),
            {Actions, NewState} = antidote_lock_server_state:on_remote_locks_received(ReadClock, AllDcIds, LockEntries, State),
            run_actions(Actions, NewState),
            {noreply, NewState}
    end.
