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
%% and manages the locks related to the transactions running on the same shard

%% TODO move transaction/IO code into separate processes

-module(antidote_lock_server).
%%
-include("antidote.hrl").
-include("antidote_message_types.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
% how long to wait for locks before requesting them again
-define(INTER_DC_RETRY_DELAY, 500).

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

-record(request_locks2, {
    lock_entries :: antidote_locks:lock_spec(),
    read_clock :: snapshot_time(),
    from :: requester()
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

-record(on_receive_remote_locks2, {
    lock_entries :: antidote_locks:lock_spec(),
    read_clock :: snapshot_time()
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
    logger:notice("on_receive_remote_locks Locks = ~p~n  SnapshotTime = ~p", [Locks, antidote_lock_server_state:print_vc(SnapshotTime)]),
    request(#on_receive_remote_locks{snapshot_time = SnapshotTime, locks = Locks}, infinity, 0).


% sends a request to the global gen-server instance, starting it if necessary
request(Req, Timeout, NumTries) ->
    try
        gen_server:call({global, ?SERVER}, Req, Timeout)
    catch
        exit:{noproc, _} when NumTries > 0 ->
            % if there is no lock server running, start one and try again
            % we register this as a transient process directly under the antidote_sup:
            {ok, _} = supervisor:start_child(antidote_sup, #{
                id => lock_server,
                start => {?MODULE, start_link, []},
                % using a transient process, because it will be started on demand and we need
                % to avoid conflicts with other shards who might als try to start a server
                restart => transient
            }),
            request(Req, Timeout, NumTries - 1);
        Err:Reason:ST ->
            logger:error("Could not handle antidote_lock_server request"),
            logger:error("Could not handle antidote_lock_server request ~p", [Req]),
            logger:error("Could not handle antidote_lock_server request ~p:~n~p~n~p~n~p", [Req, Err, Reason, ST]),
            case catch sys:get_status({global, ?SERVER}, 1000) of
                {status, Pid, {module, Mod}, [PDict, SysState, Parent, Dbg, Misc]} ->
                    logger:error("Current status of lock server:~n  Pid = ~p~n  Mod = ~p~n  PDict = ~p~n  SysState = ~p~n  Parent = ~p~n  Dbg = ~p~n  Misc = ~p~n  ", [Pid, Mod, PDict, SysState, Parent, Dbg, Misc]);
                Other ->
                    logger:error("Could not get debug info~n~p", [Other])
            end,

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
    Self = self(),
    spawn_link(fun() ->
        check_lock_state_process(Self)
    end),
    {ok, antidote_lock_server_state:initial(MyDcId)}.

check_lock_state_process(Pid) ->
    timer:sleep(100),
    {ok, Locks} = gen_server:call(Pid, get_remote_waiting_locks),

    LockObjects = antidote_lock_crdt:get_lock_objects([{L, shared} || L <- Locks]),
    case LockObjects of
        [] ->
            timer:sleep(100);
        _ ->

            case antidote:read_objects(ignore, [], LockObjects) of
                {error, Reason} ->
                    % this could happen if the shards containing the locks are down
                    % if we cannot read the locks we fail immediately since waiting
                    % would probably take too much time
                    logger:error("check_lock_state_process: Could not obtain locks:~n~p", [Reason]),
                    ok;
                {ok, LockValuesRaw, ReadClock} ->
                    LockValues = [antidote_lock_crdt:parse_lock_value(V) || V <- LockValuesRaw],
                    LockEntries = lists:zip(Locks, LockValues),
                    logger:notice("check_lock_state_process, LockEntries = ~n~p", [LockEntries]),
                    gen_server:cast(Pid, #on_receive_remote_locks2{lock_entries = LockEntries, read_clock = ReadClock})
            end
    end,

    %repeat
    check_lock_state_process(Pid).


handle_call(Req, From, State) ->
    logger:notice("handle_call~n  Req = ~p~n  State = ~p", [Req, antidote_lock_server_state:print_state(State)]),
    {Time, Res} = timer:tc(fun() -> handle_call2(Req, From, State) end),
    logger:notice("handle_call done in ~pµs", [Time]),
    Res.

handle_cast(Req, State) ->
    logger:notice("handle_cast~n  Req = ~p~n  State = ~p", [Req, antidote_lock_server_state:print_state(State)]),
    {Time, Res} = timer:tc(fun() -> handle_cast2(Req, State) end),
    logger:notice("handle_cast done in ~pµs", [Time]),
    Res.

handle_info(Req, State) ->
    logger:notice("handle_info~n  Req = ~p~n  State = ~p", [Req, antidote_lock_server_state:print_state(State)]),
    {Time, Res} = timer:tc(fun() -> handle_info2(Req, State) end),
    logger:notice("handle_info done in ~pµs", [Time]),
    Res.

handle_call2(#request_locks{client_clock = ClientClock, locks = Locks}, From, State) ->
    handle_request_locks(ClientClock, Locks, From, State);
handle_call2(#release_locks{commit_time = CommitTime}, From, State) ->
    {FromPid, _Tag} = From,
    handle_release_locks(FromPid, CommitTime, State);
handle_call2(#request_locks_remote{}=Req, From, State) ->
    handle_request_locks_remote(Req, From, State);
handle_call2(#on_receive_remote_locks{snapshot_time = SnapshotTime, locks = Locks}, _From, State) ->
    handle_on_receive_remote_locks(Locks, SnapshotTime, State);
handle_call2(get_remote_waiting_locks, _From, State) ->
    {reply, {ok, antidote_lock_server_state:get_remote_waiting_locks(State)}, State}.

handle_cast2(start_interdc_lock_requests_timer, State) ->
    AlreadyActive = antidote_lock_server_state:get_timer_active(State),
    case AlreadyActive of
        true ->
            {noreply, State};
        false ->
            erlang:send_after(?INTER_DC_RETRY_DELAY, self(), inter_dc_retry_timeout),
            {noreply, antidote_lock_server_state:set_timer_active(true, State)}
    end;
handle_cast2(#on_receive_remote_locks2{lock_entries = LockEntries, read_clock = ReadClock}, State) ->
    handle_on_receive_remote_locks2(LockEntries, ReadClock, State);
handle_cast2(#request_locks2{lock_entries = LockEntries, read_clock = ReadClock, from = From}, State) ->
    handle_request_locks2(LockEntries, ReadClock, From, State).

handle_info2(inter_dc_retry_timeout, State) ->
    Time = erlang:system_time(),
    AllDcIds = dc_meta_data_utilities:get_dcs(),
    MyDcId = antidote_lock_server_state:my_dc_id(State),
    OtherDcs = AllDcIds -- [MyDcId],
    {Waiting, InterDcRequests} = antidote_lock_server_state:retries_for_waiting_remote(Time, ?INTER_DC_RETRY_DELAY div 2, OtherDcs, State),
    logger:notice("Retrying requests: ~p, ~p~n  State = ~p", [InterDcRequests, Waiting, antidote_lock_server_state:print_state(State)]),
    send_interdc_lock_requests(InterDcRequests, MyDcId),
    case Waiting of
        true ->
            erlang:send_after(?INTER_DC_RETRY_DELAY, self(), inter_dc_retry_timeout),
            {noreply, State};
        false ->
            % disable timer if no more requests
            {noreply, antidote_lock_server_state:set_timer_active(false, State)}
    end;

handle_info2({'EXIT', FromPid, Reason}, State) ->
    % when a process crashes, its locks are released
    case Reason of
        normal -> ok;
        _ ->
            logger:notice("process exited ~p ~n  Reason = ~p", [FromPid, Reason])
    end,
    {reply, _, NewState} = handle_release_locks(FromPid, vectorclock:new(), State),
    {noreply, NewState};
handle_info2({transaction_timeout, FromPid}, State) ->
    {reply, Res, NewState} = handle_release_locks(FromPid, vectorclock:new(), State),
    case Res of
        ok ->
            % kill the transaction process if it still has the locks
            erlang:exit(FromPid, kill);
        _ -> ok
    end,
    {noreply, NewState};
handle_info2(_Info, State) ->
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
    % link the requester:
    % if we crash, then the transaction using the locks should crash as well
    % if the transaction crashes, we want to know about that to release the lock
    {FromPid, _} = From,
    link(FromPid),
    % send a message so that we can kill the transaction if it takes too long
    {ok, _} = timer:send_after(?MAX_TRANSACTION_TIME, {transaction_timeout, FromPid}),

    Self = self(),

    spawn_link(fun() ->
        LockObjects = antidote_lock_crdt:get_lock_objects(Locks),

        case antidote:read_objects(ClientClock, [], LockObjects) of
            {error, Reason} ->
                % this could happen if the shards containing the locks are down
                % if we cannot read the locks we fail immediately since waiting
                % would probably take too much time
                gen_server:reply(From, {could_not_obtain_logs, Reason});
            {ok, LockValuesRaw, ReadClock} ->
                LockValues = [antidote_lock_crdt:parse_lock_value(V) || V <- LockValuesRaw],
                LockEntries = lists:zip(Locks, LockValues),
                gen_server:cast(Self, #request_locks2{lock_entries = LockEntries, read_clock = ReadClock, from = From})
        end
    end),
    {noreply, State}.

handle_request_locks2(LockEntries, ReadClock, From, State) ->
    AllDcIds = dc_meta_data_utilities:get_dcs(),

    {Actions, NewState} = antidote_lock_server_state:new_request(From, erlang:system_time(millisecond), ?INTER_DC_LOCK_REQUEST_DELAY, ReadClock, AllDcIds, LockEntries, State),
    logger:notice("handle_request_locks~n AllDcIds = ~p~nLockEntries= ~p~n  Actions = ~p", [AllDcIds, LockEntries, antidote_lock_server_state:print_actions(Actions)]),
    run_actions(Actions, NewState),
    {noreply, NewState}.



-spec send_interdc_lock_requests(antidote_lock_server_state:lock_request_actions(), dcid()) -> ok.
send_interdc_lock_requests(M, _) when M == #{} -> ok;
send_interdc_lock_requests(RequestsByDc, MyDcID) ->
    logger:notice("send_interdc_lock_requests~n RequestsByDc = ~p", [antidote_lock_server_state:print_lock_request_actions(RequestsByDc)]),
    maps:fold(fun(OtherDcID, RequestedLocks, ok) ->
        ByTime = antidote_list_utils:group_by_first([{RequestTime, {Lock, Kind}} || {Lock, {Kind, RequestTime}} <- RequestedLocks]),
        % send one request for each request-time
        maps:fold(fun(Time, RLocks, _) ->
            ReqMsg = #request_locks_remote{timestamp = Time, locks = RLocks, my_dc_id = MyDcID},
            send_interdc_lock_request(OtherDcID, ReqMsg, 3),
            ok
        end, ok, ByTime)
    end, ok, RequestsByDc).

-spec send_interdc_lock_request(dcid(), #request_locks_remote{}, integer()) -> ok.
send_interdc_lock_request(OtherDcID, ReqMsg, Retries) ->
    {LocalPartition, _} = log_utilities:get_key_partition(locks),
    PDCID = {OtherDcID, LocalPartition},
    logger:notice("send_interdc_lock_request to ~p:~n~p", [OtherDcID, ReqMsg]),
    case inter_dc_query:perform_request(?LOCK_SERVER_REQUEST, PDCID, term_to_binary(ReqMsg), fun antidote_lock_server:on_interdc_reply/2) of
        ok ->
            logger:notice("send_interdc_lock_request ok, to ~p,~n~p", [OtherDcID, ReqMsg]),
            ok;
        Err when Retries > 0 ->
            logger:warning("send_interdc_lock_request failed ~p ~p ~p ~p", [Err, OtherDcID, ReqMsg, Retries]),
            send_interdc_lock_request(OtherDcID, ReqMsg, Retries - 1);
        Err ->
            logger:error("send_interdc_lock_request failed ~p ~p ~p", [Err, OtherDcID, ReqMsg]),
            ok
    end.

on_interdc_reply(BinaryResp, _RequestCacheEntry) ->
    logger:notice("on_interdc_reply"),
    spawn_link(fun() ->
        case catch binary_to_term(BinaryResp) of
            {ok, Locks, SnapshotTime} ->
                case on_receive_remote_locks(Locks, SnapshotTime) of
                    ok -> ok;
                    {error, Reason} ->
                        logger:error("on_interdc_reply error ~p", [Reason])
                end;
            Other ->
                logger:error("on_interdc_reply unhandled message: ~p", [Other])
        end
    end).


handle_request_locks_remote(#request_locks_remote{locks = Locks, timestamp = Timestamp, my_dc_id = RequesterDcId}, From, State) ->
    {Actions, NewState} = antidote_lock_server_state:new_remote_request(From, Timestamp, Locks, RequesterDcId, State),
    logger:notice("handle_request_locks_remote~n  State = ~p~n  Locks = ~p~n  Timestamp = ~p~n  RequesterDcId = ~p~n  Actions = ~p", [antidote_lock_server_state:print_state(State), Locks, antidote_lock_server_state:print_systemtime(Timestamp), RequesterDcId, antidote_lock_server_state:print_actions(Actions)]),
    run_actions(Actions, NewState),
    {noreply, NewState}.


-spec handoff_locks_to_other_dcs(antidote_locks:lock_spec(), dcid(), dcid(), snapshot_time(), integer()) -> {ok, snapshot_time()} | {error, reason()}.
handoff_locks_to_other_dcs(Locks, RequesterDcId, MyDcId, Snapshot, Retries) ->
    case antidote:start_transaction(Snapshot, []) of
        {error, Reason} ->
            logger:error("Could not start transaction:~n  ~p", [Reason]),
            {error, Reason};
        {ok, TxId} ->
            LockObjects = antidote_lock_crdt:get_lock_objects(Locks),
            {ok, LockValuesRaw} = antidote:read_objects(LockObjects, TxId),
            LockValues = [antidote_lock_crdt:parse_lock_value(L) || L <- LockValuesRaw],
            Updates = lists:flatmap(fun({{Lock, Kind}, LockValue1}) ->
                LockValue = maps:merge(#{MyDcId => MyDcId}, LockValue1),
                LocksToUpdate = case Kind of
                    shared ->
                        case maps:find(RequesterDcId, LockValue) of
                            {ok, MyDcId} ->
                                [{RequesterDcId, RequesterDcId}];
                            _ ->
                                []
                        end;
                    exclusive ->
                        [{K, RequesterDcId} || {K, V} <- maps:to_list(LockValue), V == MyDcId]
                end,
                antidote_lock_crdt:make_lock_updates(Lock, LocksToUpdate)
            end, lists:zip(Locks, LockValues)),
            logger:info("handoff_locks_to_other_dcs: update locks:~n  ~p", [Updates]),
            ok = antidote:update_objects(Updates, TxId),
            case antidote:commit_transaction(TxId) of
                {error, Reason} when Retries > 0 ->
                    logger:error("Handoff transaction to ~p failed with reason ~p, retrying ~p more times", [RequesterDcId, Reason, Retries]),
                    handoff_locks_to_other_dcs(Locks, RequesterDcId, MyDcId, Snapshot, Retries - 1);
                {error, Reason} ->
                    logger:error("Handoff transaction to ~p failed with reason ~p", [RequesterDcId, Reason]),
                    {error, Reason};
                {ok, Time} ->
                    logger:notice("Handoff transaction to ~p SUCCESS", [RequesterDcId]),
                    {ok, Time}
            end
    end.


-spec handle_release_locks(pid(), snapshot_time(), antidote_lock_server_state:state()) ->
    {reply, Resp, antidote_lock_server_state:state()}
    when Resp :: ok | {lock_error, reason()} | 'unrelated process'.
handle_release_locks(FromPid, CommitTime, State) ->
    case antidote_lock_server_state:is_lock_process(FromPid, State) of
        false -> {reply, 'unrelated process', State};
        true ->
            CheckResult = antidote_lock_server_state:check_release_locks(FromPid, State),
            {Actions, NewState} = antidote_lock_server_state:remove_locks(FromPid, CommitTime, State),
            logger:notice("handle_release_locks ~p~n  State = ~p~n  CommitTime = ~p~n  Actions = ~p", [FromPid, antidote_lock_server_state:print_state(State),antidote_lock_server_state:print_vc(CommitTime), antidote_lock_server_state:print_actions(Actions)]),
            run_actions(Actions, NewState),

            case CheckResult of
                {error, Reason} ->
                    {reply, {lock_error, Reason}, NewState};
                ok ->
                    {reply, ok, NewState}
            end
    end.

-spec run_actions(antidote_lock_server_state:actions(), antidote_lock_server_state:state()) -> ok.
run_actions(Actions, State) ->
    logger:notice("run_actions ~p", [antidote_lock_server_state:print_actions(Actions)]),
    {HandOverActions, LockRequestActions, Replies} = Actions,
    MyDcId = antidote_lock_server_state:my_dc_id(State),
    SnapshotTime = antidote_lock_server_state:get_snapshot_time(State),
    Self = self(),

    spawn_link(fun() ->
        % send to other data centers
        lists:foreach(fun({HandOver, RequesterDcId, Requester}) ->
            {ok, SnapshotTime2} = handoff_locks_to_other_dcs(HandOver, RequesterDcId, MyDcId, SnapshotTime, 5),
            logger:notice("antidote_lock_server: handover locks to ~p~n  HandOver = ~p~n SnapshotTime = ~p", [RequesterDcId, HandOver, antidote_lock_server_state:print_vc(SnapshotTime)]),
            gen_server:reply(Requester, {ok, HandOver, SnapshotTime2})
        end, HandOverActions),

        % send requests
        send_interdc_lock_requests(LockRequestActions, MyDcId),

        case maps:size(LockRequestActions) of
            0 -> ok;
            _ ->
                gen_server:cast(Self, start_interdc_lock_requests_timer)
        end,

        % reply to acquired locks
        lists:foreach(fun(R) ->
            Reply = {ok, SnapshotTime},
            logger:notice("antidote_lock_server: replying to ~p:~n  ~p", [R, Reply]),
            gen_server:reply(R, Reply)
        end, Replies)
    end),
    ok.


handle_on_receive_remote_locks(Locks, SnapshotTime, State) ->
    Self = self(),
    spawn_link(fun() ->
        LockObjects = antidote_lock_crdt:get_lock_objects(Locks),
        case antidote:read_objects(SnapshotTime, [], LockObjects) of
            {error, Reason} ->
                logger:error("handle_on_receive_remote_locks, could not read objects ~p~n~p", [Reason, LockObjects]),
                % ignore
                ok;
            {ok, LockValuesRaw, ReadClock} ->
                LockValues = [antidote_lock_crdt:parse_lock_value(V) || V <- LockValuesRaw],
                LockEntries = lists:zip(Locks, LockValues),
                logger:notice("handle_on_receive_remote_locks, new lock entries:~n~p", [LockEntries]),
                gen_server:cast(Self, #on_receive_remote_locks2{lock_entries = LockEntries, read_clock = ReadClock})
        end
    end),
    {reply, ok, State}.

handle_on_receive_remote_locks2(LockEntries, ReadClock, State) ->
    logger:notice("handle_on_receive_remote_locks2, new lock entries:~n~p", [LockEntries]),
    AllDcIds = dc_meta_data_utilities:get_dcs(),
    {Actions, NewState} = antidote_lock_server_state:on_remote_locks_received(ReadClock, AllDcIds, LockEntries, State),
    logger:notice("handle_on_receive_remote_locks~n  LockEntries = ~p~n  State = ~p~n  NewState = ~p~n  Actions = ~p", [LockEntries, antidote_lock_server_state:print_state(State), antidote_lock_server_state:print_state(NewState), antidote_lock_server_state:print_actions(Actions)]),
    run_actions(Actions, NewState),
    {noreply, NewState}.


-ifdef(TEST).

simple_test() ->
    ok.

-endif.

