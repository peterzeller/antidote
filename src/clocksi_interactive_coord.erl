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

%% @doc The coordinator for a given Clock SI interactive transaction.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. When a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(clocksi_interactive_coord).

-behavior(gen_statem).

-include("antidote.hrl").


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DC_META_UTIL, mock_partition).
-define(DC_UTIL, mock_partition).
-define(VECTORCLOCK, mock_partition).
-define(LOG_UTIL, mock_partition).
-define(CLOCKSI_VNODE, mock_partition).
-define(CLOCKSI_DOWNSTREAM, mock_partition).
-define(LOGGING_VNODE, mock_partition).
-define(PROMETHEUS_GAUGE, mock_partition).
-define(PROMETHEUS_COUNTER, mock_partition).

-else.
-define(DC_META_UTIL, dc_meta_data_utilities).
-define(DC_UTIL, dc_utilities).
-define(VECTORCLOCK, vectorclock).
-define(LOG_UTIL, log_utilities).
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(CLOCKSI_DOWNSTREAM, clocksi_downstream).
-define(LOGGING_VNODE, logging_vnode).
-define(PROMETHEUS_GAUGE, prometheus_gauge).
-define(PROMETHEUS_COUNTER, prometheus_counter).
-endif.


%% API
-export([
    start_link/1,
    start_link/2,
    start_link/3,
    start_link/4,
    start_link/5,
    generate_name/1,
    perform_singleitem_operation/4,
    perform_singleitem_update/5,
    finish_op/3
]).

%% gen_statem callbacks
-export([
    init/1,
    code_change/4,
    callback_mode/0,
    terminate/3,
    stop/1
]).

%% states
-export([


    receive_committed/3,
    receive_logging_responses/3,
    receive_read_objects_result/3,
    receive_aborted/3,
    single_committing/3,
    receive_prepared/3,
    execute_op/3,
    start_tx/3,

    committing/3,
    committing_single/3
    , reply_to_client/1]).

%%%===================================================================
%%% API
%%%===================================================================

%% used in cure.erl
-spec generate_name(pid()) -> atom().
generate_name(From) -> list_to_atom(pid_to_list(From) ++ "interactive_cord").

-spec start_link(pid(), clock_time() | ignore, txn_properties(), boolean(), [op_param()]) -> {ok, pid()}.
start_link(From, Clientclock, Properties, StayAlive, Operations) ->
    case StayAlive of
        true ->
            gen_statem:start_link({local, generate_name(From)}, ?MODULE, [From, Clientclock, Properties, StayAlive, Operations], []);
        false ->
            gen_statem:start_link(?MODULE, [From, Clientclock, Properties, StayAlive, Operations], [])
    end.

-spec start_link(pid(), clock_time() | ignore, txn_properties(), boolean()) -> {ok, pid()}.
start_link(From, Clientclock, Properties, StayAlive) ->
    case StayAlive of
        true ->
            gen_statem:start_link({local, generate_name(From)}, ?MODULE, [From, Clientclock, Properties, StayAlive], []);
        false ->
            gen_statem:start_link(?MODULE, [From, Clientclock, Properties, StayAlive], [])
    end.

-spec start_link(pid(), clock_time() | ignore) -> {ok, pid()}.
start_link(From, Clientclock) ->
    start_link(From, Clientclock, antidote:get_default_txn_properties()).

-spec start_link(pid(), clock_time() | ignore, txn_properties()) -> {ok, pid()}.
start_link(From, Clientclock, Properties) ->
    start_link(From, Clientclock, Properties, false).

-spec start_link(pid()) -> {ok, pid()}.
start_link(From) ->
    start_link(From, ignore, antidote:get_default_txn_properties()).

%% TODO spec
stop(Pid) -> gen_statem:stop(Pid).

%% @doc This is a standalone function for directly contacting the read
%%      server located at the vnode of the key being read.  This read
%%      is supposed to be light weight because it is done outside of a
%%      transaction fsm and directly in the calling thread.
%%      It either returns the object value or the object state.
-spec perform_singleitem_operation(snapshot_time() | ignore, key(), type(), clocksi_readitem_server:read_property_list()) ->
    {ok, val() | term(), snapshot_time()} | {error, reason()}.
perform_singleitem_operation(Clock, Key, Type, Properties) ->
    Transaction = create_transaction_record(Clock, false, undefined, true, Properties),
    %%OLD: {Transaction, _TransactionId} = create_transaction_record(ignore, update_clock, false, undefined, true),
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case clocksi_readitem_server:read_data_item(IndexNode, Key, Type, Transaction, []) of
        {error, Reason} ->
            {error, Reason};
        {ok, Snapshot} ->
            %% Read only transaction has no commit, hence return the snapshot time
            CommitTime = Transaction#transaction.vec_snapshot_time,
            {ok, Snapshot, CommitTime}
    end.

%% @doc This is a standalone function for directly contacting the update
%%      server vnode.  This is lighter than creating a transaction
%%      because the update/prepare/commit are all done at one time
-spec perform_singleitem_update(snapshot_time() | ignore, key(), type(), {atom(), term()}, list()) -> {ok, {txid(), [], snapshot_time()}} | {error, term()}.
perform_singleitem_update(Clock, Key, Type, Params, Properties) ->
    Transaction = create_transaction_record(Clock, false, undefined, true, Properties),
    Partition = ?LOG_UTIL:get_key_partition(Key),
    %% Execute pre_commit_hook if any
    case antidote_hooks:execute_pre_commit_hook(Key, Type, Params) of
        {Key, Type, Params1} ->
            case ?CLOCKSI_DOWNSTREAM:generate_downstream_op(Transaction, Partition, Key, Type, Params1, []) of
                {ok, DownstreamRecord} ->
                    UpdatedPartitions = [{Partition, [{Key, Type, DownstreamRecord}]}],
                    TxId = Transaction#transaction.txn_id,
                    LogRecord = #log_operation{
                        tx_id=TxId,
                        op_type=update,
                        log_payload=#update_log_payload{key=Key, type=Type, op=DownstreamRecord}
                    },
                    LogId = ?LOG_UTIL:get_logid_from_key(Key),
                    case ?LOGGING_VNODE:append(Partition, LogId, LogRecord) of
                        {ok, _} ->
                            case ?CLOCKSI_VNODE:single_commit_sync(UpdatedPartitions, Transaction) of
                                {committed, CommitTime} ->

                                    %% Execute post commit hook
                                    case antidote_hooks:execute_post_commit_hook(Key, Type, Params1) of
                                        {error, Reason} ->
                                            logger:info("Post commit hook failed. Reason ~p", [Reason]);
                                        _ ->
                                            ok
                                    end,

                                    TxId = Transaction#transaction.txn_id,
                                    DcId = ?DC_META_UTIL:get_my_dc_id(),

                                    CausalClock = ?VECTORCLOCK:set(
                                        DcId,
                                        CommitTime,
                                        Transaction#transaction.vec_snapshot_time
                                    ),

                                    {ok, {TxId, [], CausalClock}};

                                abort ->
                                    % TODO increment aborted transaction metrics?
                                    {error, aborted};

                                {error, Reason} ->
                                    {error, Reason}
                            end;

                        Error ->
                            {error, Error}
                    end;

                {error, Reason} ->
                    {error, Reason}
            end;

        {error, Reason} ->
            {error, Reason}
    end.

%% TODO spec
finish_op(From, Key, Result) ->
    gen_statem:cast(From, {Key, Result}).

%%%===================================================================
%%% Internal State
%%%===================================================================

%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    txid: transaction id handled by this fsm, as defined in src/antidote.hrl.
%%    updated_partitions: the partitions where update operations take place.
%%    num_to_ack: when sending prepare_commit,
%%                number of partitions that have acknowledged.
%%    num_to_read: when sending read requests
%%                 number of partitions that are asked.
%%    prepare_time: transaction prepare time.
%%    commit_time: transaction commit time.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------

-record(coord_state, {
    from :: undefined | {pid(), term()} | pid(),
    transaction :: undefined | tx(),
    updated_partitions :: list(),
    client_ops :: list(), % list of upstream updates, used for post commit hooks
    num_to_ack :: non_neg_integer(),
    num_to_read :: non_neg_integer(),
    prepare_time :: undefined | clock_time(),
    commit_time :: undefined | clock_time(),
    commit_protocol :: term(),
    state :: active | prepared | committing
    | committed | committed_read_only
    | undefined | aborted,
    operations :: undefined | list() | {update_objects, list()},
    return_accumulator :: list() | ok | {error, reason()},
    is_static :: boolean(),
    full_commit :: boolean(),
    properties :: txn_properties(),
    stay_alive :: boolean(),
    locks :: undefined | antidote_locks:lock_spec()
}).

%%%===================================================================
%%% States
%%%===================================================================

%%%== init

%% @doc Initialize the state.
init([From, ClientClock, Properties, StayAlive]) ->
    BaseState = init_state(StayAlive, false, false, Properties),
    case start_tx_internal(From, ClientClock, Properties, BaseState) of
        {ok, State} ->
            {ok, execute_op, State};
        {error, Reason} ->
            {stop_and_reply, Reason, {reply, From, {error, Reason}}}
    end;


%% @doc Initialize static transaction with Operations.
init([From, ClientClock, Properties, StayAlive, Operations]) ->
    BaseState = init_state(StayAlive, true, true, Properties),
    case start_tx_internal(From, ClientClock, Properties, BaseState) of
        {ok, State} ->
            {ok, execute_op, State#coord_state{operations = Operations, from = From}, [{state_timeout, 0, timeout}]};
        {error, Reason} ->
            {stop_and_reply, Reason, {reply, From, {error, Reason}}}
    end.



%%%== execute_op

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
%% internal state timeout
-spec execute_op(state_timeout, timeout, #coord_state{}) -> gen_statem:event_handler_result(#coord_state{});
                ({call, pid()}, undefined | list() | {update_objects, list()} | {update, list()}, #coord_state{}) -> gen_statem:event_handler_result(#coord_state{}).
execute_op(state_timeout, timeout, State = #coord_state{operations = Operations, from = From}) ->
    execute_op({call, From}, Operations, State);

%% update kept for backwards compatibility with tests.
execute_op({call, Sender}, {update, Args}, State) ->
    execute_op({call, Sender}, {update_objects, [Args]}, State);

execute_op({call, Sender}, {OpType, Args}, State) ->
    execute_command(OpType, Args, Sender, State).

%%%== receive_prepared

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared(cast, {prepared, ReceivedPrepareTime}, State) ->
    process_prepared(ReceivedPrepareTime, State);

receive_prepared(cast, abort, State) ->
    receive_prepared(cast, timeout, State);

receive_prepared(cast, timeout, State) ->
    abort(State);

%% capture regular events (e.g. logging_vnode responses)
receive_prepared(info, {_EventType, EventValue}, State) ->
    receive_prepared(cast, EventValue, State).


%%%== start_tx

%% @doc TODO
start_tx(cast, {start_tx, From, ClientClock, Properties}, State) ->
    case start_tx_internal(From, ClientClock, Properties, State) of
        {ok, State2} ->
            {next_state, execute_op, State2};
        {error, Reason} ->
            {keep_state_and_data, {reply, From, {error, Reason}}}
    end;


%% Used by static update and read transactions
start_tx(cast, {start_tx, From, ClientClock, Properties, Operation}, State) ->
    case start_tx_internal(From, ClientClock, Properties,
            State#coord_state{is_static = true, operations = Operation, from = From}) of
        {ok, State2} ->
            {next_state, execute_op, State2, [{state_timeout, 0, timeout}]};
        {error, Reason} ->
            {keep_state_and_data, {reply, From, {error, Reason}}}
    end;


%% capture regular events (e.g. logging_vnode responses)
start_tx(info, {_EventType, EventValue}, State) ->
    start_tx(cast, EventValue, State).


%%%== committing

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected
committing({call, Sender}, commit, State = #coord_state{transaction = Transaction,
    updated_partitions = UpdatedPartitions,
    commit_time = Commit_time}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            reply_to_client(State#coord_state{state = committed_read_only, from = Sender});
        _ ->
            ok = ?CLOCKSI_VNODE:commit(UpdatedPartitions, Transaction, Commit_time),
            {next_state, receive_committed,
                State#coord_state{num_to_ack = NumToAck, from = Sender, state = committing}}
    end.

%%%== single_committing

%% @doc TODO
-spec single_committing(cast, {committed | clock_time()} | abort | timeout, #coord_state{}) -> gen_statem:event_handler_result(#coord_state{});
    (info, {any(), any()}, #coord_state{}) -> gen_statem:event_handler_result(#coord_state{}).
single_committing(cast, {committed, CommitTime}, State = #coord_state{from = From, full_commit = FullCommit}) ->
    case FullCommit of
        false ->
            {next_state, committing_single, State#coord_state{commit_time = CommitTime, state = committing},
                [{reply, From, {ok, CommitTime}}]};
        true ->
            reply_to_client(State#coord_state{prepare_time = CommitTime, commit_time = CommitTime, state = committed})
    end;

single_committing(cast, abort, State) ->
    single_committing(cast, timeout, State);

single_committing(cast, timeout, State) ->
    abort(State);

%% capture regular events (e.g. logging_vnode responses)
single_committing(info, {_EventType, EventValue}, State) ->
    single_committing(cast, EventValue, State).


%%%== receive_aborted

%% @doc the fsm waits for acks indicating that each partition has successfully
%%      aborted the tx and finishes operation.
%%      Should we retry sending the aborted message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_aborted(cast, ack_abort, State = #coord_state{num_to_ack = NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(State#coord_state{state = aborted});
        _ ->
            {next_state, receive_aborted, State#coord_state{num_to_ack = NumToAck - 1}}
    end;

receive_aborted(cast, _, State) -> {next_state, receive_aborted, State};

%% capture regular events (e.g. logging_vnode responses)
receive_aborted(info, {_EventType, EventValue}, State) ->
    receive_aborted(cast, EventValue, State).


%%%== receive_read_objects_result

%% @doc After asynchronously reading a batch of keys, collect the responses here
receive_read_objects_result(cast, {ok, {Key, Type, Snapshot}}, CoordState = #coord_state{
    num_to_read = NumToRead,
    return_accumulator = ReadKeys
}) ->
    %% Apply local updates to the read snapshot
    UpdatedSnapshot = apply_tx_updates_to_snapshot(Key, CoordState, Type, Snapshot),

    %% Swap keys with their appropriate read values
    ReadValues = replace_first(ReadKeys, Key, UpdatedSnapshot),

    %% Loop back to the same state until we process all the replies
    case NumToRead > 1 of
        true ->
            {next_state, receive_read_objects_result, CoordState#coord_state{
                num_to_read = NumToRead - 1,
                return_accumulator = ReadValues
            }};

        false ->
            {next_state, execute_op, CoordState#coord_state{num_to_read = 0},
                [{reply, CoordState#coord_state.from, {ok, lists:reverse(ReadValues)}}]}
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_read_objects_result(info, {_EventType, EventValue}, State) ->
    receive_read_objects_result(cast, EventValue, State).


%%%== receive_logging_responses

%% internal state timeout
receive_logging_responses(state_timeout, timeout, State) ->
    receive_logging_responses(cast, timeout, State);
%% @doc This state reached after an execute_op(update_objects[Params]).
%% update_objects calls the perform_update function, which asynchronously
%% sends a log operation per update, to the vnode responsible of the updated
%% key. After sending all those messages, the coordinator reaches this state
%% to receive the responses of the vnodes.
receive_logging_responses(cast, Response, State = #coord_state{
    is_static = IsStatic,
    num_to_read = NumToReply,
    return_accumulator = ReturnAcc
}) ->

    NewAcc = case Response of
                 {error, _r} = Err -> Err;
                 {ok, _OpId} -> ReturnAcc;
                 timeout -> ReturnAcc
             end,

    %% Loop back to the same state until we process all the replies
    case NumToReply > 1 of
        true ->
            {next_state, receive_logging_responses, State#coord_state{
                num_to_read=NumToReply - 1,
                return_accumulator=NewAcc
            }};

        false ->
            case NewAcc of
                ok ->
                    case IsStatic of
                        true ->
                            prepare(State);
                        false ->
                            {next_state, execute_op, State#coord_state{num_to_read=0, return_accumulator=[]},
                                [{reply, State#coord_state.from, NewAcc}]}
                    end;

                _ ->
                    abort(State)
            end
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_logging_responses(info, {_EventType, EventValue}, State) ->
    receive_logging_responses(cast, EventValue, State).


%%%== receive_committed

%% @doc the fsm waits for acks indicating that each partition has successfully
%%      committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_committed(cast, committed, State = #coord_state{num_to_ack = NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(State#coord_state{state = committed});
        _ ->
            {next_state, receive_committed, State#coord_state{num_to_ack = NumToAck - 1}}
    end;

%% capture regular events (e.g. logging_vnode responses)
receive_committed(info, {_EventType, EventValue}, State) ->
    receive_committed(cast, EventValue, State).


%%%== committing_single

%% @doc There was only a single partition with an update in this transaction
%%      so the transaction has already been committed
%%      so just wait for the commit message from the client
committing_single({call, Sender}, commit, State = #coord_state{commit_time = Commit_time}) ->
    reply_to_client(State#coord_state{
        prepare_time = Commit_time,
        from = Sender,
        commit_time = Commit_time,
        state = committed
    }).

%% =============================================================================

%% TODO add to all state functions
%%handle_sync_event(stop, _From, _StateName, StateData) -> {stop, normal, ok, StateData}.

%%handle_call(From, stop, Data) ->
%%    {stop_and_reply, normal,  {reply, From, ok}, Data}.
%%
%%handle_info(Info, StateName, Data) ->
%%    {stop, {shutdown, {unexpected, Info, StateName}}, StateName, Data}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) -> ok.

callback_mode() -> state_functions.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc TODO
-spec init_state(boolean(), boolean(), boolean(), proplists:proplist()) -> #coord_state{}.
init_state(StayAlive, FullCommit, IsStatic, Properties) ->
    #coord_state{
        from = undefined,
        transaction = undefined,
        updated_partitions = [],
        client_ops = [],
        num_to_ack = 0,
        num_to_read = 0,
        prepare_time = 0,
        commit_time = undefined,
        commit_protocol = undefined,
        state = undefined,
        operations = undefined,
        return_accumulator = [],
        is_static = IsStatic,
        full_commit = FullCommit,
        properties = Properties,
        stay_alive = StayAlive,
        locks = undefined
    }.


%% @doc TODO
-spec start_tx_internal(pid(), snapshot_time(), proplists:proplist(), #coord_state{}) -> {ok, #coord_state{}} | {error, any()}.
start_tx_internal(From, ClientClock, Properties, State = #coord_state{stay_alive = StayAlive, is_static = IsStatic}) ->
    Locks = ordsets:from_list([{Lock, shared_lock} || Lock <- proplists:get_value(shared_locks, Properties, [])]
        ++ [{Lock, exclusive_lock} || Lock <- proplists:get_value(exclusive_locks, Properties, [])]),
    case antidote_locks:obtain_locks(ClientClock, Locks) of
        {error, Reason} ->
            {error, Reason};
        {ok, ClientClock2} ->
            TransactionRecord = create_transaction_record(ClientClock2, StayAlive, From, false, Properties),
            case IsStatic of
                true -> ok;
                false -> From ! {ok, TransactionRecord#transaction.txn_id}
            end,
            % a new transaction was started, increment metrics
            ?PROMETHEUS_GAUGE:inc(antidote_open_transactions),
            {ok, State#coord_state{transaction = TransactionRecord, num_to_read = 0, properties = Properties, locks = Locks}}
    end.


%% @doc TODO
-spec create_transaction_record(snapshot_time() | ignore,
    boolean(), pid() | undefined, boolean(), txn_properties()) -> tx().
%%noinspection ErlangUnresolvedFunction
create_transaction_record(ClientClock, StayAlive, From, _IsStatic, Properties) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    _Res = rand_compat:seed(erlang:phash2([node()]), erlang:monotonic_time(), erlang:unique_integer()),
    {ok, SnapshotTime} = case ClientClock of
                             ignore ->
                                 get_snapshot_time();
                             _ ->
                                 case antidote:get_txn_property(update_clock, Properties) of
                                     update_clock ->
                                         get_snapshot_time(ClientClock);
                                     no_update_clock ->
                                         {ok, ClientClock}
                                 end
                         end,
    DcId = ?DC_META_UTIL:get_my_dc_id(),
    LocalClock = ?VECTORCLOCK:get(DcId, SnapshotTime),
    Name = case StayAlive of
               true ->
                   generate_name(From);
               false ->
                   self()
           end,
    TransactionId = #tx_id{local_start_time = LocalClock, server_pid = Name},
    #transaction{snapshot_time_local = LocalClock,
        vec_snapshot_time = SnapshotTime,
        txn_id = TransactionId,
        properties = Properties}.


%% @doc Execute the commit protocol
-spec execute_command(atom(), term(), pid(), #coord_state{}) -> gen_statem:event_handler_result(#coord_state{}).
execute_command(prepare, Protocol, Sender, State0) ->
    State = State0#coord_state{from=Sender, commit_protocol=Protocol},
    prepare(State);

%% @doc Abort the current transaction
execute_command(abort, _Protocol, Sender, State) ->
    abort(State#coord_state{from=Sender});

%% @doc Perform a single read, synchronous
execute_command(read, {Key, Type}, Sender, State = #coord_state{
    transaction=Transaction,
    updated_partitions=UpdatedPartitions
}) ->
    case perform_read({Key, Type}, UpdatedPartitions, Transaction, Sender) of
        {error, _} ->
            abort(State);
        ReadResult ->
            {next_state, execute_op, State, {reply, Sender, {ok, ReadResult}}}
    end;

%% @doc Read a batch of objects, asynchronous
execute_command(read_objects, Objects, Sender, State = #coord_state{transaction=Transaction}) ->
    ExecuteReads = fun({Key, Type}, AccState) ->
        ?PROMETHEUS_COUNTER:inc(antidote_operations_total, [read_async]),
        Partition = ?LOG_UTIL:get_key_partition(Key),
        ok = clocksi_vnode:async_read_data_item(Partition, Transaction, Key, Type),
        ReadKeys = AccState#coord_state.return_accumulator,
        AccState#coord_state{return_accumulator=[Key | ReadKeys]}
                   end,

    NewCoordState = lists:foldl(
        ExecuteReads,
        State#coord_state{num_to_read = length(Objects), return_accumulator=[]},
        Objects
    ),

    {next_state, receive_read_objects_result, NewCoordState#coord_state{from=Sender}};

%% @doc Perform update operations on a batch of Objects
execute_command(update_objects, UpdateOps, Sender, State = #coord_state{transaction=Transaction}) ->
    ExecuteUpdates = fun(Op, AccState=#coord_state{
        client_ops = ClientOps0,
        updated_partitions = UpdatedPartitions0
    }) ->
        case perform_update(Op, UpdatedPartitions0, Transaction, Sender, ClientOps0) of
            {error, _} = Err ->
                AccState#coord_state{return_accumulator = Err};

            {UpdatedPartitions, ClientOps} ->
                NumToRead = AccState#coord_state.num_to_read,
                AccState#coord_state{
                    client_ops=ClientOps,
                    num_to_read=NumToRead + 1,
                    updated_partitions=UpdatedPartitions
                }
        end
                     end,

    NewCoordState = lists:foldl(
        ExecuteUpdates,
        State#coord_state{num_to_read=0, return_accumulator=ok},
        UpdateOps
    ),

    LoggingState = NewCoordState#coord_state{from=Sender},
    case LoggingState#coord_state.num_to_read > 0 of
        true ->
            {next_state, receive_logging_responses, LoggingState};
        false ->
            {next_state, receive_logging_responses, LoggingState, [{state_timeout, 0, timeout}]}
    end.


%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
reply_to_client(State = #coord_state{
    from=From,
    state=TxState,
    is_static=IsStatic,
    stay_alive=StayAlive,
    client_ops=ClientOps,
    commit_time=CommitTime,
    full_commit=FullCommit,
    transaction=Transaction,
    return_accumulator=ReturnAcc
}) ->
    TxId = Transaction#transaction.txn_id,
    case From of
        undefined ->
            ok;

        Node ->

            Reply = case TxState of
                        committed_read_only ->
                            case IsStatic of
                                false ->
                                    {ok, {TxId, Transaction#transaction.vec_snapshot_time}};
                                true ->
                                    {ok, {TxId, ReturnAcc, Transaction#transaction.vec_snapshot_time}}
                            end;

                        committed ->
                            %% Execute post_commit_hooks
                            _Result = execute_post_commit_hooks(ClientOps),
                            %% TODO: What happens if commit hook fails?
                            DcId = ?DC_META_UTIL:get_my_dc_id(),
                            CausalClock = ?VECTORCLOCK:set(DcId, CommitTime, Transaction#transaction.vec_snapshot_time),
                            case IsStatic of
                                false ->
                                    {ok, {TxId, CausalClock}};
                                true ->
                                    {ok, CausalClock}
                            end;

                        aborted ->
                            ?PROMETHEUS_COUNTER:inc(antidote_aborted_transactions_total),
                            case ReturnAcc of
                                {error, Reason} ->
                                    {error, Reason};
                                _ ->
                                    {error, {aborted, TxId}}
                            end

                        %% can never match (dialyzer)
%%                        Reason ->
%%                            {TxId, Reason}
                    end,
            case is_pid(Node) of
                false ->
                    gen_statem:reply(Node, Reply);
                true ->
                    From ! Reply
            end
    end,

    % transaction is finished, decrement count
    ?PROMETHEUS_GAUGE:dec(antidote_open_transactions),

    case StayAlive of
        true ->
            {next_state, start_tx, init_state(StayAlive, FullCommit, IsStatic, [])};
        false ->
            {stop, normal, State}
    end.


%% @doc The following function is used to apply the updates that were performed by the running
%% transaction, to the result returned by a read.
-spec apply_tx_updates_to_snapshot (key(), #coord_state{}, type(), snapshot()) -> snapshot().
apply_tx_updates_to_snapshot(Key, CoordState, Type, Snapshot)->
    Partition = ?LOG_UTIL:get_key_partition(Key),
    Found = lists:keyfind(Partition, 1, CoordState#coord_state.updated_partitions),

    case Found of
        false ->
            Snapshot;

        {Partition, WS} ->
            FilteredAndReversedUpdates=clocksi_vnode:reverse_and_filter_updates_per_key(WS, Key),
            clocksi_materializer:materialize_eager(Type, Snapshot, FilteredAndReversedUpdates)
    end.


%%@doc Set the transaction Snapshot Time to the maximum value of:
%%     1.ClientClock, which is the last clock of the system the client
%%       starting this transaction has seen, and
%%     2.machine's local time, as returned by erlang:now().
-spec get_snapshot_time(snapshot_time()) -> {ok, snapshot_time()}.
get_snapshot_time(ClientClock) ->
    wait_for_clock(ClientClock).


-spec get_snapshot_time() -> {ok, snapshot_time()}.
get_snapshot_time() ->
    Now = dc_utilities:now_microsec() - ?OLD_SS_MICROSEC,
    {ok, VecSnapshotTime} = ?DC_UTIL:get_stable_snapshot(),
    DcId = ?DC_META_UTIL:get_my_dc_id(),
    SnapshotTime = vectorclock:set(DcId, Now, VecSnapshotTime),
    {ok, SnapshotTime}.


-spec wait_for_clock(snapshot_time()) -> {ok, snapshot_time()}.
wait_for_clock(Clock) ->
    {ok, VecSnapshotTime} = get_snapshot_time(),
    case vectorclock:ge(VecSnapshotTime, Clock) of
        true ->
            %% No need to wait
            {ok, VecSnapshotTime};
        false ->
            %% wait for snapshot time to catch up with Client Clock
            %TODO Refactor into constant
            timer:sleep(10),
            wait_for_clock(Clock)
    end.


%% Replaces the first occurrence of an entry;
%% yields error if there the element to be replaced is not in the list
replace_first([], _, _) ->
    error;

replace_first([Key|Rest], Key, NewKey) ->
    [NewKey|Rest];

replace_first([NotMyKey|Rest], Key, NewKey) ->
    [NotMyKey|replace_first(Rest, Key, NewKey)].


perform_read({Key, Type}, UpdatedPartitions, Transaction, Sender) ->
    ?PROMETHEUS_COUNTER:inc(antidote_operations_total, [read]),
    Partition = ?LOG_UTIL:get_key_partition(Key),

    WriteSet = case lists:keyfind(Partition, 1, UpdatedPartitions) of
                   false ->
                       [];
                   {Partition, WS} ->
                       WS
               end,

    case ?CLOCKSI_VNODE:read_data_item(Partition, Transaction, Key, Type, WriteSet) of
        {ok, Snapshot} ->
            Snapshot;

        {error, Reason} ->
            case Sender of
                undefined -> ok;
                _ -> gen_statem:reply(Sender, {error, Reason})
            end,
            {error, Reason}
    end.


perform_update(Op, UpdatedPartitions, Transaction, _Sender, ClientOps) ->
    ?PROMETHEUS_COUNTER:inc(antidote_operations_total, [update]),
    {Key, Type, Update} = Op,
    Partition = ?LOG_UTIL:get_key_partition(Key),

    WriteSet = case lists:keyfind(Partition, 1, UpdatedPartitions) of
                   false ->
                       [];
                   {Partition, WS} ->
                       WS
               end,

    %% Execute pre_commit_hook if any
    case antidote_hooks:execute_pre_commit_hook(Key, Type, Update) of
        {error, Reason} ->
            logger:debug("Execute pre-commit hook failed ~p", [Reason]),
            {error, Reason};

        {Key, Type, PostHookUpdate} ->

            %% Generate the appropriate state operations based on older snapshots
            GenerateResult = ?CLOCKSI_DOWNSTREAM:generate_downstream_op(
                Transaction,
                Partition,
                Key,
                Type,
                PostHookUpdate,
                WriteSet
            ),

            case GenerateResult of
                {error, Reason} ->
                    {error, Reason};

                {ok, DownstreamOp} ->
                    ok = async_log_propagation(Partition, Transaction#transaction.txn_id, Key, Type, DownstreamOp),

                    %% Append to the write set of the updated partition
                    GeneratedUpdate = {Key, Type, DownstreamOp},
                    NewUpdatedPartitions = append_updated_partitions(
                        UpdatedPartitions,
                        WriteSet,
                        Partition,
                        GeneratedUpdate
                    ),

                    UpdatedOps = [{Key, Type, PostHookUpdate} | ClientOps],
                    {NewUpdatedPartitions, UpdatedOps}
            end
    end.

%% @doc Add new updates to the write set of the given partition.
%%
%%      If there's no write set, create a new one.
%%
append_updated_partitions(UpdatedPartitions, [], Partition, Update) ->
    [{Partition, [Update]} | UpdatedPartitions];

append_updated_partitions(UpdatedPartitions, WriteSet, Partition, Update) ->
    %% Update the write set entry with the new record
    AllUpdates = {Partition, [Update | WriteSet]},
    lists:keyreplace(Partition, 1, UpdatedPartitions, AllUpdates).


-spec async_log_propagation(index_node(), txid(), key(), type(), effect()) -> ok.
async_log_propagation(Partition, TxId, Key, Type, Record) ->
    LogRecord = #log_operation{
        op_type=update,
        tx_id=TxId,
        log_payload=#update_log_payload{key=Key, type=Type, op=Record}
    },

    LogId = ?LOG_UTIL:get_logid_from_key(Key),
    ?LOGGING_VNODE:asyn_append(Partition, LogId, LogRecord, {fsm, undefined, self()}).


%% @doc this function sends a prepare message to all updated partitions and goes
%%      to the "receive_prepared"state.
-spec prepare(#coord_state{}) -> gen_statem:event_handler_result(#coord_state{}).
prepare(State = #coord_state{
    num_to_read=NumToRead,
    full_commit=FullCommit,
    transaction=Transaction,
    updated_partitions=UpdatedPartitions,
    commit_protocol = CommitProtocol
}) ->
    case UpdatedPartitions of
        [] ->
            if
                CommitProtocol == two_phase orelse NumToRead == 0 -> %TODO explain this condition, it makes no sense
                    case FullCommit of
                        true ->
                            prepare_done(State, commit_read_only);
                        false ->
                            Transaction = State#coord_state.transaction,
                            SnapshotTimeLocal = Transaction#transaction.snapshot_time_local,
                            prepare_done(State, {reply_and_then_commit, SnapshotTimeLocal})
                    end;
                true ->
                    {next_state, receive_prepared, State#coord_state{state = prepared}}
            end;

        [_] when CommitProtocol /= two_phase ->
            prepare_done(State, single_committing);
        [_|_] ->
            ok = ?CLOCKSI_VNODE:prepare(UpdatedPartitions, Transaction),
            Num_to_ack = length(UpdatedPartitions),
            {next_state, receive_prepared, State#coord_state{num_to_ack = Num_to_ack, state = prepared}}
    end.



%% This function is called when we are done with the prepare phase.
%% There are different options to continue the commit phase:
%% single_committing: special case for when we just touched a single partition
%% commit_read_only: special case for when we have not updated anything
%% {reply_and_then_commit, clock_time()}: first reply that we have successfully committed and then try to commit TODO rly?
%% {normal_commit, clock_time(): wait until all participants have acknowledged the commit and then reply to the client
-spec prepare_done(#coord_state{}, Action) -> gen_statem:event_handler_result(#coord_state{})
    when Action :: single_committing | commit_read_only | {reply_and_then_commit, clock_time()} | {normal_commit, clock_time()}.
prepare_done(State1, Action) ->
    case before_commit_checks(State1) of
        {error, Reason} ->
            abort(State1#coord_state{return_accumulator = {error, Reason}});
        {ok, State} ->
            case Action of
                single_committing ->
                    UpdatedPartitions = State#coord_state.updated_partitions,
                    Transaction = State#coord_state.transaction,
                    ok = ?CLOCKSI_VNODE:single_commit(UpdatedPartitions, Transaction),
                    {next_state, single_committing, State#coord_state{state = committing, num_to_ack = 1}};
                commit_read_only ->
                    reply_to_client(State#coord_state{state = committed_read_only});
                {reply_and_then_commit, CommitSnapshotTime} ->
                    From = State#coord_state.from,
                    {next_state, committing, State#coord_state{
                        state = committing,
                        commit_time = CommitSnapshotTime},
                        [{reply, From, {ok, CommitSnapshotTime}}]};
                {normal_commit, MaxPrepareTime} ->
                    UpdatedPartitions = State#coord_state.updated_partitions,
                    Transaction = State#coord_state.transaction,
                    ok = ?CLOCKSI_VNODE:commit(UpdatedPartitions, Transaction, MaxPrepareTime),
                    {next_state, receive_committed,
                        State#coord_state{
                            num_to_ack = length(UpdatedPartitions),
                            commit_time = MaxPrepareTime,
                            state = committing}}
            end
    end.




%% Checks executed before starting the commit phase.
-spec before_commit_checks(#coord_state{}) -> {ok, #coord_state{}} | {error, any()}.
before_commit_checks(State) ->
    Transaction = State#coord_state.transaction,
    CommitSnapshot =
        case State#coord_state.updated_partitions of
            [] ->
                Transaction#transaction.vec_snapshot_time;
            _ ->
                CommitTime = State#coord_state.commit_time,
                DcId = ?DC_META_UTIL:get_my_dc_id(),
                ?VECTORCLOCK:set_clock_of_dc(DcId, CommitTime, Transaction#transaction.vec_snapshot_time)
        end,
    Locks = State#coord_state.locks,
    case antidote_locks:release_locks(CommitSnapshot, Locks) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.


process_prepared(ReceivedPrepareTime, State = #coord_state{num_to_ack = NumToAck,
    full_commit = FullCommit,
    prepare_time = PrepareTime}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of
        1 ->
            % this is the last ack we expected
            case FullCommit of
                true ->
                    prepare_done(State, {normal_commit, MaxPrepareTime});
                false ->
                    prepare_done(State, {reply_and_then_commit, MaxPrepareTime})
            end;
        _ ->
            {next_state, receive_prepared, State#coord_state{num_to_ack = NumToAck - 1, prepare_time = MaxPrepareTime}}
    end.


%% @doc when an error occurs or an updated partition
%% does not pass the certification check, the transaction aborts.
abort(State = #coord_state{transaction = Transaction,
    updated_partitions = UpdatedPartitions}) ->
    NumToAck = length(UpdatedPartitions),
    case NumToAck of
        0 ->
            reply_to_client(State#coord_state{state = aborted});
        _ ->
            ok = ?CLOCKSI_VNODE:abort(UpdatedPartitions, Transaction),
            {next_state, receive_aborted, State#coord_state{num_to_ack = NumToAck, state = aborted}}
    end.


execute_post_commit_hooks(Ops) ->
    lists:foreach(fun({Key, Type, Update}) ->
        case antidote_hooks:execute_post_commit_hook(Key, Type, Update) of
            {error, Reason} ->
                logger:info("Post commit hook failed. Reason ~p", [Reason]);
            _ -> ok
        end
                  end, lists:reverse(Ops)).

%%%===================================================================
%%% Unit Tests
%%%===================================================================

-ifdef(TEST).

main_test_() ->
    {foreach,
        fun setup/0,
        fun cleanup/1,
        [
            fun empty_prepare_test/1,
            fun timeout_test/1,

            fun update_single_abort_test/1,
            fun update_single_success_test/1,
            fun update_multi_abort_test1/1,
            fun update_multi_abort_test2/1,
            fun update_multi_success_test/1,

            fun read_single_fail_test/1,
            fun read_success_test/1,

            fun downstream_fail_test/1,
            fun get_snapshot_time_test/0,
            fun wait_for_clock_test/0
        ]}.

% Setup and Cleanup
setup() ->
    {ok, Pid} = clocksi_interactive_coord:start_link(self(), ignore),
    Pid.

cleanup(Pid) ->
    case process_info(Pid) of undefined -> io:format("Already cleaned");
        _ -> clocksi_interactive_coord:stop(Pid) end.

empty_prepare_test(Pid) ->
    fun() ->
        ?assertMatch({ok, _}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

timeout_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {timeout, nothing, nothing}}, infinity)),
        ?assertMatch({error, {aborted , _}}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_single_abort_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({error, {aborted , _}}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_single_success_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {single_commit, nothing, nothing}}, infinity)),
        ?assertMatch({ok, _}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test1(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({error, {aborted , _}}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test2(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {fail, nothing, nothing}}, infinity)),
        ?assertMatch({error, {aborted , _}}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

update_multi_success_test(Pid) ->
    fun() ->
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertEqual(ok, gen_statem:call(Pid, {update, {success, nothing, nothing}}, infinity)),
        ?assertMatch({ok, _}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

read_single_fail_test(Pid) ->
    fun() ->
        ?assertEqual({error, mock_read_fail},
            gen_statem:call(Pid, {read, {read_fail, nothing}}, infinity))
    end.

read_success_test(Pid) ->
    fun() ->
        {ok, State} = gen_statem:call(Pid, {read, {counter, antidote_crdt_counter_pn}}, infinity),
        ?assertEqual({ok, 2},
            {ok, antidote_crdt_counter_pn:value(State)}),
        ?assertEqual({ok, [a]},
            gen_statem:call(Pid, {read, {set, antidote_crdt_set_go}}, infinity)),
        ?assertEqual({ok, mock_value},
            gen_statem:call(Pid, {read, {mock_type, mock_partition_fsm}}, infinity)),
        ?assertMatch({ok, _}, gen_statem:call(Pid, {prepare, empty}, infinity))
    end.

downstream_fail_test(Pid) ->
    fun() ->
        ?assertMatch({error, _},
            gen_statem:call(Pid, {update, {downstream_fail, nothing, nothing}}, infinity))
    end.


get_snapshot_time_test() ->
    {ok, SnapshotTime} = get_snapshot_time(),
    ?assertMatch([{mock_dc, _}], vectorclock:to_list(SnapshotTime)).

wait_for_clock_test() ->
    {ok, SnapshotTime} = wait_for_clock(vectorclock:from_list([{mock_dc, 10}])),
    ?assertMatch([{mock_dc, _}], vectorclock:to_list(SnapshotTime)),
    VecClock = dc_utilities:now_microsec(),
    {ok, SnapshotTime2} = wait_for_clock(vectorclock:from_list([{mock_dc, VecClock}])),
    ?assertMatch([{mock_dc, _}], vectorclock:to_list(SnapshotTime2)).

-endif.
