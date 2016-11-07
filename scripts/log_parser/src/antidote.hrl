-record(commit_log_payload, {commit_time :: dc_and_commit_time(),
			     snapshot_time :: snapshot_time()
			    }).

-record(update_log_payload, {key :: key(),
			     bucket :: bucket(),
			     type :: type(),
			     op :: op()
			    }).

-record(read_log_payload, {key :: key(),
			     bucket :: bucket(),
			     type :: type(),
			     snapshot_time :: snapshot_time()
			    }).

-record(abort_log_payload, {}).

-record(prepare_log_payload, {prepare_time :: non_neg_integer()}).

-type any_log_payload() :: #update_log_payload{} | #commit_log_payload{} | #abort_log_payload{} | #prepare_log_payload{}.

-record(log_operation, {
	  tx_id :: txid(),
	  op_type :: update | prepare | commit | abort | noop,
	  log_payload :: #commit_log_payload{}| #update_log_payload{} | #abort_log_payload{} | #prepare_log_payload{}}).
-record(op_number, {
  % TODO 19 undefined is required here, because of the use in inter_dc_log_sender_vnode. The use there should be refactored.
  node :: undefined | {node(),dcid()},
  global :: undefined | non_neg_integer(),
  local :: undefined | non_neg_integer()}).

%% The way records are stored in the log.
-record(log_record,{
	  version :: non_neg_integer(), %% The version of the log record, for backwards compatability
	  op_number :: #op_number{},
	  bucket_op_number :: #op_number{},
	  log_operation :: #log_operation{}}).

-type bucket() :: term().
-type downstream_record() :: term(). %% For this library type of downstream record doesn't matter
-type key() :: term().
-type op()  :: {update | merge, downstream_record()}.
-type type() :: atom().
-type dcid() :: 'undefined' | {atom(),tuple()}. %% TODO, is this the only structure that is returned by riak_core_ring:cluster_name(Ring)?
-type snapshot_time() ::  vectorclock().
-type clock_time() :: non_neg_integer().
-type dc_and_commit_time() ::  {dcid(), clock_time()}.

-record(tx_id, {local_start_time :: clock_time(),
                server_pid :: pid()}).
-type vectorclock() :: dict:dict().
-type txid() :: #tx_id{}.

-export_type([key/0, op/0, type/0, dcid/0, snapshot_time/0, dc_and_commit_time/0]).
