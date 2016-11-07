-module(log_parser).
-include("antidote.hrl").

%% API exports
-export([main/1]).

%%====================================================================
%% API functions
%%====================================================================

%% escript Entry point
main(Args) ->
    io:format("Args: ~p~n", [Args]),
    [In,Out] = Args,
    init(In, Out),
    erlang:halt(0).

%%====================================================================
%% Internal functions
%%====================================================================

init(InFile, OutFile) ->
  case disk_log:open([{file, InFile}, {name, "data"}, {mode, read_only}]) of
  {ok, Log} ->
        {ok, Out} = file:open(OutFile, [write]),
        process(Log, Out),
        file:close(Out),
        disk_log:close(Log);
  Other ->
    io:format("Could not open file: ~p", [Other]),
    halt(1)
end.

process(Log, Out) ->
  {eof, Records} = read_log(Log, start, []),
  LogRecords = lists:map(fun({_, LogRecord}) -> LogRecord end,
                         Records),
  {Txns, _State} = log_txn_assembler:process_all(LogRecords, log_txn_assembler:new_state()),
  Ops = get_ops(Txns),
  write_to_file(Ops, Out).

write_to_file(Ops, Out) ->
  %%Ops is list of list of ops = [[op]], so flatten it before printing.
  FOps = lists:flatten(Ops),
  lists:foreach(fun(Op) ->
                    file:write(Out, io_lib:write(Op)),
                    file:write(Out, io_lib:fwrite("~n",[]))
                end, FOps).

%% read_log gives a list of logrecords
read_log(_Log, error, Ops) ->
  {error, Ops};
read_log(_Log, eof, Ops) ->
  {eof, Ops};
read_log(Log, Continuation, Ops) ->
  {NewContinuation, NewOps} =
        case disk_log:chunk(Log, Continuation) of
            {C, O} -> {C,O};
            {C, O, _} -> {C,O};
            eof -> {eof, []}
        end,
  read_log(Log, NewContinuation, Ops ++ NewOps).


%% Given a list of Txns, where each transaction is a list of records of update,read,commit,
%% get_ops(Txns) extracts updates and read records, trimming away all unnecessary information about
%% transactions.
%% update() = {update, key(), CommitTime::vectorclock(), dcid()}
%% read()  = {read, key(), SnapshotTime::vectorclock(), dcid()}\
%% op() = update() | read()
%% getops(Txns) -> [[op()]].
get_ops(Txns) ->
  lists:map(fun(Txn) ->
      get_ops_from_txn(Txn)
    end,
    Txns).

get_ops_from_txn(Txn) ->
   {Dc, CommitTime} = get_commit_time(Txn),
   SnapshotTime = get_snapshot_time(Txn),
   Ops = lists:filtermap(fun(LogRecord) ->
                        re_assemble_op(LogRecord, Dc, CommitTime, SnapshotTime)
                      end,
                      Txn
     ),
     Ops.

re_assemble_op(LogRecord, DcId, CommitTime, SnapshotTime) ->
    Payload = LogRecord#log_record.log_operation,
    case Payload#log_operation.op_type of
      commit -> false;
      abort -> false;
      prepare -> false;
      update ->
        UpdateOp = Payload#log_operation.log_payload,
        Key = UpdateOp#update_log_payload.key,
        Op = {update, Key, CommitTime, DcId},
        {true, Op};
      read ->
        ReadOp = Payload#log_operation.log_payload,
        Key = ReadOp#read_log_payload.key,
        Op = {read, Key, SnapshotTime, DcId},
        {true, Op}
    end.

get_commit_time(Txn) ->
  CommitLogRecord = lists:last(Txn),
  CommitRecord = CommitLogRecord#log_record.log_operation#log_operation.log_payload,
  case CommitRecord of
    C when is_record(C, commit_log_payload) ->
      {Dc, CT} = CommitRecord#commit_log_payload.commit_time,
      SnapshotTime = CommitRecord#commit_log_payload.snapshot_time,
      CommitTime = dict:store(Dc, CT, SnapshotTime),
      {Dc, dict:to_list(CommitTime)};
    R when is_record(R, read_log_payload) ->
      {dcid, ignore}
  end.

get_snapshot_time(Txn) ->
  CommitLogRecord = lists:last(Txn),
  CommitRecord = CommitLogRecord#log_record.log_operation#log_operation.log_payload,
  SnapshotTime = case CommitRecord of
                      C when is_record(C, commit_log_payload) ->
                          CommitRecord#commit_log_payload.snapshot_time;
                      R when is_record(R, read_log_payload) ->
                          CommitRecord#read_log_payload.snapshot_time
                 end,
  dict:to_list(SnapshotTime).
