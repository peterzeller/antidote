-module(antidote_pb_handler).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

-type request() :: {MsgCode :: non_neg_integer(), ProtoMsg :: binary()}.

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts) ->
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, <<>>).

loop(Socket, Transport, Buffer) ->
    case Transport:recv(Socket, 0, 30000) of
        {ok, Data} ->
            Buffer2 = <<Buffer/binary, Data/binary>>,
            {Requests, BufferRest} = split(Buffer2, []),
            [handle(Socket, Transport, R) || R <- Requests],
            loop(Socket, Transport, BufferRest);
        _ ->
            ok = Transport:close(Socket)
    end.


% splits a buffer into a list of requests and the remaining buffer
-spec split(binary(), [request()]) -> {[request()], binary()}.
split(Buffer, Acc) ->
    case Buffer of
        <<Size:32, Rest/binary>> ->
            case Rest of
                <<Msg:Size/binary, Rest2/binary>> ->
                    <<MsgCode:8, ProtoBufMsg/bits>> = Msg,
                    % continue reading remaining messages
                    split(Rest2, [{MsgCode, ProtoBufMsg} | Acc]);
                _ -> % message not complete yet:
                    {Acc, Buffer}
            end;
        _ -> % incomplete message (not even has size)
            {Acc, Buffer}
    end.

% handles a single request
-spec handle(_Socket, _Transport, request()) -> ok.
handle(Socket, Transport, Request) ->
    {MsgCode, ProtoBufMsg} = Request,
    Decoded = antidote_pb_codec:decode_msg(MsgCode, ProtoBufMsg),
    case antidote_pb_txn:process(Decoded, <<>>) of
        {reply, Response, _State} ->
            Message = antidote_pb_codec:encode_msg(Response),
            Size = iolist_size(Message),
            ok = Transport:send(Socket, [<<Size:32>> | Message]),
            ok
    end.
