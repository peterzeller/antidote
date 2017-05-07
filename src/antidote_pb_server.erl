%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(antidote_pb_server).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
  code_change/3, terminate/2]).


-define(TIME, 800).
-define(EXP, 50).

-record(state, {
  messages = queue:new(),
  socket % the current socket
}).


start_link(Socket) ->
  gen_server:start_link(?MODULE, Socket, []).

init(Socket) ->
  gen_server:cast(self(), accept),
  {ok, #state{socket=Socket}}.

%% never called
handle_call(_E, _From, State) ->
  {noreply, State}.


handle_cast(accept, S = #state{socket=ListenSocket}) ->
  {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
  % start a new socket accepting other connections
  antidote_pb_server_sup:start_socket(),
  send(AcceptSocket, "What's your character's name?", []),
  {noreply, S#state{socket=AcceptSocket}};
handle_cast(process, S) ->
  {Val, NewMessages} = queue:out(S#state.messages),
  case Val of
    {value, Msg} ->
      % use decode_packet: decodes packet with 4 bytes at the beginning denoting the length
      case erlang:decode_packet(4, Msg, []) of
        {ok, <<MsgCode:8, MsgData/binary>>, Rest} ->
          ok;
        {ok, Binary, Rest} ->
          ok;
        {more, _Length} ->
          ok;
        {error, Reason} ->
          ok
      end;
    empty ->
      ok
  end,
  {noreply, S}.

handle_info({tcp, _Socket, Msg}, S) ->
  % add message to queue and call process (async)
  gen_server:cast(self(), process),
  NewState = S#state{
    messages = queue:in(Msg, S#state.messages)
  },
  {noreply, NewState};
handle_info({tcp_closed, _Socket}, S) ->
  {stop, normal, S};
handle_info({tcp_error, _Socket, _}, S) ->
  {stop, normal, S};
handle_info(E, S) ->
  io:format("unexpected: ~p~n", [E]),
  {noreply, S}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(normal, _State) ->
  ok;
terminate(_Reason, _State) ->
  io:format("terminate reason: ~p~n", [_Reason]).

send(Socket, Str, Args) ->
  ok = gen_tcp:send(Socket, io_lib:format(Str++"~n", Args)),
  ok = inet:setopts(Socket, [{active, once}]),
  ok.