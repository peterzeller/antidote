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
-module(antidote_pb_server_sup).
-behaviour(supervisor).

% This supervisor manages antidote_pb_server, which are listening on the PB-port
% the code follows the description in http://learnyousomeerlang.com/buckets-of-sockets

-export([start_link/0, start_socket/0]).
-export([init/1]).


start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  {ok, Port} = application:get_env(antidote_pb_port, 8087),
  {ok, ListenSocket} = gen_tcp:listen(Port, [{active, once}, {packet, line}]),
  spawn_link(fun empty_listeners/0),
  {ok, {{simple_one_for_one, 60, 3600},
    [{socket,
      {antidote_pb_server, start_link, [ListenSocket]}, % pass the socket!
      temporary, 1000, worker, [antidote_pb_server]}
    ]}}.

start_socket() ->
  supervisor:start_child(?MODULE, []).

empty_listeners() ->
  [start_socket() || _ <- lists:seq(1, 20)],
  ok.
