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
-module(inter_dc_query_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0]).

%% tests
-export([
    asynchronous_test_1/1
]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").
-include("../../include/antidote.hrl").
-include("../../include/inter_dc_repl.hrl").





init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    %Nodes = hd(Clusters),
    [{nodes, Clusters}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() -> [
    asynchronous_test_1 % fail
].



%% Let 3 processes asynchronously increment the same counter each 100times while using a lock to restrict the access.
%% 30 ms delay between increments
asynchronous_test_1(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    [[Node1|_], [Node2|_], [Node3 |_]] = Nodes,

    Dc1 = rpc:call(Node1, dc_meta_data_utilities, get_my_dc_id, []),
    Dc2 = rpc:call(Node2, dc_meta_data_utilities, get_my_dc_id, []),
    Dc3 = rpc:call(Node3, dc_meta_data_utilities, get_my_dc_id, []),

    OnAnswer = fun(BinaryResp, _RequestCacheEntry) ->
        logger:info("on_interdc_reply1 ~p", [BinaryResp]),
        logger:info("on_interdc_reply2 ~p", [binary_to_term(BinaryResp)]),
        ok
    end,

    Fun = fun(Node, To, Msg) ->
        % perform_request(?LOCK_SERVER_REQUEST, PDCID, term_to_binary(ReqMsg), fun antidote_lock_server:on_interdc_reply/2)
        {LocalPartition, _} = rpc:call(Node, log_utilities, get_key_partition, [locks]),
        PDCID = {To, LocalPartition},
        ok = rpc:call(Node, inter_dc_query, perform_request, [123, PDCID, term_to_binary(Msg), OnAnswer])
    end,

    lists:foreach(fun(I) ->
        spawn_link(fun() -> Fun(Node1, Dc2, {I, "hello1"}) end),
        spawn_link(fun() -> Fun(Node2, Dc1, {I, "hello2"}) end),
        spawn_link(fun() -> Fun(Node3, Dc2, {I, "hello3"}) end),
        timer:sleep(100)
    end, lists:seq(1, 100)),

    timer:sleep(1000),
    ok.

