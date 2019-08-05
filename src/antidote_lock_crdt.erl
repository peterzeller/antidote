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

%% @doc Functions to work with the CRDT that stores lock values
-module(antidote_lock_crdt).

-include("antidote.hrl").

-define(LOCK_BUCKET, <<"__antidote_lock_bucket">>).

% there is one lock-part per datacenter.
% the map stores lock-part to current owner
-export_type([value/0]).

-export([get_lock_objects/1, get_lock_object/1, parse_lock_value/1, make_lock_updates/2]).

-type value() :: #{dcid() => dcid()}.


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

-spec parse_lock_value(antidote_crdt_map_rr:value()) -> value().
parse_lock_value(RawV) ->
    maps:from_list([{K, read_mv(V)} || {{K, _}, V} <- RawV]).


read_mv([V]) -> V;
read_mv(Vs) ->
    throw({'antidote_lock_crdt does not have a unique value', Vs}).


-spec make_lock_updates(antidote_locks:lock(), [{dcid(), dcid()}]) -> [{bound_object(), op_name(), op_param()}].
make_lock_updates(_Lock, []) -> [];
make_lock_updates(Lock, Updates) ->
    [{get_lock_object(Lock), update,
        [{{K, antidote_crdt_register_mv}, {assign, V}} || {K,V} <- Updates]}].


