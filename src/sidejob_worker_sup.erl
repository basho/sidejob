%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
-module(sidejob_worker_sup).
-behaviour(supervisor).

%% API
-export([start_link/4]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Name, NumWorkers, StatsName, Mod) ->
    NameBin = atom_to_binary(Name, latin1),
    RegName = binary_to_atom(<<NameBin/binary, "_worker_sup">>, latin1),
    supervisor:start_link({local, RegName}, ?MODULE,
                          [Name, NumWorkers, StatsName, Mod]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Name, NumWorkers, StatsName, Mod]) ->
    Children = [begin
                    WorkerName = sidejob_worker:reg_name(Name, Id),
                    {WorkerName,
                     {sidejob_worker, start_link,
                      [WorkerName, Name, Id, WorkerName, StatsName, Mod]},
                     permanent, 5000, worker, [sidejob_worker]}
                end || Id <- lists:seq(1, NumWorkers)],
    {ok, {{one_for_one, 10, 10}, Children}}.
