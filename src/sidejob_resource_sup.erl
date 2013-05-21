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

%% @doc
%% The sidejob_resource_sup manages the entire supervision hierarchy for
%% a sidejob resource. Thus, there is one resource supervisor for each
%% registered sidejob resource.
%%
%% The resource supervisor is the owner of a resource's limit and stats
%% ETS tables, therefore ensuring the ETS tables survive crashes elsewhere
%% in the resource hierarchy. 
%%
%% The resource supervisor has two children: a {@link sidejob_worker_sup}
%% that supervises the actual worker processes for a given resource, and
%% a {@link sidejob_resource_stats} server that aggregates statistics
%% reported by the worker processes.
-module(sidejob_resource_sup).
-behaviour(supervisor).

%% API
-export([start_link/2, stats_ets/1]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link(Name, Mod) ->
    supervisor:start_link({local, Name}, ?MODULE, [Name, Mod]).

stats_ets(Name) ->
    ETS = iolist_to_binary([atom_to_binary(Name, latin1), "_stats_ets"]),
    binary_to_atom(ETS, latin1).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Name, Mod]) ->
    Width = Name:width(),
    StatsETS = stats_ets(Name),
    StatsName = Name:stats(),
    WorkerNames = sidejob_worker:workers(Name, Width),

    _WorkerETS = [begin
                      WorkerTab = ets:new(WorkerName, [named_table,
                                                       public]),
                      ets:insert(WorkerTab, [{usage, 0},
                                             {full, 0}]),
                      WorkerTab
                  end || WorkerName <- WorkerNames],

    StatsTab = ets:new(StatsETS, [named_table,
                                  public,
                                  {read_concurrency,true},
                                  {write_concurrency,true}]),
    sidejob_resource_stats:init_stats(StatsTab),

    WorkerSup = {sidejob_worker_sup,
                 {sidejob_worker_sup, start_link,
                  [Name, Width, StatsName, Mod]},
                 permanent, infinity, supervisor, [sidejob_worker_sup]},
    StatsServer = {StatsName,
                   {sidejob_resource_stats, start_link, [StatsName, StatsTab]},
                   permanent, 5000, worker, [sidejob_resource_stats]},
    {ok, {{one_for_one, 10, 10}, [WorkerSup, StatsServer]}}.
