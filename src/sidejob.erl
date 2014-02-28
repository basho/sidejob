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
-module(sidejob).
-export([new_resource/3, new_resource/4, call/2, call/3, cast/2,
         unbounded_cast/2]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc
%% Create a new sidejob resource that uses the provided worker module,
%% enforces the requested usage limit, and is managed by the specified
%% number of worker processes.
%%
%% This call will generate and load a new module, via {@link sidejob_config},
%% that provides information about the new resource. It will also start up the
%% supervision hierarchy that manages this resource: ensuring that the workers
%% and stats aggregation server for this resource remain running.
new_resource(Name, Mod, Limit, Workers) ->
    StatsETS = sidejob_resource_sup:stats_ets(Name),
    WorkerNames = sidejob_worker:workers(Name, Workers),
    StatsName = sidejob_resource_stats:reg_name(Name),
    WorkerLimit = Limit div Workers,
    sidejob_config:load_config(Name, [{width, Workers},
                                      {limit, Limit},
                                      {worker_limit, WorkerLimit},
                                      {stats_ets, StatsETS},
                                      {workers, list_to_tuple(WorkerNames)},
                                      {worker_ets, list_to_tuple(WorkerNames)},
                                      {stats, StatsName}]),
    sidejob_sup:add_resource(Name, Mod).

%% @doc
%% Same as {@link new_resource/4} except that the number of workers defaults
%% to the number of scheduler threads.
new_resource(Name, Mod, Limit) ->
    Workers = erlang:system_info(schedulers),
    new_resource(Name, Mod, Limit, Workers).


%% @doc
%% Same as {@link call/3} with a default timeout of 5 seconds.
call(Name, Msg) ->
    call(Name, Msg, 5000).

%% @doc
%% Perform a synchronous call to the specified resource, failing if the
%% resource has reached its usage limit.
call(Name, Msg, Timeout) ->
    case available(Name) of
        none ->
            overload;
        Worker ->
            gen_server:call(Worker, Msg, Timeout)
    end.

%% @doc
%% Perform an asynchronous cast to the specified resource, failing if the
%% resource has reached its usage limit.
cast(Name, Msg) ->
    case available(Name) of
        none ->
            overload;
        Worker ->
            gen_server:cast(Worker, Msg)
    end.

%% @doc
%% Perform an asynchronous cast to the specified resource, ignoring
%% usage limits
unbounded_cast(Name, Msg) ->
    Worker = preferred_worker(Name),
    gen_server:cast(Worker, Msg).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Return the preferred worker for the current scheduler
preferred_worker(Name) ->
    Width = Name:width(),
    Scheduler = erlang:system_info(scheduler_id),
    Worker = Scheduler rem Width,
    worker_reg_name(Name, Worker).

%% Find an available worker or return none if all workers at limit
available(Name) ->
    WorkerETS = Name:worker_ets(),
    Width = Name:width(),
    Limit = Name:worker_limit(),
    Scheduler = erlang:system_info(scheduler_id),
    Worker = Scheduler rem Width,
    case is_available(WorkerETS, Limit, Worker) of
        true ->
            worker_reg_name(Name, Worker);
        false ->
            available(Name, WorkerETS, Width, Limit, Worker+1, Worker)
    end.

available(Name, _WorkerETS, _Width, _Limit, End, End) ->
    ets:update_counter(Name:stats_ets(), rejected, 1),
    none;
available(Name, WorkerETS, Width, Limit, X, End) ->
    Worker = X rem Width,
    case is_available(WorkerETS, Limit, Worker) of
        false ->
            available(Name, WorkerETS, Width, Limit, (Worker+1) rem Width, End);
        true ->
            worker_reg_name(Name, Worker)
    end.

is_available(WorkerETS, Limit, Worker) ->
    ETS = element(Worker+1, WorkerETS),
    case ets:lookup_element(ETS, full, 2) of
        1 ->
            false;
        0 ->
            Value = ets:update_counter(ETS, usage, 1),
            if Value >= Limit ->
                    ets:insert(ETS, {full, 1});
               true ->
                    ok
            end,
            true
    end.

worker_reg_name(Name, Id) ->
    element(Id+1, Name:workers()).
