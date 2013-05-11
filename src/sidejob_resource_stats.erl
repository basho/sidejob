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
-module(sidejob_resource_stats).
-behaviour(gen_server).

%% API
-export([reg_name/1, start_link/2, report/5, init_stats/1, stats/1, usage/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {worker_reports = dict:new(),
                stats_ets      = undefined,
                usage          = 0,
                rejected       = 0,
                in             = 0,
                out            = 0,
                stats_60s      = sidejob_stat:new(),
                next_stats_60s = sidejob_stat:new(),
                left_60s       = 60,
                stats_total    = sidejob_stat:new()}).

%%%===================================================================
%%% API
%%%===================================================================

reg_name(Name) when is_atom(Name) ->
    reg_name(atom_to_binary(Name, latin1));
reg_name(Name) ->
    binary_to_atom(<<Name/binary, "_stats">>, latin1).

start_link(RegName, StatsETS) ->
    gen_server:start_link({local, RegName}, ?MODULE, [StatsETS], []).

%% @doc
%% Used by {@link sidejob_worker} processes to report per-worker statistics
report(Name, Id, Usage, In, Out) ->
    gen_server:cast(Name, {report, Id, Usage, In, Out}).

%% @doc
%% Used by {@link sidejob_resource_sup} to initialize a newly created
%% stats ETS table to ensure the table is non-empty before bringing a
%% resource online
init_stats(StatsETS) ->
    EmptyStats = compute(#state{}),
    ets:insert(StatsETS, [{rejected, 0},
                          {usage, 0},
                          {stats, EmptyStats}]).

%% @doc
%% Return the computed stats for the given sidejob resource
stats(Name) ->
    StatsETS = Name:stats_ets(),
    ets:lookup_element(StatsETS, stats, 2).

%% @doc
%% Return the current usage for the given sidejob resource
usage(Name) ->
    StatsETS = Name:stats_ets(),
    ets:lookup_element(StatsETS, usage, 2).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([StatsETS]) ->
    schedule_tick(),
    {ok, #state{stats_ets=StatsETS}}.

handle_call(get_stats, _From, State) ->
    {reply, compute(State), State};

handle_call(usage, _From, State=#state{usage=Usage}) ->
    {reply, Usage, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({report, Id, UsageVal, InVal, OutVal},
            State=#state{worker_reports=Reports}) ->
    Reports2 = dict:store(Id, {UsageVal, InVal, OutVal}, Reports),
    State2 = State#state{worker_reports=Reports2},
    {noreply, State2};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    schedule_tick(),
    State2 = tick(State),
    {noreply, State2};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

schedule_tick() ->
    erlang:send_after(1000, self(), tick).

%% Aggregate all reported worker stats into unified stat report for
%% this resource
tick(State=#state{stats_ets=StatsETS,
                  left_60s=Left60,
                  next_stats_60s=Next60,
                  stats_total=Total}) ->
    {Usage, In, Out} = combine_reports(State),

    Rejected = ets:update_counter(StatsETS, rejected, 0),
    ets:update_counter(StatsETS, rejected, {2,-Rejected,0,0}),

    NewNext60 = sidejob_stat:add(Rejected, In, Out, Next60),
    NewTotal = sidejob_stat:add(Rejected, In, Out, Total),
    State2 = State#state{usage=Usage,
                         rejected=Rejected,
                         in=In,
                         out=Out,
                         next_stats_60s=NewNext60,
                         stats_total=NewTotal},

    State3 = case Left60 of
                 0 ->
                     State2#state{left_60s=59,
                                  stats_60s=NewNext60,
                                  next_stats_60s=sidejob_stat:new()};
                 _ ->
                     State2#state{left_60s=Left60-1}
             end,

    ets:insert(StatsETS, [{usage, Usage},
                          {stats, compute(State3)}]),
    State3.

%% Total all reported worker stats into a single sum for each metric
combine_reports(#state{worker_reports=Reports}) ->
    dict:fold(fun(_, {Usage, In, Out}, {UsageAcc, InAcc, OutAcc}) ->
                      {UsageAcc + Usage, InAcc + In, OutAcc + Out}
              end, {0,0,0}, Reports).

compute(#state{usage=Usage, rejected=Rejected, in=In, out=Out,
               stats_60s=Stats60s, stats_total=StatsTotal}) ->
    {Usage60, Rejected60, InAvg60, InMax60, OutAvg60, OutMax60} =
        sidejob_stat:compute(Stats60s),

    {UsageTot, RejectedTot, InAvgTot, InMaxTot, OutAvgTot, OutMaxTot} =
        sidejob_stat:compute(StatsTotal),

    [{usage, Usage},
     {rejected, Rejected},
     {in_rate, In},
     {out_rate, Out},
     {usage_60s, Usage60},
     {rejected_60s, Rejected60},
     {avg_in_rate_60s, InAvg60},
     {max_in_rate_60s, InMax60},
     {avg_out_rate_60s, OutAvg60},
     {max_out_rate_60s, OutMax60},
     {usage_total, UsageTot},
     {rejected_total, RejectedTot},
     {avg_in_rate_total, InAvgTot},
     {max_in_rate_total, InMaxTot},
     {avg_out_rate_total, OutAvgTot},
     {max_out_rate_total, OutMaxTot}].
