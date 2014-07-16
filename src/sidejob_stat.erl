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
-module(sidejob_stat).
-export([new/0, add/4, compute/1]).

-behaviour(exometer_entry).

-export([behaviour/0,
	 new/3, delete/3, get_value/4, update/4, reset/3,
	 sample/3, get_datapoints/3, setopts/4]).

-record(stat, {rejected = 0,
               in_sum   = 0,
               in_max   = 0,
               out_sum  = 0,
               out_max  = 0,
               samples  = 0}).

-define(ADD(Field, Value), Field = Stat#stat.Field + Value).
-define(MAX(Field, Value), Field = max(Stat#stat.Field, Value)).

behaviour() ->
    entry.

new() ->
    #stat{}.

add(Rejected, In, Out, Stat) ->
    Stat#stat{?ADD(rejected, Rejected),
              ?ADD(in_sum, In),
              ?ADD(out_sum, Out),
              ?ADD(samples, 1),
              ?MAX(in_max, In),
              ?MAX(out_max, Out)}.

compute(#stat{rejected=Rejected, in_sum=InSum, in_max=InMax,
              out_sum=OutSum, out_max=OutMax, samples=Samples}) ->
    InAvg = InSum div max(1,Samples),
    OutAvg = OutSum div max(1,Samples),
    {InSum, Rejected, InAvg, InMax, OutAvg, OutMax}.


%% exometer_entry callbacks

new(_Name, _Type, Opts) ->
    case lists:keyfind(ref, 1, Opts) of
	{_, Ref} ->  {ok, Ref};
	false    ->  {error, missing_ref}
    end.

delete(_, _, _) -> ok.

get_value(_Name, _Type, Ref, DPs) ->
    try filter_datapoints(sidejob_resource_stats:stats(Ref), DPs)
    catch
	error:_ ->
	    unavailable
    end.

filter_datapoints(Stats, default) ->
    Stats;
filter_datapoints(Stats, DPs) ->
    [S || {K, _} = S <- Stats,
	  lists:member(K, DPs)].

update(_, _, _, _) -> {error, not_supported}.

reset(_, _, _) -> {error, not_supported}.

sample(_, _, _) -> {error, not_supported}.

get_datapoints(_, _, _) ->
    [usage,rejected, in_rate, out_rate, usage_60s, rejected_60s,
     avg_in_rate_60s, max_in_rate_60s, avg_out_rate_60s,
     max_out_rate_60s, usage_total, rejected_total,
     avg_in_rate_total, max_in_rate_total, avg_out_rate_total].

setopts(_, _, _, _) -> {error, not_supported}.
