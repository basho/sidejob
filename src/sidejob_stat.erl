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

-record(stat, {rejected = 0,
               in_sum   = 0,
               in_max   = 0,
               out_sum  = 0,
               out_max  = 0,
               samples  = 0}).

-define(ADD(Field, Value), Field = Stat#stat.Field + Value).
-define(MAX(Field, Value), Field = max(Stat#stat.Field, Value)).

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
