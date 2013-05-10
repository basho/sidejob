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
-module(sidejob_app).
-behaviour(application).

%% Application callbacks
-export([start/2, start_phase/3, prep_stop/1, stop/1, config_change/3]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

start(_Type, _StartArgs) ->
    case sidejob_sup:start_link() of
        {ok, Pid} ->
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    ok.

start_phase(_Phase, _Type, _PhaseArgs) ->
    ok.

prep_stop(State) ->
    State.

config_change(_Changed, _New, _Removed) ->
    ok.
