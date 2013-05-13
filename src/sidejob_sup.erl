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
%% The top-level supervisor for the sidejob application.
%%
%% When a new resource is created via {@link sidejob:new_resource/4},
%% a new {@link sidejob_resource_sup} is added to this supervisor.
%%
%% The actual resource supervisor manages a given resource's process
%% hierarchy. This top-level supervisor simply ensures that all registered
%% resource supervisors remain up.

-module(sidejob_sup).
-behaviour(supervisor).

%% API
-export([start_link/0, add_resource/2]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

add_resource(Name, Mod) ->
    Child = {Name,
             {sidejob_resource_sup, start_link, [Name, Mod]},
             permanent, infinity, supervisor, [sidejob_resource_sup]},
    supervisor:start_child(?MODULE, Child).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    {ok, {{one_for_one, 10, 10}, []}}.
