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
%% This module implements a sidejob_worker behavior that operates as a
%% parallel, capacity-limited supervisor of dynamic, transient children.

-module(sidejob_supervisor).
-behaviour(gen_server).

%% API
-export([start_child/4, spawn/2, spawn/4, which_children/1]).

%% sidejob_worker callbacks
-export([current_usage/1, rate/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {name,
                children=sets:new(),
                spawned=0,
                died=0}).

-type resource() :: atom().

%%%===================================================================
%%% API
%%%===================================================================

-spec start_child(resource(), module(), atom(), term()) -> {ok, pid()} |
                                                           {error, overload} |
                                                           {error, term()}.
start_child(Name, Mod, Fun, Args) ->
    case sidejob:call(Name, {start_child, Mod, Fun, Args}, infinity) of
        overload ->
            {error, overload};
        Other ->
            Other
    end.

-spec spawn(resource(), function() | {module(), atom(), [term()]}) -> {ok, pid()} | {error, overload}.
spawn(Name, Fun) ->
    case sidejob:call(Name, {spawn, Fun}, infinity) of
        overload ->
            {error, overload};
        Other ->
            Other
    end.

-spec spawn(resource(), module(), atom(), [term()]) -> {ok, pid()} |
                                                     {error, overload}.
spawn(Name, Mod, Fun, Args) ->
    ?MODULE:spawn(Name, {Mod, Fun, Args}).

-spec which_children(resource()) -> [pid()].
which_children(Name) ->
    Workers = tuple_to_list(Name:workers()),
    Children = [gen_server:call(Worker, get_children) || Worker <- Workers],
    lists:flatten(Children).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Name]) ->
    process_flag(trap_exit, true),
    {ok, #state{name=Name}}.

handle_call(get_children, _From, State=#state{children=Children}) ->
    {reply, sets:to_list(Children), State};

handle_call({start_child, Mod, Fun, Args}, _From, State) ->
    Result = (catch apply(Mod, Fun, Args)),
    {Reply, State2} = case Result of
                          {ok, Pid} when is_pid(Pid) ->
                              {Result, add_child(Pid, State)};
                          {ok, Pid, _Info} when is_pid(Pid) ->
                              {Result, add_child(Pid, State)};
                          ignore ->
                              {{ok, undefined}, State};
                          {error, _} ->
                              {Result, State};
                          Error ->
                              {{error, Error}, State}
                      end,
    {reply, Reply, State2};

handle_call({spawn, Fun}, _From, State) ->
    Pid = case Fun of
              _ when is_function(Fun) ->
                  spawn_link(Fun);
              {M, F, A} ->
                  spawn_link(M, F, A)
          end,
    State2 = add_child(Pid, State),
    {reply, Pid, State2};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, State=#state{children=Children,
                                                died=Died}) ->
    State2 = case sets:is_element(Pid, Children) of
        true ->
            Children2 = sets:del_element(Pid, Children),
            Died2 = Died + 1,
            State#state{children=Children2, died=Died2};
        false ->
            State
    end,
    {noreply, State2};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

current_usage(#state{children=Children}) ->
    {message_queue_len, Pending} = process_info(self(), message_queue_len),
    Current = sets:size(Children),
    Pending + Current.

rate(State=#state{spawned=Spawned, died=Died}) ->
    State2 = State#state{spawned=0,
                         died=0},
    {Spawned, Died, State2}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

add_child(Pid, State=#state{children=Children, spawned=Spawned}) ->
    Children2 = sets:add_element(Pid, Children),
    Spawned2 = Spawned + 1,
    State#state{children=Children2, spawned=Spawned2}.
