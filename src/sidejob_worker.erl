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
%% This module implements the sidejob_worker logic used by all worker
%% processes created to manage a sidejob resource. This code emulates
%% the gen_server API, wrapping a provided user-specified module which
%% implements the gen_server behavior.
%%
%% The primary purpose of this module is updating the usage information
%% published in a given resource's ETS table, such that capacity limiting
%% operates correctly. The sidejob_worker also cooperates with a given
%% {@link sidejob_resource_stats} server to maintain statistics about a
%% given resource.
%%
%% By default, a sidejob_worker calculates resource usage based on message
%% queue size. However, the user-specified module can also choose to
%% implement the `current_usage/1' and `rate/1' callbacks to change how
%% usage is calculated. An example is the {@link sidejob_supervisor} module
%% which reports usage as: queue size + num_children.

-module(sidejob_worker).
-behaviour(gen_server).

%% API
-export([start_link/6, reg_name/1, reg_name/2, workers/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {id              :: non_neg_integer(),
                ets             :: term(),
                width           :: pos_integer(),
                limit           :: pos_integer(),
                reporter        :: term(),
                mod             :: module(),
                modstate        :: term(),
                usage           :: custom | default,
                last_mq_len = 0 :: non_neg_integer(),
                enqueue     = 0 :: non_neg_integer(),
                dequeue     = 0 :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

reg_name(Id) ->
    IdBin = list_to_binary(integer_to_list(Id)),
    binary_to_atom(<<"sidejob_worker_", IdBin/binary>>, latin1).

reg_name(Name, Id) when is_atom(Name) ->
    reg_name(atom_to_binary(Name, latin1), Id);
reg_name(NameBin, Id) ->
    WorkerName = iolist_to_binary([NameBin, "_", integer_to_list(Id)]),
    binary_to_atom(WorkerName, latin1).

workers(Name, Count) ->
    NameBin = atom_to_binary(Name, latin1),
    [reg_name(NameBin, Id) || Id <- lists:seq(1,Count)].

start_link(RegName, ResName, Id, ETS, StatsName, Mod) ->
    gen_server:start_link({local, RegName}, ?MODULE,
                          [ResName, Id, ETS, StatsName, Mod], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([ResName, Id, ETS, StatsName, Mod]) ->
    %% TODO: Add ability to pass args
    case Mod:init([ResName]) of
        {ok, ModState} ->
            Exports = proplists:get_value(exports, Mod:module_info()),
            Usage = case lists:member({current_usage, 1}, Exports) of
                        true ->
                            custom;
                        false ->
                            default
                    end,
            schedule_tick(),
            Width = ResName:width(),
            Limit = ResName:limit(),
            State = #state{id=Id,
                           ets=ETS,
                           mod=Mod,
                           modstate=ModState,
                           usage=Usage,
                           width=Width,
                           limit=Limit,
                           reporter=StatsName},
            ets:insert(ETS, [{usage,0}, {full,0}]),
            {ok, State};
        Other ->
            Other
    end.

handle_call(Request, From, State=#state{mod=Mod,
                                        modstate=ModState}) ->
    Result = Mod:handle_call(Request, From, ModState),
    {Pos, ModState2} = case Result of
                           {reply,_Reply,NewState} ->
                               {3, NewState};
                           {reply,_Reply,NewState,hibernate} ->
                               {3, NewState};
                           {reply,_Reply,NewState,_Timeout} ->
                               {3, NewState};
                           {noreply,NewState} ->
                               {2, NewState};
                           {noreply,NewState,hibernate} ->
                               {2, NewState};
                           {noreply,NewState,_Timeout} ->
                               {2, NewState};
                           {stop,_Reason,_Reply,NewState} ->
                               {4, NewState};
                           {stop,_Reason,NewState} ->
                               {3, NewState}
                       end,
    State2 = State#state{modstate=ModState2},
    State3 = update_rate(update_usage(State2)),
    Return = setelement(Pos, Result, State3),
    Return.

handle_cast(Request, State=#state{mod=Mod,
                                  modstate=ModState}) ->
    Result = Mod:handle_cast(Request, ModState),
    {Pos, ModState2} = case Result of
                           {noreply,NewState} ->
                               {2, NewState};
                           {noreply,NewState,hibernate} ->
                               {2, NewState};
                           {noreply,NewState,_Timeout} ->
                               {2, NewState};
                           {stop,_Reason,NewState} ->
                               {3, NewState}
                       end,
    State2 = State#state{modstate=ModState2},
    State3 = update_rate(update_usage(State2)),
    Return = setelement(Pos, Result, State3),
    Return.

handle_info('$sidejob_worker_tick', State) ->
    State2 = tick(State),
    schedule_tick(),
    {noreply, State2};

handle_info(Info, State=#state{mod=Mod,
                               modstate=ModState}) ->
    Result = Mod:handle_info(Info, ModState),
    {Pos, ModState2} = case Result of
                           {noreply,NewState} ->
                               {2, NewState};
                           {noreply,NewState,hibernate} ->
                               {2, NewState};
                           {noreply,NewState,_Timeout} ->
                               {2, NewState};
                           {stop,_Reason,NewState} ->
                               {3, NewState}
                       end,
    State2 = State#state{modstate=ModState2},
    State3 = update_rate(update_usage(State2)),
    Return = setelement(Pos, Result, State3),
    Return.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

schedule_tick() ->
    erlang:send_after(1000, self(), '$sidejob_worker_tick').

tick(State=#state{id=Id, reporter=Reporter}) ->
    Usage = current_usage(State),
    {In, Out, State2} = current_rate(State),
    sidejob_resource_stats:report(Reporter, Id, Usage, In, Out),
    State2.

update_usage(State=#state{ets=ETS, width=Width, limit=Limit}) ->
    Usage = current_usage(State),
    Full = case Usage >= (Limit div Width) of
               true ->
                   1;
               false ->
                   0
           end,
    ets:insert(ETS, [{usage, Usage},
                     {full, Full}]),
    State.

current_usage(#state{usage=default}) ->
    {message_queue_len, Len} = process_info(self(), message_queue_len),
    Len;
current_usage(#state{usage=custom, mod=Mod, modstate=ModState}) ->
    Mod:current_usage(ModState).

update_rate(State=#state{usage=custom}) ->
    %% Assume this is updated internally in the custom module
    State;
update_rate(State=#state{usage=default,
                         last_mq_len=LastLen}) ->
    {message_queue_len, Len} = process_info(self(), message_queue_len),
    Enqueue = Len - LastLen + 1,
    Dequeue = State#state.dequeue + 1,
    State#state{enqueue=Enqueue, dequeue=Dequeue}.

%% TODO: Probably should rename since it resets rate
current_rate(State=#state{usage=default,
                          enqueue=Enqueue,
                          dequeue=Dequeue}) ->
    State2 = State#state{enqueue=0, dequeue=0},
    {Enqueue, Dequeue, State2};
current_rate(State=#state{usage=custom, mod=Mod, modstate=ModState}) ->
    {Enqueue, Dequeue, ModState2} = Mod:rate(ModState),
    State2 = State#state{modstate=ModState2},
    {Enqueue, Dequeue, State2}.
