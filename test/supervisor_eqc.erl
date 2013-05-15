%%% File        : supervisor_eqc.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 15 May 2013 by Ulf Norell
-module(supervisor_eqc).

-compile(export_all).

-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("pulse/include/pulse.hrl").

-record(state, {limit, width, children = []}).
-record(child, {pid}).

-import(eqc_statem, [eq/2, tag/2]).

-define(RESOURCE, resource).
-define(SLEEP, 1).
-define(TIMEOUT, 5000).
-define(RESTART_LIMIT, 10).

initial_state() ->
  #state{}.

%% -- Commands ---------------------------------------------------------------

%% -- new_resource
new_resource(Limit) ->
  R = sidejob:new_resource(?RESOURCE, sidejob_supervisor, Limit),
  timer:sleep(?SLEEP),
  R.

new_resource(Limit, Width) ->
  R = sidejob:new_resource(?RESOURCE, sidejob_supervisor, Limit, Width),
  timer:sleep(?SLEEP),
  R.

new_resource_args(_S) ->
  ?LET({K, W}, {choose(1, 5), oneof([?SHRINK(default, [1]), choose(1, 8)])},
  case W of
    default -> [K * erlang:system_info(schedulers)];
    Width   -> [K * Width, Width]
  end).

new_resource_pre(S) -> S#state.limit == undefined.

new_resource_next(S, V, Args=[_]) ->
  new_resource_next(S, V, Args ++ [erlang:system_info(schedulers)]);
new_resource_next(S, _, [Limit, Width]) ->
  S#state{ limit = Limit, width = Width }.

new_resource_post(_, _, V) ->
  case V of
    {ok, Pid} when is_pid(Pid) -> true;
    _                          -> {not_ok, V}
  end.

%% -- work
work(Cmd, Scheduler) ->
  Worker = spawn_opt(fun proxy/0, [{scheduler, Scheduler}]),
  Worker ! {Cmd, self()},
  receive
    {Worker, Reply} ->
      case Reply of
        {ok, Pid} when is_pid(Pid) -> Pid;
        Other -> Other
      end
    after 100 -> timeout
  end.

-ifdef(PULSE).
gen_scheduler() -> 1.
-else.
gen_scheduler() -> choose(1, erlang:system_info(schedulers)).
-endif.

work_args(_) ->
  [elements([start_child, spawn_mfa, spawn_fun]), gen_scheduler()].

work_pre(S) ->
  S#state.limit /= undefined.

work_next(S, V, [_Cmd, _Sched]) ->
  case length(S#state.children) < S#state.limit of
    false -> S;
    true  -> S#state{ children = S#state.children ++ [#child{pid = V}] }
  end.

work_post(S, [_Cmd, _Sched], V) ->
  case length(S#state.children) < S#state.limit of
    false -> eq(V, {error, overload});
    true  -> assert({not_pid, V}, is_pid(V))
  end.

%% -- Finish work ------------------------------------------------------------

terminate(Pid, Reason) ->
  Pid ! Reason,
  timer:sleep(?SLEEP).

terminate_args(S) ->
  [elements([ C#child.pid || C <- S#state.children ]),
   elements([normal, crash])].

terminate_pre(S) -> S#state.children /= [].
terminate_pre(S, [Pid, _]) -> lists:keymember(Pid, #child.pid, S#state.children).
terminate_next(S, _, [Pid, _]) ->
  S#state{ children = lists:keydelete(Pid, #child.pid, S#state.children) }.

%% -- which_children ---------------------------------------------------------

which_children_command(_S) ->
  {call, sidejob_supervisor, which_children, [?RESOURCE]}.

which_children_pre(S) -> S#state.limit /= undefined.

which_children_post(S, [_], V) when is_list(V) ->
  eq(lists:sort(V), lists:sort([ C#child.pid || C <- S#state.children ]));
which_children_post(_, [_], V) ->
  {not_a_list, V}.

%% -- Weights ----------------------------------------------------------------

weight(_, work)           -> 5;
weight(_, terminate)      -> 4;
weight(_, which_children) -> 1;
weight(_, _)              -> 1.

%% -- Helpers ----------------------------------------------------------------

assert(_, true)  -> true;
assert(T, false) -> T;
assert(T, X)     -> {T, X}.

%% -- Workers and proxies ----------------------------------------------------

worker() ->
  receive normal -> ok;
          crash  -> exit(crash) end.

start_worker() ->
  {ok, spawn_link(fun worker/0)}.

proxy() ->
  receive
    {Cmd, From} ->
      Res =
        case Cmd of
          start_child -> sidejob_supervisor:start_child(?RESOURCE, ?MODULE, start_worker, []);
          spawn_mfa   -> sidejob_supervisor:spawn(?RESOURCE, ?MODULE, worker, []);
          spawn_fun   -> sidejob_supervisor:spawn(?RESOURCE, fun() -> worker() end)
        end,
      From ! {self(), Res}
  end.

%% -- Property ---------------------------------------------------------------

prop_test() ->
  ?FORALL(Cmds, commands(?MODULE),
  ?TIMEOUT(?TIMEOUT,
  ?SOMETIMES(4,
  begin
    cleanup(),
    HSR={H, S, R} = run_commands(?MODULE, Cmds),
    kill_all_pids({H, S}),
    aggregate(command_names(Cmds),
    pretty_commands(?MODULE, Cmds, HSR,
      R == ok))
  end))).

prop_par() ->
  ?FORALL(Cmds, parallel_commands(?MODULE),
  ?TIMEOUT(?TIMEOUT,
  % ?SOMETIMES(4,
  begin
    cleanup(),
    HSR={SeqH, ParH, R} = run_parallel_commands(?MODULE, Cmds),
    kill_all_pids({SeqH, ParH}),
    aggregate(command_names(Cmds),
    pretty_commands(?MODULE, Cmds, HSR,
      R == ok))
  end)).

prop_pulse() ->
  ?SETUP(fun() -> N = erlang:system_flag(schedulers_online, 1),
                  fun() -> erlang:system_flag(schedulers_online, N) end end,
  ?FORALL(Cmds, parallel_commands(?MODULE),
  ?PULSE(HSR={_, _, R},
    begin
      cleanup(),
      run_parallel_commands(?MODULE, Cmds)
    end,
    aggregate(command_names(Cmds),
    pretty_commands(?MODULE, Cmds, HSR,
      R == ok))))).

kill_all_pids(Pid) when is_pid(Pid) -> exit(Pid, kill);
kill_all_pids([H|T])                -> kill_all_pids(H), kill_all_pids(T);
kill_all_pids(T) when is_tuple(T)   -> kill_all_pids(tuple_to_list(T));
kill_all_pids(_)                    -> ok.

cleanup() ->
  error_logger:tty(false),
  (catch application:stop(sidejob)),
  % error_logger:tty(true),
  application:start(sidejob).

the_prop() -> prop_par().

test({N, h})   -> test({N * 60, min});
test({N, min}) -> test({N * 60, sec});
test({N, s})   -> test({N, sec});
test({N, sec}) ->
  quickcheck(eqc:testing_time(N, the_prop()));
test(N) when is_integer(N) ->
  quickcheck(numtests(N, the_prop())).

test() -> test(100).

recheck() -> eqc:recheck(the_prop()).
check()   -> eqc:check(the_prop()).
check(CE) -> eqc:check(the_prop(), CE).

verbose()   -> eqc:check(eqc_statem:show_states(the_prop())).
verbose(CE) -> eqc:check(eqc_statem:show_states(the_prop(), CE)).

