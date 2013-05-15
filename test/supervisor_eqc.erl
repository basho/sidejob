%%% File        : supervisor_eqc.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 15 May 2013 by Ulf Norell
-module(supervisor_eqc).

-compile(export_all).

-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eqc/include/eqc.hrl").
-include_lib("pulse/include/pulse.hrl").

-record(state, {limit, width, restarts = 0, supervisors = []}).
-record(supervisor, {queue, children = []}).
-record(child, {}).

-import(eqc_statem, [eq/2, tag/2]).

-define(RESOURCE, resource).
-define(SLEEP, 1).
-define(TIMEOUT, 5000).
-define(RESTART_LIMIT, 10).

initial_state() ->
  #state{}.

%% -- Commands ---------------------------------------------------------------

%% -- new_resource
new_resource_command(_S) ->
  ?LET({K, W}, {choose(1, 5), oneof([?SHRINK(default, [1]), choose(1, 8)])},
  case W of
    default ->
      Width = erlang:system_info(schedulers),
      {call, sidejob, new_resource, [?RESOURCE, sidejob_supervisor, K * Width]};
    Width ->
      {call, sidejob, new_resource, [?RESOURCE, sidejob_supervisor, K * Width, Width]}
  end).

new_resource_pre(S) -> S#state.limit == undefined.

new_resource_next(S, V, Args=[_, _, _]) ->
  new_resource_next(S, V, Args ++ [erlang:system_info(schedulers)]);
new_resource_next(S, _, [_, _, Limit, Width]) ->
  S#state{ limit = Limit, width = Width,
           supervisors = [ #supervisor{ queue = Q } || Q <- lists:seq(1, Width) ] }.

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

work_next(S, _V, [_Cmd, Sched]) ->
  case schedule(S, Sched) of
    overload -> S;
    Sup      -> add_child(#child{}, Sup, S)
  end.

work_post(S, [_Cmd, Sched], V) ->
  case schedule(S, Sched) of
    overload -> eq(V, {error, overload});
    _Sup     -> assert({not_pid, V}, is_pid(V))
  end.

%% -- Helpers ----------------------------------------------------------------

assert(_, true)  -> true;
assert(T, false) -> T;
assert(T, X)     -> {T, X}.

drop(N, Xs) -> element(2, lists:split(N, Xs)).
take(N, Xs) -> lists:sublist(Xs, N).

usage(#supervisor{children = Cs}) -> length(Cs).

add_child(Child, Sup, S) ->
  NewSup = Sup#supervisor{ children = Sup#supervisor.children ++ [Child] },
  S#state{ supervisors = lists:keystore(Sup#supervisor.queue,
                                        #supervisor.queue,
                                        S#state.supervisors,
                                        NewSup) }.

schedule(S, Scheduler) ->
  Limit = S#state.limit,
  Width = S#state.width,
  N     = Scheduler rem Width,
  case [ Sup || Sup <- drop(N, S#state.supervisors) ++
                       take(N, S#state.supervisors),
                usage(Sup) < Limit div Width ] of
    []      -> overload;
    [Sup|_] -> Sup
  end.

%% -- Workers and proxies ----------------------------------------------------

worker() ->
  receive finish -> ok;
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

the_prop() -> prop_test().

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

