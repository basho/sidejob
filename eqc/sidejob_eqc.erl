%%% File        : sidejob_eqc.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 13 May 2013 by Ulf Norell
-module(sidejob_eqc).

-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eqc/include/eqc.hrl").
-ifdef(PULSE).
-export([prop_pulse/0, pulse_instrument/0, pulse_instrument/1]).
-include_lib("pulse/include/pulse.hrl").
-endif.

-export([initial_state/0]).
-export([prop_seq/0, prop_par/0]).
-export([work/2, finish_work/1, crash/1, get_element/2, get_status/1]).
-export([new_resource_command/1,
         new_resource_pre/1, new_resource_next/3, new_resource_post/3]).

-record(state, {limit, width, restarts = 0, workers = []}).
-record(worker, {pid, scheduler, queue, status = ready, cmd}).

-import(eqc_statem, [tag/2]).

-define(RESOURCE, resource).
-define(SLEEP, 1).
-define(TIMEOUT, 5000).
-define(RESTART_LIMIT, 10).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

initial_state() ->
  #state{}.

%% -- Commands ---------------------------------------------------------------

%% -- new_resource
new_resource_command(_S) ->
  ?LET({K, W}, {choose(1, 5), oneof([?SHRINK(default, [1]), choose(1, 8)])},
  case W of
    default ->
      Width = erlang:system_info(schedulers),
      {call, sidejob, new_resource, [?RESOURCE, worker, K * Width]};
    Width ->
      {call, sidejob, new_resource, [?RESOURCE, worker, K * Width, Width]}
  end).

new_resource_pre(S) -> S#state.limit == undefined.

new_resource_next(S, V, Args=[_, _, _]) ->
  new_resource_next(S, V, Args ++ [erlang:system_info(schedulers)]);
new_resource_next(S, _, [_, _, Limit, Width]) ->
  S#state{ limit = Limit, width = Width }.

new_resource_post(_, _, V) ->
  case V of
    {ok, Pid} when is_pid(Pid) -> true;
    _                          -> {not_ok, V}
  end.

%% -- work
work(Cmd, Scheduler) ->
  status_keeper ! {start_worker, self(), Cmd, Scheduler},
  Worker = receive {start_worker, Worker0} -> Worker0 end,
  timer:sleep(?SLEEP),
  {Worker, get_status(Worker)}.

-ifdef(PULSE).
gen_scheduler() -> 1.
-else.
gen_scheduler() -> choose(1, erlang:system_info(schedulers)).
-endif.

work_args(_) ->
  [elements([call, cast]), gen_scheduler()].

work_pre(S) ->
  S#state.limit /= undefined.

work_next(S, V, [Cmd, Sched]) ->
  Pid    = {call, ?MODULE, get_element, [1, V]},
  Status = {call, ?MODULE, get_element, [2, V]},
  S1 = do_work(S, Pid, [Cmd, Sched]),
  get_status_next(S1, Status, [Pid]).

do_work(S, Pid, [Cmd, Sched]) ->
  {Queue, Status} =
    case schedule(S, Sched) of
      full         -> {keep, {finished, overload}};
      {blocked, Q} -> {Q, blocked};
      {ready, Q}   -> {Q, working}
    end,
  W = #worker{ pid = Pid,
               scheduler = Sched,
               queue = Queue,
               cmd = Cmd,
               status = Status },
  S#state{ workers = S#state.workers ++ [W] }.

work_post(S, [Cmd, Sched], {Pid, Status}) ->
  get_status_post(do_work(S, Pid, [Cmd, Sched]), [Pid], Status).

%% -- get_status
get_status(Worker) ->
  status_keeper ! {get_status, self(), Worker},
  receive {Worker, R} -> R
  end.

get_status_args(S) ->
  [busy_worker(S)].

get_status_pre(S) ->
  busy_workers(S) /= [].

get_status_pre(S, [Pid]) ->
  case get_worker(S, Pid) of
    #worker{ status = {working, _} } -> false;
    #worker{} -> true;
    _ -> false
  end.

get_status_next(S, V, [WPid]) ->
  NewStatus =
    case (get_worker(S, WPid))#worker.status of
      {finished, _} -> stopped;
      blocked       -> blocked;
      zombie        -> zombie;
      working       -> {working, {call, ?MODULE, get_element, [2, V]}}
    end,
  set_worker_status(S, WPid, NewStatus).

get_status_post(S, [WPid], R) ->
  case (get_worker(S, WPid))#worker.status of
    {finished, Res} -> eq(R, Res);
    blocked         -> eq(R, blocked);
    zombie          -> eq(R, blocked);
    working         ->
      case R of
        {working, Pid} when is_pid(Pid) -> true;
        _ -> {R, '/=', {working, 'Pid'}}
      end
  end.

%% -- finish
finish_work(bad_element) -> ok;
finish_work(Pid) ->
  Pid ! finish,
  timer:sleep(?SLEEP).

finish_work_args(S) ->
  [elements(working_workers(S))].

finish_work_pre(S) ->
  working_workers(S) /= [].

finish_work_pre(S, [Pid]) ->
  lists:member(Pid, working_workers(S)).

finish_work_next(S, _, [Pid]) ->
  W = #worker{} = lists:keyfind({working, Pid}, #worker.status, S#state.workers),
  Status =
    case W#worker.cmd of
      cast -> stopped;
      call -> {finished, done}
    end,
  wakeup_worker(set_worker_status(S, W#worker.pid, Status), W#worker.queue).

%. -- crash
crash(bad_element) -> ok;
crash(Pid) ->
  Pid ! crash,
  timer:sleep(?SLEEP).

crash_args(S) ->
  [elements(working_workers(S))].

crash_pre(S) ->
  working_workers(S) /= [].

crash_pre(S, [Pid]) ->
  lists:member(Pid, working_workers(S)).

crash_next(S, _, [Pid]) ->
  W = #worker{} = lists:keyfind({working, Pid}, #worker.status, S#state.workers),
  S1 = S#state{ restarts = S#state.restarts + 1 },
  S2 = set_worker_status(S1, W#worker.pid, stopped),
  case S2#state.restarts > ?RESTART_LIMIT of
    true  -> kill_all_queues(S2#state{ restarts = 0 });
    false -> kill_queue(S2, W#worker.queue)
  end.

kill_queue(S, Q) ->
  Kill =
    fun(W=#worker{ queue = Q1, status = blocked }) when Q1 == Q ->
          W#worker{ queue = zombie };
       (W) -> W end,
  S#state{ workers = lists:map(Kill, S#state.workers) }.

kill_all_queues(S) ->
  Kill = fun(W) -> W#worker{ queue = zombie } end,
  S#state{ workers = lists:map(Kill, S#state.workers) }.

%% -- Helpers ----------------------------------------------------------------

schedule(S, Scheduler) ->
  Limit = S#state.limit,
  Width = S#state.width,
  N     = (Scheduler - 1) rem Width + 1,
  Queues = lists:sublist(lists:seq(N, Width) ++ lists:seq(1, Width), Width),
  NotReady  = fun(ready)         -> false;
                 ({finished, _}) -> false;
                 (_)             -> true end,
  IsWorking = fun({working, _}) -> true;
                 (working)      -> true;
                 (_)            -> false end,
  QueueLen =
    fun(Q) ->
      Stats = [ St || #worker{ queue = AlsoQ, status = St } <- S#state.workers,
                      AlsoQ == Q ],
      {length(lists:filter(NotReady, Stats)), lists:any(IsWorking, Stats)}
    end,
  Ss = [ {Q, Worker}
        || Q             <- Queues,
           {Len, Worker} <- [QueueLen(Q)],
           Len < Limit div Width ],
  case Ss of
    []              -> full;
    [{Sc, false}|_] -> {ready, Sc};
    [{Sc, _}|_]     -> {blocked, Sc}
  end.

get_worker(S, Pid) ->
  lists:keyfind(Pid, #worker.pid, S#state.workers).

wakeup_worker(S, Q) ->
  Blocked = [ W || W=#worker{status = blocked, queue = Q1} <- S#state.workers, Q == Q1 ],
  case Blocked of
    [] -> S;
    [#worker{pid = Pid}|_] ->
      set_worker_status(S, Pid, working)
  end.

set_worker_status(S, Pid, Status) ->
  set_worker_status(S, Pid, keep, Status).

set_worker_status(S, Pid, _, stopped) ->
  S#state{ workers = lists:keydelete(Pid, #worker.pid, S#state.workers) };
set_worker_status(S, Pid, Q0, Status) ->
  W = get_worker(S, Pid),
  Q = if Q0 == keep -> W#worker.queue;
         true       -> Q0 end,
  S#state{ workers =
    lists:keystore(Pid, #worker.pid,
                   S#state.workers,
                   W#worker{ queue = Q, status = Status }) }.

busy_worker(S) ->
  ?LET(W, elements(busy_workers(S)),
    W#worker.pid).

busy_workers(S) ->
  S#state.workers.

working_workers(S) ->
  [ Pid || #worker{ status = {working, Pid}, queue = Q } <- S#state.workers, Q /= zombie ].

get_element(N, T) when is_tuple(T) -> element(N, T);
get_element(_, _) -> bad_element.

%% -- Worker loop ------------------------------------------------------------

worker() ->
  receive
    {call, From} ->
      Res = sidejob:call(?RESOURCE, {start, self(), From}),
      From ! {self(), Res};
    {cast, From} ->
      Ref = make_ref(),
      Res =
        case sidejob:cast(?RESOURCE, {start, Ref, self()}) of
          overload -> overload;
          ok ->
            receive
              {started, Ref, Pid} -> {working, Pid}
            end
        end,
        From ! {self(), Res}
  end.

%% -- Status keeper ----------------------------------------------------------
%% When running with parallel_commands we need a proxy process that holds the
%% statuses of the workers.
start_status_keeper() ->
  catch erlang:exit(whereis(status_keeper), kill),
  timer:sleep(?SLEEP),
  register(status_keeper, spawn(fun() -> status_keeper([]) end)).

status_keeper(State) ->
  receive
    {start_worker, From, Cmd, Scheduler} ->
      Worker = spawn_opt(fun worker/0, [{scheduler, Scheduler}]),
      Worker ! {Cmd, self()},
      timer:sleep(?SLEEP),
      From ! {start_worker, Worker},
      status_keeper([{worker, Worker, []} | State]);
    {Worker, Status} when is_pid(Worker) ->
      {worker, Worker, OldStatus} = lists:keyfind(Worker, 2, State),
      status_keeper(lists:keystore(Worker, 2, State, {worker, Worker, OldStatus ++ [Status]}));
    {get_status, From, Worker} ->
      case lists:keyfind(Worker, 2, State) of
        {worker, Worker, [Status | NewStatus]} ->
          From ! {Worker, Status},
          status_keeper(lists:keystore(Worker, 2, State, {worker, Worker, NewStatus}));
        _ ->
          From ! {Worker, blocked},
          status_keeper(State)
      end
  end.

%% -- Property ---------------------------------------------------------------

prop_seq() ->
  ?FORALL(Cmds, commands(?MODULE),
  ?TIMEOUT(?TIMEOUT,
  ?SOMETIMES(10,
  begin
    cleanup(),
    HSR={_, S, R} = run_commands(?MODULE, Cmds),
    [ exit(Pid, kill) || #worker{ pid = Pid } <- S#state.workers, is_pid(Pid) ],
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

-ifdef(PULSE).
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
-endif.

kill_all_pids(Pid) when is_pid(Pid) -> exit(Pid, kill);
kill_all_pids([H|T])                -> kill_all_pids(H), kill_all_pids(T);
kill_all_pids(T) when is_tuple(T)   -> kill_all_pids(tuple_to_list(T));
kill_all_pids(_)                    -> ok.

cleanup() ->
  start_status_keeper(),
  error_logger:tty(false),
  (catch application:stop(sidejob)),
  % error_logger:tty(true),
  application:start(sidejob).

-ifdef(PULSE).
pulse_instrument() ->
  [ pulse_instrument(File) || File <- filelib:wildcard("../src/*.erl") ++
                                      filelib:wildcard("../test/*.erl") ].

pulse_instrument(File) ->
  Modules = [ application, application_controller, application_master,
              application_starter, gen, gen_event, gen_fsm, gen_server,
              proc_lib, supervisor ],
  ReplaceModules =
    [{Mod, list_to_atom(lists:concat([pulse_, Mod]))}
      || Mod <- Modules],
    io:format("compiling ~p~n", [File]),
  {ok, Mod} = compile:file(File, [{d, 'PULSE', true}, {d, 'EQC', true},
                                  {parse_transform, pulse_instrument},
                                  {pulse_side_effect, [{ets, '_', '_'}]},
                                  {pulse_replace_module, ReplaceModules}]),
  code:purge(Mod),
  code:load_file(Mod),
  Mod.
-endif.