%%% File        : sidejob_eqc.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 13 May 2013 by Ulf Norell
-module(sidejob_eqc).

-compile(export_all).

-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eqc/include/eqc.hrl").

-record(state, {limit, width, restarts = 0, workers = []}).
-record(worker, {pid, scheduler, queue, status = ready}).

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

%% -- cast
cast(Scheduler) ->
  Worker = spawn_opt(fun worker/0, [{scheduler, Scheduler}]),
  Worker ! {cast, self()},
  timer:sleep(?SLEEP),
  {Worker, get_status(Worker)}.

cast_args(_) ->
  [choose(1, erlang:system_info(schedulers))].

cast_pre(S) ->
  S#state.limit /= undefined.

cast_next(S, V, [Sched]) ->
  Pid    = {call, erlang, element, [1, V]},
  Status = {call, erlang, element, [2, V]},
  S1 = do_cast(S, Pid, [Sched]),
  get_status_next(S1, Status, [Pid]).

do_cast(S, Pid, [Sched]) ->
  {Queue, Status} =
    case schedule(S, Sched) of
      full         -> {keep, {finished, overload}};
      {blocked, Q} -> {Q, blocked};
      {ready, Q}   -> {Q, working}
    end,
  W = #worker{ pid = Pid,
               scheduler = Sched,
               queue = Queue,
               status = Status },
  S#state{ workers = S#state.workers ++ [W] }.

cast_post(S, [Sched], {Pid, Status}) ->
  get_status_post(do_cast(S, Pid, [Sched]), [Pid], Status).

%% -- get_status
get_status(Worker) ->
  receive {Worker, R} -> R
  after ?SLEEP -> blocked
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
      working       -> {working, {call, erlang, element, [2, V]}}
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
  wakeup_worker(set_worker_status(S, W#worker.pid, stopped), W#worker.queue).

%. -- crash
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

%% -- Worker loop ------------------------------------------------------------

worker() ->
  receive
    {cast, From} ->
      Ref = make_ref(),
      %% io:format("~p on ~p\n", [self(), erlang:system_info(scheduler_id)]),
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

%% -- Property ---------------------------------------------------------------

prop_test() ->
  ?FORALL(Cmds, commands(?MODULE),
  ?TIMEOUT(?TIMEOUT,
  ?SOMETIMES(4,
  begin
    cleanup(),
    HSR={_, S, R} = run_commands(?MODULE, Cmds),
    [ exit(Pid, kill) || #worker{ pid = Pid } <- S#state.workers ],
    aggregate(command_names(Cmds),
    pretty_commands(?MODULE, Cmds, HSR,
      R == ok))
  end))).

cleanup() ->
  error_logger:tty(false),
  (catch application:stop(sidejob)),
  % error_logger:tty(true),
  application:start(sidejob).

test(N) ->
  quickcheck(numtests(N, prop_test())).

test() -> test(100).

verbose() -> eqc:check(eqc_statem:show_states(prop_test())).
check(C) -> eqc:check(prop_test(), [C]).
check() -> eqc:check(prop_test()).
recheck() -> recheck(prop_test()).

