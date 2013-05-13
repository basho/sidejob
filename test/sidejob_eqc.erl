%%% File        : sidejob_eqc.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 13 May 2013 by Ulf Norell
-module(sidejob_eqc).

-compile(export_all).

-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eqc/include/eqc.hrl").

-record(state, {limit, width, workers = []}).
-record(worker, {pid, scheduler, queue, status = ready}).

-import(eqc_statem, [eq/2, tag/2]).

-define(RESOURCE, bla).

initial_state() ->
  #state{}.

%% -- Commands ---------------------------------------------------------------

%% -- new_resource
new_resource_command(_S) ->
  N     = erlang:system_info(schedulers),
  Limit = ?LET(K, choose(1, 5), K * N),
  {call, sidejob, new_resource, [?RESOURCE, worker, Limit]}.

new_resource_pre(S) -> S#state.limit == undefined.

new_resource_next(S, _, [_, _, Limit]) ->
  N = erlang:system_info(schedulers),
  S#state{ limit = Limit, width = N }.

new_resource_post(_, _, V) ->
  case V of
    {ok, Pid} when is_pid(Pid) -> true;
    _                          -> {not_ok, V}
  end.

%% -- new_worker

new_worker(Scheduler) ->
  spawn_opt(fun worker/0, [{scheduler, Scheduler}]).

new_worker_args(_) ->
  [choose(1, erlang:system_info(schedulers))].

new_worker_next(S, Pid, [Sched]) ->
  S#state{ workers = S#state.workers ++ [#worker{ pid = Pid, scheduler = Sched }] }.

%% -- cast
cast(Worker) ->
  Worker ! {cast, self()},
  timer:sleep(2),
  ok.

cast_args(S) ->
  [ready_worker(S)].

cast_pre(S) ->
  S#state.limit /= undefined andalso
  ready_workers(S) /= [].

cast_pre(S, [Pid]) ->
  ready == (get_worker(S, Pid))#worker.status.

cast_next(S, _, [Pid]) ->
  W = get_worker(S, Pid),
  {Queue, Status} =
    case schedule(S, W#worker.scheduler) of
      full         -> {keep, {finished, overload}};
      {blocked, Q} -> {Q, blocked};
      {ready, Q}   -> {Q, working}
    end,
  set_worker_status(S, Pid, Queue, Status).

%% -- get_status

get_status(Worker) ->
  receive {Worker, R} -> R
  after 5 -> blocked
  end.

get_status_args(S) ->
  [busy_worker(S)].

get_status_pre(S) ->
  busy_workers(S) /= [].

get_status_pre(S, [Pid]) ->
  case get_worker(S, Pid) of
    #worker{ status = {working, _} } -> false;
    #worker{ status = ready }        -> false;
    _ -> true
  end.

get_status_next(S, V, [WPid]) ->
  NewStatus =
    case (get_worker(S, WPid))#worker.status of
      {finished, _} -> ready;
      blocked       -> blocked;
      working       -> {working, safe_element(2, V)} % {call, erlang, element, [2, V]}}
    end,
  set_worker_status(S, WPid, NewStatus).

safe_element(N, V={var, _}) -> {element, N, V};
safe_element(N, T) -> element(N, T).

get_status_post(S, [WPid], R) ->
  case (get_worker(S, WPid))#worker.status of
    {finished, Res} -> eq(R, Res);
    blocked         -> eq(R, blocked);
    working         ->
      case R of
        {working, Pid} when is_pid(Pid) -> true;
        _ -> {R, '/=', {working, 'Pid'}}
      end
  end.

%% -- Common -----------------------------------------------------------------

%% dynamic_precondition(_, {call, eqc_statem, apply, [erlang, element, [2, T]]}) ->
%%   io:format("DYNAMIC PRE!\n"),
%%   is_tuple(T);
%% dynamic_precondition(_S, Call) ->
%%   io:format("DYNAMIC PRE: ~p!\n", [Call]),
%%   true.

%% -- Helpers ----------------------------------------------------------------

schedule(S, Scheduler) ->
  Limit = S#state.limit,
  Width = S#state.width,
  N     = (Scheduler - 1) rem Width + 1,
  Queues = lists:sublist(lists:seq(N, Width) ++ lists:seq(1, Width), Width),
  IsReady  = fun(ready)         -> true;
                ({finished, _}) -> true;
                (_)             -> false end,
  QueueLen =
    fun(Q) ->
      length([ {} || #worker{ queue = AlsoQ, status = St } <- S#state.workers,
                     AlsoQ == Q, not IsReady(St) ]) end,
  Ss = [ {Q, Len}
        || Q   <- Queues,
           Len <- [QueueLen(Q)],
           Len < Limit div Width ],
  case Ss of
    []          -> full;
    [{Sc, 0}|_] -> {ready, Sc};
    [{Sc, _}|_] -> {blocked, Sc}
  end.

get_worker(S, Pid) ->
  lists:keyfind(Pid, #worker.pid, S#state.workers).

set_worker_status(S, Pid, Status) ->
  set_worker_status(S, Pid, keep, Status).

set_worker_status(S, Pid, Q0, Status) ->
  W = get_worker(S, Pid),
  Q = if Q0 == keep -> W#worker.queue;
         true       -> Q0 end,
  S#state{ workers =
    lists:keystore(Pid, #worker.pid,
                   S#state.workers,
                   W#worker{ queue = Q, status = Status }) }.

ready_worker(S) ->
  ?LET(W, elements(ready_workers(S)),
    W#worker.pid).

busy_worker(S) ->
  ?LET(W, elements(busy_workers(S)),
    W#worker.pid).

ready_workers(S) ->
  [ W || W = #worker{ status = ready } <- S#state.workers ].

busy_workers(S) ->
  [ W || W = #worker{ status = Status } <- S#state.workers, Status /= ready ].

%% -- Worker loop ------------------------------------------------------------

worker() ->
  receive
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
        From ! {self(), Res},
        worker()
  end.

%% -- Property ---------------------------------------------------------------

prop_test() ->
  ?FORALL(Cmds, commands(?MODULE),
  ?TIMEOUT(1000,
  begin
    cleanup(),
    HSR={_, S, R} = run_commands(?MODULE, Cmds),
    [ exit(Pid, kill) || #worker{ pid = Pid } <- S#state.workers ],
    pretty_commands(?MODULE, Cmds, HSR,
      R == ok)
  end)).

cleanup() ->
  error_logger:tty(false),
  (catch application:stop(sidejob)),
  error_logger:tty(true),
  application:start(sidejob).

test(N) ->
  quickcheck(numtests(N, prop_test())).

test() -> test(100).

verbose() -> eqc:check(eqc_statem:show_states(prop_test())).
check(C) -> eqc:check(prop_test(), [C]).
check() -> eqc:check(prop_test()).
recheck() -> recheck(prop_test()).

