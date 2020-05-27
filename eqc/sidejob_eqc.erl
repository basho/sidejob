%%% File        : sidejob_eqc.erl
%%% Author      : Ulf Norell
%%% Description : 
%%% Created     : 13 May 2013 by Ulf Norell
-module(sidejob_eqc).

%% Sidejob is intended to run jobs (of the form call or cast), running
%% at most W jobs in parallel, and returning 'overload' if more than
%% K*W jobs are waiting to be completed. Here W is the number of
%% sidejob workers, and K-1 is the maximum number of jobs that can be
%% waiting for any particular worker. When new jobs are submitted,
%% sidejob looks for an available worker (with fewer than K-1 jobs in
%% its queue), starting with one corresponding to the scheduler number
%% that the caller is running on; it returns overload only if every
%% worker has K-1 waiting jobs at the time sidejob checks.

%% If a job crashes, then the worker that was running it is restarted,
%% but any jobs waiting for that worker are lost (and, in the event of
%% a call job, cause their caller to crash too).

%% Sidejob is inherently non-deterministic. For example, if K*W jobs
%% are running, one is about to finish, and another is about to be
%% submitted, then there is a race between these two events that can
%% lead the new job to be rejected as overload, or not. Even in a
%% non-overload situation, a worker with a full queue which is about
%% to finish a job may be assigned a new job if the finish happens
%% first, or it may be assigned to the next worker if the finish
%% happens second. Thus it is impossible to predict reliably which
%% worker a job will be assigned to, and thus which jobs will be
%% discarded when a job crashes.

%% Nevertheless, this model tries to predict such outcomes
%% precisely. As a result, the tests suffer from race conditions, and
%% (even the sequential) tests have been failing. To address this, the
%% model sleeps after every action, to allow sidejob to complete all
%% the resulting actions. This sleep was originally 1ms, which was not
%% always long enough, leading tests to fail. Now
%%  * we sleep for 2ms,
%%  * we check to see if the VM is "quiescent" before continuing, and
%%    if not, we sleep again,
%%  * we retry calls that return results that could be transient
%%    ('overload' from call and cast, 'blocked' from get_status)
%%  * after a restart of the task supervisor, we wait 10ms (!) because
%%    weird stuff happens if we don't
%% This makes tests much more deterministic, at least. Fewer than one
%% test in 300,000 should fail--if they fail more often than that,
%% there is something wrong.

%% The disadvantages of this approach are:
%%  * It is still possible, if much less likely, that a test fail when
%%    nothing is wrong.
%%  * This model cannot test rapid sequences of events, and so risks
%%    missing some bugs, because it must wait for quiescence after
%%    every operation.
%%  * It does not make sense to run parallel tests with this model.

%% Three ways in which testing could be improved are:
%%  1. Use PULSE, not for parallel testing, but to run sequential
%%     tests, because PULSE can guarantee quiescence before proceeding
%%     to the next operation, without sleeping in reality. This could
%%     make tests very much faster to run.
%%  2. Create a different model that tolerates non-determinism,
%%     instead checking global properties such as that no more than
%%     W jobs are in progress simultaneously, that jobs are rejected
%%     as overload iff K*W jobs are currently in the system, that *at
%%     least* ceiling (N/K) jobs are actually running when N jobs are
%%     in the system. Such a model could be used to test sidejob with
%%     rapidly arriving events, and so might find race conditions that
%%     this model misses. It could also potentially run much faster,
%%     and thus find bugs that are simply too rare to find in a
%%     realistic time with a model that sleeps frequently.
%%  3. Make the 'intensity' parameter of the sidejob supervisor
%%     configurable--at present it is always 10, which means that a
%%     supervisor restart only happens after ten jobs crash. This
%%     makes test that fail in this situation long, and as a result
%%     they shrink very slowly.

-include_lib("eqc/include/eqc_statem.hrl").
-include_lib("eqc/include/eqc.hrl").
-ifdef(PULSE).
-export([prop_pulse/0, pulse_instrument/0, pulse_instrument/1]).
-include_lib("pulse/include/pulse.hrl").
-endif.

-export([initial_state/0]).
-export([prop_seq/0]).
-export([work/2, finish_work/1, crash/1, get_element/2, get_status/1]).
-export([new_resource_command/1,
         new_resource_pre/1, new_resource_next/3, new_resource_post/3]).

-record(state, {limit, width, restarts = 0, workers = []}).
-record(worker, {pid, scheduler, queue, status = ready, cmd}).

-import(eqc_statem, [tag/2]).

-compile(nowarn_unused_function).

-define(RESOURCE, resource).
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
    wait_until_quiescent(),
    {Worker, Status} = work0 (Cmd, Scheduler),
    case Status of
	%%overload ->
	    %% Temporary overload is not necessarily a problem--there
	    %% may be workers stopping/dying/being replaced.
	  %%  wait_until_quiescent(),
	    %%work0(Cmd, Scheduler);
	_ ->
	    {Worker, Status}
    end.

work0(Cmd, Scheduler) ->
  status_keeper ! {start_worker, self(), Cmd, Scheduler},
  Worker = receive {start_worker, Worker0} -> Worker0 end,
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
    case get_status0(Worker) of
	blocked ->
	    %% May just not have started yet
	    wait_until_quiescent(),
	    get_status0 (Worker);
	R -> R
    end.

get_status0(Worker) ->
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
      crashed       -> crashed;
      working       -> {working, {call, ?MODULE, get_element, [2, V]}}
    end,
  set_worker_status(S, WPid, NewStatus).

get_status_post(S, [WPid], R) ->
  case (get_worker(S, WPid))#worker.status of
    {finished, Res} -> eq(R, Res);
    blocked         -> eq(R, blocked);
    zombie          -> eq(R, blocked);
    crashed         -> eq(R, crashed);
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
  wait_until_quiescent().

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
  wait_until_quiescent().

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

crash_post(#state{ restarts=Restarts }, [_Pid], _) ->
    %% This is a truly horrible hack!
    %% At the restart limit, the sidejob supervisor is restarted,
    %% which takes longer, and we see non-deterministic effects. In
    %% *sequential* tests, the post-condition is called directly after
    %% the call to crash, and we can tell from the dynamic state
    %% whether or not the restart limit was reached. If so, we give
    %% sidejob a bit more time, to avoid concommitant errors.
    [begin status_keeper ! supervisor_restart,
	   timer:sleep(10)
     end || Restarts==?RESTART_LIMIT], 
    true.

kill_queue(S, Q) ->
  Kill =
    fun(W=#worker{ queue = Q1, status = blocked, cmd = Cmd }) when Q1 == Q ->
          W#worker{ queue = zombie, status = case Cmd of call->crashed; cast->zombie end };
       (W) -> W end,
  S#state{ workers = lists:map(Kill, S#state.workers) }.

kill_all_queues(S) ->
  Kill = fun(W=#worker{ status = Status, cmd = Cmd }) when Status /= {finished,done} -> 
		 W#worker{ queue = zombie, 
			   status = case Cmd of call->crashed; cast->zombie end };
	    (W) -> W end,
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
              {started, Ref, Pid} -> 
		    {working, Pid}
            end
        end,
        From ! {self(), Res}
  end.

%% -- Status keeper ----------------------------------------------------------
%% When running with parallel_commands we need a proxy process that holds the
%% statuses of the workers.
start_status_keeper() ->
  case whereis(status_keeper) of
    undefined -> ok;
    Pid -> unregister(status_keeper), exit(Pid,kill)
  end,
  register(status_keeper, spawn(fun() -> status_keeper([]) end)).

status_keeper(State) ->
  receive
    {start_worker, From, Cmd, Scheduler} ->
      Worker = spawn_opt(fun worker/0, [{scheduler, Scheduler}]),
      monitor(process,Worker),
      Worker ! {Cmd, self()},
      From ! {start_worker, Worker},
      status_keeper([{worker, Worker, [], Cmd} | State]);
    {Worker, Status} when is_pid(Worker) ->
      {worker, Worker, OldStatus, Cmd} = lists:keyfind(Worker, 2, State),
      status_keeper(lists:keystore(Worker, 2, State, 
	{worker, Worker, OldStatus ++ [Status], Cmd}));
    {'DOWN',_,process,Worker,Reason} ->
      [self() ! {Worker,crashed} || Reason/=normal],
      status_keeper(State);
    {get_status, From, Worker} ->
      case lists:keyfind(Worker, 2, State) of
        {worker, Worker, [Status | NewStatus0], Cmd} ->
	  NewStatus = case Status of crashed -> [crashed]; _ -> NewStatus0 end,
          From ! {Worker, Status},
          status_keeper(lists:keystore(Worker, 2, State, 
            {worker, Worker, NewStatus, Cmd}));
        _ ->
	      From ! {Worker, blocked},
	      status_keeper(State)
      end;
    supervisor_restart ->
      %% all workers crash; pending status messages must be discarded
      flush_all_messages(),
      status_keeper([{worker,Worker,
		      [case Msg of
			 {working,_} when Cmd==call -> crashed;
			 {working,_} when Cmd==cast -> blocked;
			  _                         -> Msg
		       end || Msg <- Msgs],
		      Cmd}
		     || {worker,Worker,Msgs,Cmd} <- State])
  end.

flush_all_messages() ->
    receive _ -> flush_all_messages() after 0 -> ok end.

%% -- Property ---------------------------------------------------------------

prop_seq() ->
  ?FORALL(Repetitions,?SHRINK(1,[100]),
  ?FORALL(Cmds, commands(?MODULE),
  ?ALWAYS(Repetitions,
  ?TIMEOUT(?TIMEOUT,
  ?SOMETIMES(1,%10,
  begin
    cleanup(),
    HSR={_, S, R} = run_commands(?MODULE, Cmds),
    [ exit(Pid, kill) || #worker{ pid = Pid } <- S#state.workers, is_pid(Pid) ],
    aggregate(command_names(Cmds),
    pretty_commands(?MODULE, Cmds, HSR,
      R == ok))
  end))))).

%% Because these tests try to wait for quiescence after each
%% operation, it is not really meaninful to run parallel tests.

%% prop_par() ->
%%   ?FORALL(Cmds, parallel_commands(?MODULE),
%%   ?TIMEOUT(?TIMEOUT,
%%   % ?SOMETIMES(4,
%%   begin
%%     cleanup(),
%%     HSR={SeqH, ParH, R} = run_parallel_commands(?MODULE, Cmds),
%%     kill_all_pids({SeqH, ParH}),
%%     aggregate(command_names(Cmds),
%%     pretty_commands(?MODULE, Cmds, HSR,
%%       R == ok))
%%   end)).
%% 
%% -ifdef(PULSE).
%% prop_pulse() ->
%%   ?SETUP(fun() -> N = erlang:system_flag(schedulers_online, 1),
%%                   fun() -> erlang:system_flag(schedulers_online, N) end end,
%%   ?FORALL(Cmds, parallel_commands(?MODULE),
%%   ?PULSE(HSR={_, _, R},
%%     begin
%%       cleanup(),
%%       run_parallel_commands(?MODULE, Cmds)
%%     end,
%%     aggregate(command_names(Cmds),
%%     pretty_commands(?MODULE, Cmds, HSR,
%%       R == ok))))).
%% -endif.

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

%% Wait for quiescence: to get deterministic testing, we need to let
%% sidejob finish what it is doing.

busy_processes() ->
    [Pid || Pid <- processes(),
	    {status,Status} <- [erlang:process_info(Pid,status)],
	    Status /= waiting,
	    Status /= suspended].

quiescent() ->
    busy_processes() == [self()].

wait_until_quiescent() ->
    timer:sleep(2),
    case quiescent() of
	true ->
	    ok;
	false ->
	    %% This happens regularly
	    wait_until_quiescent()
    end.
