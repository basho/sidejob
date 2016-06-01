-module(supervisor_test).
-export([fail_to_start_link/0]).
-include_lib("eunit/include/eunit.hrl").

-define(RESOURCE, supervisor_test_resource).

% Setup a sidejob resource and start a child that links to the parent but doesn't return {ok, Pid}.
% The supervisor should not shut down.
spurious_exit_test() ->
  {ok, _} = application:ensure_all_started(sidejob),
  {ok, _} = sidejob:new_resource(?RESOURCE, sidejob_supervisor, 5, 1),
  {WorkerReg} = ?RESOURCE:workers(),
  WorkerPid = whereis(WorkerReg),
  ?assert(is_process_alive(WorkerPid)),
  {ok, undefined} = sidejob_supervisor:start_child(?RESOURCE, ?MODULE, fail_to_start_link, []),
  sidejob_supervisor:which_children(?RESOURCE), % wait for sidejob_supervisor to process the EXIT
  ?assert(is_process_alive(WorkerPid)).

fail_to_start_link() ->
  spawn_link(fun () -> ok end),
  ignore.
