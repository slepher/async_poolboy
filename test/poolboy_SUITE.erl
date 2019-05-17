-module(poolboy_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @spec suite() -> Info
%% Info = [tuple()]
%% @end
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{seconds,30}}].

%%--------------------------------------------------------------------
%% @spec init_per_suite(Config0) ->
%%     Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    dbg:tracer(),
    dbg:p(all, [c]),
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_suite(Config0) -> term() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    dbg:ctpl(),
    dbg:stop(),
    ok.

%%--------------------------------------------------------------------
%% @spec init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_group(_GroupName, Config) ->
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_group(GroupName, Config0) ->
%%               term() | {save_config,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_group(_GroupName, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% @spec init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_testcase(TestCase, Config0) ->
%%               term() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% @spec groups() -> [Group]
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%% Shuffle = shuffle | {shuffle,{integer(),integer(),integer()}}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%% N = integer() | forever
%% @end
%%--------------------------------------------------------------------
groups() ->
    [].

%%--------------------------------------------------------------------
%% @spec all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
all() -> 
    [pool_startup, pool_overflow, pool_empty, pool_empty_no_overflow, worker_death,
     worker_death_while_full, worker_death_while_full_no_overflow,pool_full_nonblocking_no_overflow,
     pool_full_nonblocking,owner_death,checkin_after_exception_in_transaction,
     pool_returns_status, demonitors_previously_waiting_processes,
     demonitors_when_checkout_cancelled, worker_call_child, worker_call_child_twice,
     child_worker_death,
     default_strategy_lifo,lifo_strategy, fifo_strategy, reuses_waiting_monitor_on_worker_exit,
     transaction_timeout_without_exit,transaction_timeout,
     promise_checkout, promise_transaction, promise_invalid_transaction, promise_transaction_with_exception
    ].

%% Tell a worker to exit and await its impending doom.
kill_worker(Pid) ->
    erlang:monitor(process, Pid),
    pool_call(Pid, die),
    receive
        {'DOWN', _, process, Pid, _} ->
            ok
    end.

checkin_worker(Pid, Worker) ->
    %% There's no easy way to wait for a checkin to complete, because it's
    %% async and the supervisor may kill the process if it was an overflow
    %% worker. The only solution seems to be a nasty hardcoded sleep.
    async_poolboy:checkin(Pid, Worker),
    timer:sleep(500).

transaction_timeout_without_exit(_Config) ->
    {ok, Pid} = new_pool(1, 0),
    ?assertEqual({ready,1,0,0}, pool_call(Pid, status)),
    WorkerList = pool_call(Pid, get_all_workers),
    ?assertMatch([_], WorkerList),
    spawn(async_poolboy, transaction, [Pid,
        fun(Worker) ->
            ok = pool_call(Worker, work)
        end,
        0]),
    timer:sleep(100),
    ?assertEqual(WorkerList, pool_call(Pid, get_all_workers)),
    ?assertEqual({ready,1,0,0}, pool_call(Pid, status)).

transaction_timeout(_Config) ->
    {ok, Pid} = new_pool(1, 0),
    ?assertEqual({ready,1,0,0}, pool_call(Pid, status)),
    WorkerList = pool_call(Pid, get_all_workers),
    ?assertMatch([_], WorkerList),
    ?assertExit(
        {timeout, _},
        async_poolboy:transaction(Pid,
            fun(Worker) ->
                ok = pool_call(Worker, work)
            end,
            0)),
    ?assertEqual(WorkerList, pool_call(Pid, get_all_workers)),
    ?assertEqual({ready,1,0,0}, pool_call(Pid, status)).


pool_startup(_Config) ->
    %% Check basic pool operation.
    {ok, Pid} = new_pool(10, 5),
    ?assertEqual(10, queue:len(pool_call(Pid, get_avail_workers))),
    async_poolboy:checkout(Pid),
    ?assertEqual(9, queue:len(pool_call(Pid, get_avail_workers))),
    Worker = async_poolboy:checkout(Pid),
    ?assertEqual(8, queue:len(pool_call(Pid, get_avail_workers))),
    checkin_worker(Pid, Worker),
    ?assertEqual(9, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(1, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

pool_overflow(_Config) ->
    %% Check that the pool overflows properly.
    {ok, Pid} = new_pool(5, 5),
    Workers = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    [A, B, C, D, E, F, G] = Workers,
    checkin_worker(Pid, A),
    checkin_worker(Pid, B),
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(2, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, E),
    checkin_worker(Pid, F),
    ?assertEqual(4, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, G),
    ?assertEqual(5, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(0, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

pool_empty(_Config) ->
    %% Checks that the the pool handles the empty condition correctly when
    %% overflow is enabled.
    {ok, Pid} = new_pool(5, 2),
    Workers = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    [A, B, C, D, E, F, G] = Workers,
    Self = self(),
    spawn(fun() ->
        Worker = async_poolboy:checkout(Pid),
        Self ! got_worker,
        checkin_worker(Pid, Worker)
    end),

    %% Spawned process should block waiting for worker to be available.
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    checkin_worker(Pid, A),
    checkin_worker(Pid, B),

    %% Spawned process should have been able to obtain a worker.
    receive
        got_worker -> ?assert(true)
    after
        500 -> ?assert(false)
    end,
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(2, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, E),
    checkin_worker(Pid, F),
    ?assertEqual(4, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, G),
    ?assertEqual(5, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(0, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

pool_empty_no_overflow(_Config) ->
    %% Checks the pool handles the empty condition properly when overflow is
    %% disabled.
    {ok, Pid} = new_pool(5, 0),
    Workers = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    [A, B, C, D, E] = Workers,
    Self = self(),
    spawn(fun() ->
        Worker = async_poolboy:checkout(Pid),
        Self ! got_worker,
        checkin_worker(Pid, Worker)
    end),

    %% Spawned process should block waiting for worker to be available.
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    checkin_worker(Pid, A),
    checkin_worker(Pid, B),

    %% Spawned process should have been able to obtain a worker.
    receive
        got_worker -> ?assert(true)
    after
        500 -> ?assert(false)
    end,
    ?assertEqual(2, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, C),
    checkin_worker(Pid, D),
    ?assertEqual(4, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    checkin_worker(Pid, E),
    ?assertEqual(5, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(0, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

worker_call_child(_Config) ->
    {ok, Pid} = new_pool(5, 2),
    [A|_Workers] = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    Self = self(),
    Action = 
        fun() ->
                async_poolboy:transaction(
                  Pid,
                  fun(Worker) ->
                          gen_server:call(Worker, {action, fun() -> timer:sleep(800) end})
                  end)
        end,
    spawn(
      fun() ->
              Result = gen_server:call(A, {action, Action}, 5000),
              Self ! {got_worker, Result}
      end),
    timer:sleep(500),
    ?assertEqual(8, length(pool_call(Pid, get_all_workers))),
    receive
        {got_worker, WorkerResult} -> 
            ?assert(true),
            ?assertEqual(ok, WorkerResult)
    after
        500 -> ?assert(false)
    end,
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    ok.

worker_call_child_twice(_Config) ->
    {ok, Pid} = new_pool(5, 2),
    [A,B|_Workers] = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    Self = self(),
    Action = 
        fun() ->
                async_poolboy:checkout(Pid),
                Self ! got_worker_1,
                async_poolboy:checkout(Pid),
                Self ! got_worker_2
        end,
    spawn(
      fun() ->
              gen_server:call(A, {action, Action}, infinity)
      end),
    timer:sleep(200),
    ?assertEqual(8, length(pool_call(Pid, get_all_workers))),
    receive
        got_worker_1 ->
            ?assert(true)
    after
        500 -> ?assert(false)
    end,
    receive
        got_worker_2 ->
            ?assert(false)
    after
        500 -> ?assert(true)
    end,
    ?assertEqual(8, length(pool_call(Pid, get_all_workers))),
    async_poolboy:checkin(Pid, B),
    receive
        got_worker_2 ->
            ?assert(true)
    after
        500 -> ?assert(false)
    end,    
    ok.

worker_death(_Config) ->
    %% Check that dead workers are only restarted when the pool is not full
    %% and the overflow count is 0. Meaning, don't restart overflow workers.
    {ok, Pid} = new_pool(5, 2),
    Worker = async_poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, queue:len(pool_call(Pid, get_avail_workers))),
    [A, B, C|_Workers] = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    kill_worker(A),
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(6, length(pool_call(Pid, get_all_workers))),
    kill_worker(B),
    kill_worker(C),
    ?assertEqual(1, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(4, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

child_worker_death(_Config) ->
    {ok, Pid} = new_pool(5, 2),
    [A|_Workers] = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    Self = self(),
    Action = 
        fun() ->
                ChildWorker1 = async_poolboy:checkout(Pid),
                timer:sleep(1000),
                kill_worker(ChildWorker1),
                async_poolboy:checkout(Pid),
                Self ! got_worker
        end,
    spawn(
      fun() ->
              gen_server:call(A, {action, Action}, infinity)
      end),
    timer:sleep(200),
    ?assertEqual(8, length(pool_call(Pid, get_all_workers))),
    receive
        got_worker ->
            ?assert(false)
    after
        500 -> ?assert(true)
    end,
    ?assertEqual(8, length(pool_call(Pid, get_all_workers))),
    receive
        got_worker ->
            ?assert(true)
    after
        500 -> ?assert(false)
    end,
    ?assertEqual(8, length(pool_call(Pid, get_all_workers))),
    ok.

worker_with_child_death(_Config) ->
    {ok, Pid} = new_pool(5, 3),
    [A|_Workers] = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    Action = 
        fun() ->
                async_poolboy:checkout(Pid),
                async_poolboy:checkout(Pid)
        end,
    spawn(
      fun() ->
              gen_server:call(A, {action, Action}, infinity)
      end),
    timer:sleep(200),
    ?assertEqual(9, length(pool_call(Pid, get_all_workers))),
    exit(A, die),
    timer:sleep(200),
    ?assertEqual(6, length(pool_call(Pid, get_all_workers))),
    ok.

worker_death_while_full(_Config) ->
    %% Check that if a worker dies while the pool is full and there is a
    %% queued checkout, a new worker is started and the checkout serviced.
    %% If there are no queued checkouts, a new worker is not started.
    {ok, Pid} = new_pool(5, 2),
    Worker = async_poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, queue:len(pool_call(Pid, get_avail_workers))),
    [A, B|_Workers] = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    Self = self(),
    spawn(fun() ->
        async_poolboy:checkout(Pid),
        Self ! got_worker,
        %% XXX: Don't release the worker. We want to also test what happens
        %% when the worker pool is full and a worker dies with no queued
        %% checkouts.
        timer:sleep(5000)
    end),

    %% Spawned process should block waiting for worker to be available.
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    kill_worker(A),

    %% Spawned process should have been able to obtain a worker.
    receive
        got_worker -> ?assert(true)
    after
        1000 -> ?assert(false)
    end,
    kill_worker(B),
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(6, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(6, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

worker_death_while_full_no_overflow(_Config) ->
    %% Check that if a worker dies while the pool is full and there's no
    %% overflow, a new worker is started unconditionally and any queued
    %% checkouts are serviced.
    {ok, Pid} = new_pool(5, 0),
    Worker = async_poolboy:checkout(Pid),
    kill_worker(Worker),
    ?assertEqual(5, queue:len(pool_call(Pid, get_avail_workers))),
    [A, B, C|_Workers] = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    Self = self(),
    spawn(fun() ->
        async_poolboy:checkout(Pid),
        Self ! got_worker,
        %% XXX: Do not release, need to also test when worker dies and no
        %% checkouts queued.
        timer:sleep(5000)
    end),

    %% Spawned process should block waiting for worker to be available.
    receive
        got_worker -> ?assert(false)
    after
        500 -> ?assert(true)
    end,
    kill_worker(A),

    %% Spawned process should have been able to obtain a worker.
    receive
        got_worker -> ?assert(true)
    after
        1000 -> ?assert(false)
    end,
    kill_worker(B),
    ?assertEqual(1, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    kill_worker(C),
    ?assertEqual(2, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(3, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

pool_full_nonblocking_no_overflow(_Config) ->
    %% Check that when the pool is full, checkouts return 'full' when the
    %% option to use non-blocking checkouts is used.
    {ok, Pid} = new_pool(5, 0),
    Workers = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 4)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(full, async_poolboy:checkout(Pid, false)),
    ?assertEqual(full, async_poolboy:checkout(Pid, false)),
    A = hd(Workers),
    checkin_worker(Pid, A),
    ?assertEqual(A, async_poolboy:checkout(Pid)),
    ?assertEqual(5, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

pool_full_nonblocking(_Config) ->
    %% Check that when the pool is full, checkouts return 'full' when the
    %% option to use non-blocking checkouts is used.
    {ok, Pid} = new_pool(5, 5),
    Workers = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 9)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(10, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(full, async_poolboy:checkout(Pid, false)),
    A = hd(Workers),
    checkin_worker(Pid, A),
    NewWorker = async_poolboy:checkout(Pid, false),
    ?assertEqual(false, is_process_alive(A)), %% Overflow workers get shutdown
    ?assert(is_pid(NewWorker)),
    ?assertEqual(full, async_poolboy:checkout(Pid, false)),
    ?assertEqual(10, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

owner_death(_Config) ->
    %% Check that a dead owner (a process that dies with a worker checked out)
    %% causes the pool to dismiss the worker and prune the state space.
    {ok, Pid} = new_pool(5, 5),
    spawn(fun() ->
        async_poolboy:checkout(Pid),
        receive after 500 -> exit(normal) end
    end),
    timer:sleep(1000),
    ?assertEqual(5, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(5, length(pool_call(Pid, get_all_workers))),
    ?assertEqual(0, length(pool_call(Pid, get_all_monitors))),
    ok = pool_call(Pid, stop).

checkin_after_exception_in_transaction(_Config) ->
    {ok, Pool} = new_pool(2, 0),
    ?assertEqual(2, queue:len(pool_call(Pool, get_avail_workers))),
    Tx = fun(Worker) ->
        ?assert(is_pid(Worker)),
        ?assertEqual(1, queue:len(pool_call(Pool, get_avail_workers))),
        throw(it_on_the_ground),
        ?assert(false)
    end,
    try
        async_poolboy:transaction(Pool, Tx)
    catch
        throw:it_on_the_ground -> ok
    end,
    ?assertEqual(2, queue:len(pool_call(Pool, get_avail_workers))),
    ok = pool_call(Pool, stop).

pool_returns_status(_Config) ->
    {ok, Pool} = new_pool(2, 0),
    ?assertEqual({ready, 2, 0, 0}, async_poolboy:status(Pool)),
    async_poolboy:checkout(Pool),
    ?assertEqual({ready, 1, 0, 1}, async_poolboy:status(Pool)),
    async_poolboy:checkout(Pool),
    ?assertEqual({full, 0, 0, 2}, async_poolboy:status(Pool)),
    ok = pool_call(Pool, stop),

    {ok, Pool2} = new_pool(1, 1),
    ?assertEqual({ready, 1, 0, 0}, async_poolboy:status(Pool2)),
    async_poolboy:checkout(Pool2),
    ?assertEqual({overflow, 0, 0, 1}, async_poolboy:status(Pool2)),
    async_poolboy:checkout(Pool2),
    ?assertEqual({full, 0, 1, 2}, async_poolboy:status(Pool2)),
    ok = pool_call(Pool2, stop),

    {ok, Pool3} = new_pool(0, 2),
    ?assertEqual({overflow, 0, 0, 0}, async_poolboy:status(Pool3)),
    async_poolboy:checkout(Pool3),
    ?assertEqual({overflow, 0, 1, 1}, async_poolboy:status(Pool3)),
    async_poolboy:checkout(Pool3),
    ?assertEqual({full, 0, 2, 2}, async_poolboy:status(Pool3)),
    ok = pool_call(Pool3, stop),

    {ok, Pool4} = new_pool(0, 0),
    ?assertEqual({full, 0, 0, 0}, async_poolboy:status(Pool4)),
    ok = pool_call(Pool4, stop).

demonitors_previously_waiting_processes(_Config) ->
    {ok, Pool} = new_pool(1,0),
    Self = self(),
    Pid = spawn(fun() ->
        W = async_poolboy:checkout(Pool),
        Self ! ok,
        timer:sleep(500),
        async_poolboy:checkin(Pool, W),
        receive ok -> ok end
    end),
    receive ok -> ok end,
    Worker = async_poolboy:checkout(Pool),
    ?assertEqual(1, length(get_monitors(Pool))),
    async_poolboy:checkin(Pool, Worker),
    timer:sleep(500),
    ?assertEqual(0, length(get_monitors(Pool))),
    Pid ! ok,
    ok = pool_call(Pool, stop).

demonitors_when_checkout_cancelled(_Config) ->
    {ok, Pool} = new_pool(1,0),
    Self = self(),
    Pid = spawn(fun() ->
        async_poolboy:checkout(Pool),
        _ = (catch async_poolboy:checkout(Pool, true, 1000)),
        Self ! ok,
        receive ok -> ok end
    end),
    timer:sleep(500),
    ?assertEqual(2, length(get_monitors(Pool))),
    receive ok -> ok end,
    ?assertEqual(1, length(get_monitors(Pool))),
    Pid ! ok,
    ok = pool_call(Pool, stop).

default_strategy_lifo(_Config) ->
    %% Default strategy is LIFO
    {ok, Pid} = new_pool(2, 0),
    Worker1 = async_poolboy:checkout(Pid),
    ok = async_poolboy:checkin(Pid, Worker1),
    Worker1 = async_poolboy:checkout(Pid),
    async_poolboy:stop(Pid).

lifo_strategy(_Config) ->
    {ok, Pid} = new_pool(2, 0, lifo),
    Worker1 = async_poolboy:checkout(Pid),
    ok = async_poolboy:checkin(Pid, Worker1),
    Worker1 = async_poolboy:checkout(Pid),
    async_poolboy:stop(Pid).

fifo_strategy(_Config) ->
    {ok, Pid} = new_pool(2, 0, fifo),
    Worker1 = async_poolboy:checkout(Pid),
    ok = async_poolboy:checkin(Pid, Worker1),
    Worker2 = async_poolboy:checkout(Pid),
    ?assert(Worker1 =/= Worker2),
    Worker1 = async_poolboy:checkout(Pid),
    async_poolboy:stop(Pid).

reuses_waiting_monitor_on_worker_exit(_Config) ->
    {ok, Pool} = new_pool(1,0),

    Self = self(),
    Pid = spawn(fun() ->
        Worker = async_poolboy:checkout(Pool),
        Self ! {worker, Worker},
        async_poolboy:checkout(Pool),
        receive ok -> ok end
    end),

    Worker = receive {worker, Worker1} -> Worker1 end,
    Ref = monitor(process, Worker),
    exit(Worker, kill),
    receive
        {'DOWN', Ref, _, _, _} ->
            ok
    end,

    ?assertEqual(1, length(get_monitors(Pool))),

    Pid ! ok,
    ok = pool_call(Pool, stop).

promise_checkout(_Config) ->
    {ok, Pid} = new_pool(5, 2),
    _Workers = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    Promise = async_poolboy:promise_checkout(Pid, #{timeout => 500}),
    Result = async_m:wait(Promise),
    ?assertEqual({error, timeout}, Result),
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    ok.

promise_transaction(_Config) ->
    dbg:tpl(async_poolboy, checkout, cx),
    dbg:tpl(async_poolboy, checkin, cx),
    {ok, Pid} = new_pool(5, 2),
    [A|_Workers] = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    Promise = async_poolboy:promise_call(Pid, {action, fun() -> timer:sleep(500) end}),
    async_poolboy:checkin(Pid, A),
    Value = async_m:wait(Promise),
    ?assertEqual({ok, ok}, Value),
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(6, length(pool_call(Pid, get_all_workers))),
    ok.

promise_transaction_with_exception(_Config) ->
    {ok, Pid} = new_pool(5, 2),
    [A|_Workers] = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    Promise = 
        async_poolboy:promise_transaction(
          Pid, 
          fun(_Worker) ->
                  exit(die)
          end),
    async_poolboy:checkin(Pid, A),
    ?assertExit(die, async_m:wait(Promise), "wait dies"),
    timer:sleep(200),
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(6, length(pool_call(Pid, get_all_workers))),
    ok.

promise_invalid_transaction(_Config) ->
    {ok, Pid} = new_pool(5, 2),
    [A|_Workers] = [async_poolboy:checkout(Pid) || _ <- lists:seq(0, 6)],
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(7, length(pool_call(Pid, get_all_workers))),
    Promise = 
        async_poolboy:promise_transaction(
          Pid, 
          fun(_Worker) ->
                  ok
          end),
    async_poolboy:checkin(Pid, A),
    ?assertExit({async_promise_expected, ok}, async_m:wait(Promise), "wait dies"),
    timer:sleep(200),
    ?assertEqual(0, queue:len(pool_call(Pid, get_avail_workers))),
    ?assertEqual(6, length(pool_call(Pid, get_all_workers))),
    ok.

get_monitors(Pid) ->
    %% Synchronise with the Pid to ensure it has handled all expected work.
    _ = sys:get_status(Pid),
    [{monitors, Monitors}] = erlang:process_info(Pid, [monitors]),
    Monitors.

new_pool(Size, MaxOverflow) ->
    async_poolboy:start_link([{name, {local, poolboy_test}},
                              {worker_module, poolboy_test_worker},
                              {size, Size}, {max_overflow, MaxOverflow}]).

new_pool(Size, MaxOverflow, Strategy) ->
    async_poolboy:start_link([{name, {local, poolboy_test}},
                              {worker_module, poolboy_test_worker},
                              {size, Size}, {max_overflow, MaxOverflow},
                              {strategy, Strategy}]).

pool_call(ServerRef, Request) ->
    gen_server:call(ServerRef, Request).
