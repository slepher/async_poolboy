%%%-------------------------------------------------------------------
%%% @author Chen Slepher <slepheric@gmail.com>
%%% @copyright (C) 2019, Chen Slepher
%%% @doc
%%%
%%% @end
%%% Created : 11 Apr 2019 by Chen Slepher <slepheric@gmail.com>
%%%-------------------------------------------------------------------
%%% these codes is based on https://github.com/devinus/poolboy/blob/master/src/poolboy.erl
-module(async_poolboy).

-behaviour(gen_server).

-include_lib("erlando/include/do.hrl").
-include_lib("erlando/include/gen_fun.hrl").

-export([promise_checkout/1, promise_checkout/2, 
         call/2, call/3, 
         promise_call/2, promise_call/3, 
         promise_transaction/2, promise_transaction/3]).

-export([child_spec/2, child_spec/3, child_spec/4, start/1, start/2, start_link/1, start_link/2]).

%% imported from poolboy.
-gen_fun(#{remote => poolboy, functions => [checkout/1, checkout/2, checkout/3, checkin/2, transaction/2,
                                            transaction/3, stop/1, status/1]}).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-define(TIMEOUT, 5000).

-define(SERVER, ?MODULE).

-ifdef(pre17).
-type pid_queue() :: queue().
-else.
-type pid_queue() :: queue:queue().
-endif.

% Copied from gen:start_ret/0
-type start_ret() :: {'ok', pid()} | 'ignore' | {'error', term()}.

-record(state, {
    supervisor :: undefined | pid(),
    workers :: undefined | pid_queue(),
    working_status = poolboy_working_status:new(),
    waiting :: pid_queue(),
    monitors :: ets:tid(),
    size = 5 :: non_neg_integer(),
    overflow = 0 :: non_neg_integer(),
    max_overflow = 10 :: non_neg_integer(),
    strategy = lifo :: lifo | fifo
}).

%%%===================================================================
%%% API
%%%===================================================================
-spec child_spec(PoolId :: term(), PoolArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(PoolId, PoolArgs) ->
    child_spec(PoolId, PoolArgs, []).

-spec child_spec(PoolId :: term(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist())
    -> supervisor:child_spec().
child_spec(PoolId, PoolArgs, WorkerArgs) ->
    child_spec(PoolId, PoolArgs, WorkerArgs, tuple).

-spec child_spec(PoolId :: term(),
                 PoolArgs :: proplists:proplist(),
                 WorkerArgs :: proplists:proplist(),
                 ChildSpecFormat :: 'tuple' | 'map')
    -> supervisor:child_spec().
child_spec(PoolId, PoolArgs, WorkerArgs, tuple) ->
    {PoolId, {?MODULE, start_link, [PoolArgs, WorkerArgs]},
     permanent, 5000, worker, [?MODULE]};
child_spec(PoolId, PoolArgs, WorkerArgs, map) ->
    #{id => PoolId,
      start => {?MODULE, start_link, [PoolArgs, WorkerArgs]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [?MODULE]}.

-spec start(PoolArgs :: proplists:proplist())
    -> start_ret().
start(PoolArgs) ->
    start(PoolArgs, PoolArgs).

-spec start(PoolArgs :: proplists:proplist(),
            WorkerArgs:: proplists:proplist())
    -> start_ret().
start(PoolArgs, WorkerArgs) ->
    start_pool(start, PoolArgs, WorkerArgs).

-spec start_link(PoolArgs :: proplists:proplist())
    -> start_ret().
start_link(PoolArgs)  ->
    %% for backwards compatability, pass the pool args as the worker args as well
    start_link(PoolArgs, PoolArgs).

-spec start_link(PoolArgs :: proplists:proplist(),
                 WorkerArgs:: proplists:proplist())
    -> start_ret().
start_link(PoolArgs, WorkerArgs)  ->
    start_pool(start_link, PoolArgs, WorkerArgs).

promise_checkout(Pool) ->
    promise_checkout(Pool, #{}).

promise_checkout(Pool, Options) ->
    Block = maps:get(block, Options, true),
    Timeout = maps:get(timeout, Options, infinity),
    Ref = make_ref(),
    do([async_m ||
           Reply <- async_m:lift_final_reply(async_gen_server:promise_call(Pool, {checkout, Ref, Block}, Timeout)),
           case Reply of
               {error, Reason} ->
                   gen_server:cast(Pool, {cancel_waiting, Ref}),
                   fail(Reason);
               Worker ->
                   async_m:pure_return(Worker)
           end
       ]).

call(Pool, Args) ->
    call(Pool, Args, #{}).

call(Pool, Args, Options) ->
    Promise = promise_call(Pool, Args, Options),
    async_m:wait(Promise).

promise_call(Pool, Args) ->
    promise_call(Pool, Args, #{}).

promise_call(Pool, Args, Options) ->
    promise_transaction(Pool, fun(Worker) -> async_gen_server:promise_call(Worker, Args) end, Options).

promise_transaction(Pool, Fun) ->
    promise_transaction(Pool, Fun, #{}).

promise_transaction(Pool, Fun, Options) ->
    do([async_m ||
           Worker <- promise_checkout(Pool, Options),
           case try_fun(Pool, Fun, Worker) of
               {async_t, _} = Async ->
                   do([async_m || 
                          Value <- Async,
                          ok = checkin(Pool, Worker),
                          return(Value)
                      ]);
               Other ->
                   ok = checkin(Pool, Worker),
                   exit({async_promise_expected, Other})
           end
       ]).
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid :: pid()} |
                      {error, Error :: {already_started, pid()}} |
                      {error, Error :: term()} |
                      ignore.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, State :: term()} |
                              {ok, State :: term(), Timeout :: timeout()} |
                              {ok, State :: term(), hibernate} |
                              {stop, Reason :: term()} |
                              ignore.
init({PoolArgs, WorkerArgs}) ->
    process_flag(trap_exit, true),
    Waiting = queue:new(),
    Monitors = ets:new(monitors, [private]),
    init(PoolArgs, WorkerArgs, #state{waiting = Waiting, monitors = Monitors}).

init([{worker_module, Mod} | Rest], WorkerArgs, State) when is_atom(Mod) ->
    {ok, Sup} = poolboy_sup:start_link(Mod, WorkerArgs),
    init(Rest, WorkerArgs, State#state{supervisor = Sup});
init([{size, Size} | Rest], WorkerArgs, State) when is_integer(Size) ->
    init(Rest, WorkerArgs, State#state{size = Size});
init([{max_overflow, MaxOverflow} | Rest], WorkerArgs, State) when is_integer(MaxOverflow) ->
    init(Rest, WorkerArgs, State#state{max_overflow = MaxOverflow});
init([{strategy, lifo} | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State#state{strategy = lifo});
init([{strategy, fifo} | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State#state{strategy = fifo});
init([_ | Rest], WorkerArgs, State) ->
    init(Rest, WorkerArgs, State);
init([], _WorkerArgs, #state{size = Size, supervisor = Sup} = State) ->
    Workers = prepopulate(Size, Sup),
    {ok, State#state{workers = Workers}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
                         {reply, Reply :: term(), NewState :: term()} |
                         {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()} |
                         {reply, Reply :: term(), NewState :: term(), hibernate} |
                         {noreply, NewState :: term()} |
                         {noreply, NewState :: term(), Timeout :: timeout()} |
                         {noreply, NewState :: term(), hibernate} |
                         {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
                         {stop, Reason :: term(), NewState :: term()}.
handle_call({checkout, CRef, Block}, {FromPid, _} = From, State) ->
    MRef = erlang:monitor(process, FromPid),
    State1 = handle_checkout(From, CRef, MRef, Block, State),
    {noreply, State1};

handle_call(status, _From, State) ->
    #state{workers = Workers,
           monitors = Monitors,
           overflow = Overflow} = State,
    StateName = state_name(State),
    {reply, {StateName, queue:len(Workers), Overflow, ets:info(Monitors, size)}, State};

handle_call(get_avail_workers, _From, State) ->
    Workers = State#state.workers,
    {reply, Workers, State};

handle_call(get_all_workers, _From, State) ->
    Sup = State#state.supervisor,
    WorkerList = supervisor:which_children(Sup),
    {reply, WorkerList, State};

handle_call(get_all_monitors, _From, State) ->
    Monitors = ets:select(State#state.monitors,
                          [{{'$1', '_', '$2'}, [], [{{'$1', '$2'}}]}]),
    {reply, Monitors, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(Request, _From, State) ->
    Reply = {error, {invalid_request, Request}},
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) ->
                         {noreply, NewState :: term()} |
                         {noreply, NewState :: term(), Timeout :: timeout()} |
                         {noreply, NewState :: term(), hibernate} |
                         {stop, Reason :: term(), NewState :: term()}.
handle_cast({checkin, Pid}, State = #state{monitors = Monitors}) ->
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            {noreply, State}
    end;

handle_cast({cancel_waiting, CRef}, State) ->
    case ets:match(State#state.monitors, {'$1', CRef, '$2'}) of
        [[Pid, MRef]] ->
            demonitor(MRef, [flush]),
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            Cancel = fun({_, Ref, MRef}) when Ref =:= CRef ->
                             demonitor(MRef, [flush]),
                             false;
                        (_) ->
                             true
                     end,
            Waiting = queue:filter(Cancel, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: term()) ->
                         {noreply, NewState :: term()} |
                         {noreply, NewState :: term(), Timeout :: timeout()} |
                         {noreply, NewState :: term(), hibernate} |
                         {stop, Reason :: normal | term(), NewState :: term()}.
handle_info({'DOWN', MRef, _, _, _}, State) ->
    case ets:match(State#state.monitors, {'$1', '_', MRef}) of
        [[Pid]] ->
            true = ets:delete(State#state.monitors, Pid),
            NewState = handle_checkin(Pid, State),
            {noreply, NewState};
        [] ->
            Waiting = queue:filter(fun ({_, _, R}) -> R =/= MRef end, State#state.waiting),
            {noreply, State#state{waiting = Waiting}}
    end;

handle_info({'EXIT', Pid, _Reason}, State) ->
    #state{supervisor = Sup,
           monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
        [{Pid, _, MRef}] ->
            true = erlang:demonitor(MRef),
            true = ets:delete(Monitors, Pid),
            NewState = handle_checkin(Pid, true, State),
            {noreply, NewState};
        [] ->
            case queue:member(Pid, State#state.workers) of
                true ->
                    W = filter_worker_by_pid(Pid, State#state.workers),
                    {noreply, State#state{workers = queue:in(new_worker(Sup), W)}};
                false ->
                    {noreply, State}
            end
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
                State :: term()) -> any().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()},
                  State :: term(),
                  Extra :: term()) -> {ok, NewState :: term()} |
                                      {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for changing the form and appearance
%% of gen_server status when it is returned from sys:get_status/1,2
%% or when it appears in termination error logs.
%% @end
%%--------------------------------------------------------------------
-spec format_status(Opt :: normal | terminate,
                    Status :: list()) -> Status :: term().
format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
try_fun(Pool, Fun, Worker) ->
    try 
        Fun(Worker)
    catch
        Raise:Exception:Stacktrace ->
            ok = checkin(Pool, Worker),
            erlang:raise(Raise, Exception, Stacktrace)
    end.

start_pool(StartFun, PoolArgs, WorkerArgs) ->
    case proplists:get_value(name, PoolArgs) of
        undefined ->
            gen_server:StartFun(?MODULE, {PoolArgs, WorkerArgs}, []);
        Name ->
            gen_server:StartFun(Name, ?MODULE, {PoolArgs, WorkerArgs}, [])
    end.

new_worker(Sup, _Pid, true) ->
    new_worker(Sup);
new_worker(_Sup, Pid, false) ->
    Pid.

new_worker(Sup) ->
    {ok, Pid} = supervisor:start_child(Sup, []),
    true = link(Pid),
    Pid.

dismiss_worker(_Sup, _Pid, true) ->
    ok;
dismiss_worker(Sup, Pid, false) ->
    dismiss_worker(Sup, Pid).

dismiss_worker(Sup, Pid) ->
    true = unlink(Pid),
    supervisor:terminate_child(Sup, Pid).

get_worker_with_strategy(Workers, fifo) ->
    queue:out(Workers);
get_worker_with_strategy(Workers, lifo) ->
    queue:out_r(Workers).

filter_worker_by_pid(Pid, Workers) ->
    queue:filter(fun (WPid) -> WPid =/= Pid end, Workers).

prepopulate(N, _Sup) when N < 1 ->
    queue:new();
prepopulate(N, Sup) ->
    prepopulate(N, Sup, queue:new()).

prepopulate(0, _Sup, Workers) ->
    Workers;
prepopulate(N, Sup, Workers) ->
    prepopulate(N-1, Sup, queue:in(new_worker(Sup), Workers)).

handle_checkout({FromPid, _} = From, CRef, MRef, Block,
                #state{supervisor = Sup, 
                       workers = Workers,
                       overflow = Overflow,
                       max_overflow = MaxOverflow,
                       strategy = Strategy,
                       working_status = WorkingStatus} = State) ->
    case poolboy_working_status:should_spawn(FromPid, WorkingStatus) of
        true ->
            Pid = new_worker(Sup),
            add_worker(Pid, From, CRef, MRef, State);
        false ->
            case get_worker_with_strategy(Workers, Strategy) of
                {{value, Pid},  Left} ->
                    add_worker(Pid, From, CRef, MRef, State#state{workers = Left});
                {empty, _Left} when MaxOverflow > 0, Overflow < MaxOverflow ->
                    Pid = new_worker(Sup),
                    add_worker(Pid, From, CRef, MRef, State#state{overflow = Overflow + 1});
                {empty, _Left} when Block =:= false ->
                    gen_server:reply(From, full),
                    State;
                {empty, _Left} ->
                    Waiting = queue:in({From, CRef, MRef}, State#state.waiting),
                    State#state{waiting = Waiting}
            end
    end.

handle_checkin(Pid, State) ->
    handle_checkin(Pid, false, State).

handle_checkin(Pid, Dismissed, State) ->
    #state{supervisor = Sup,
           workers = Workers,
           waiting = Waiting,
           overflow = Overflow,
           working_status = WorkingStatus} = State,
    WorkingStatus1 = poolboy_working_status:del_worker(Pid, WorkingStatus),
    State1 = State#state{working_status = WorkingStatus1},
    case poolboy_working_status:should_dismiss(Pid, WorkingStatus) of
        true ->
            ok = dismiss_worker(Sup, Pid, Dismissed),
            State1;
        false ->
            case queue:out(Waiting) of
                {{value, {From, CRef, MRef}}, Left} ->
                    Pid1 = new_worker(Sup, Pid, Dismissed),
                    Workers1 = queue:in(Pid1, Workers),
                    State2 = State1#state{waiting = Left, workers = Workers1},
                    handle_checkout(From, CRef, MRef, #{}, State2);
                {empty, Empty} when Overflow > 0 ->
                    ok = dismiss_worker(Sup, Pid),
                    State1#state{waiting = Empty, overflow = Overflow - 1};
                {empty, Empty} ->
                    Pid1 = new_worker(Sup, Pid, Dismissed),
                    Workers1 = queue:in(Pid1, Workers),
                    State1#state{waiting = Empty, workers = Workers1}
            end
    end.

add_worker(Pid, {FromPid, _} = From, CRef, MRef, #state{monitors = Monitors, working_status = WorkingStatus} = State) ->
    WorkingStatus1 = poolboy_working_status:add_worker(FromPid, Pid, WorkingStatus),
    true = ets:insert(Monitors, {Pid, CRef, MRef}),
    gen_server:reply(From, Pid),
    State#state{working_status = WorkingStatus1}.

state_name(State = #state{overflow = Overflow}) when Overflow < 1 ->
    #state{max_overflow = MaxOverflow, workers = Workers} = State,
    case queue:len(Workers) == 0 of
        true when MaxOverflow < 1 -> full;
        true -> overflow;
        false -> ready
    end;
state_name(#state{overflow = MaxOverflow, max_overflow = MaxOverflow}) ->
    full;
state_name(_State) ->
    overflow.
