%%%-------------------------------------------------------------------
%%% @author Chen Slepher <slepheric@gmail.com>
%%% @copyright (C) 2019, Chen Slepher
%%% @doc
%%%
%%% @end
%%% Created : 11 Apr 2019 by Chen Slepher <slepheric@gmail.com>
%%%-------------------------------------------------------------------
-module(poolboy_working_status_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("erlando/include/do.hrl").
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
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_suite(Config0) -> term() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
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
    [spawn_with_child, spawn_with_child_2, spawn_with_child_3].

%%--------------------------------------------------------------------
%% @spec TestCase() -> Info
%% Info = [tuple()]
%% @end
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @spec TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%% @end
%%--------------------------------------------------------------------
spawn_with_child(_Config) -> 
    StateM = 
        do([ state_m ||
               spawns(),
               false <- del_worker(4),
               true  <- del_worker(5),
               false <- del_worker(1),
               return(ok)
           ]),
    Status = state_m:exec(StateM, poolboy_working_status:new()),
    [2, 3] = poolboy_working_status:working(Status),
    0 = maps:size(poolboy_working_status:parents(Status)),
    0 = maps:size(poolboy_working_status:children(Status)),
    ok.

spawn_with_child_2(_Config) ->
    StateM = 
        do([ state_m ||
               spawns(),
               true  <- add_worker(4, 6),
               false <- add_worker(4, 7),
               true  <- del_worker(4),
               false <- del_worker(6),
               false <- del_worker(7),
               true  <- del_worker(5),
               false <- del_worker(1),
               return(ok)
           ]),
    Status = state_m:exec(StateM, poolboy_working_status:new()),
    [2, 3] = poolboy_working_status:working(Status),
    0 = maps:size(poolboy_working_status:parents(Status)),
    0 = maps:size(poolboy_working_status:children(Status)),
    ok.

spawn_with_child_3(_Config) ->
    StateM = 
        do([ state_m ||
               spawns(),
               true  <- add_worker(4, 6),
               false <- add_worker(4, 7),
               true  <- del_worker(1),
               true  <- del_worker(4),
               false <- del_worker(6),
               false <- del_worker(7),
               false <- del_worker(5),
               return(ok)
       ]),
    Status = state_m:exec(StateM, poolboy_working_status:new()),
    [2, 3] = poolboy_working_status:working(Status),
    0 = maps:size(poolboy_working_status:parents(Status)),
    0 = maps:size(poolboy_working_status:children(Status)),
    ok.

spawns() ->
    do([monad ||
           false <- add_worker(0, 1),
           false <- add_worker(0, 2),
           false <- add_worker(0, 3),
           true  <- add_worker(1, 4),
           false <- add_worker(1, 5),
           return(ok)
       ]).

add_worker(Caller, Worker) ->
    monad_state:state(
      fun(State) ->
              ShouldSpawn = poolboy_working_status:should_spawn(Caller, State),
              State1 = poolboy_working_status:add_worker(Caller, Worker, State),
              {ShouldSpawn, State1}
      end).

del_worker(Worker) ->
    monad_state:state(
      fun(State) ->
              ShouldDismiss = poolboy_working_status:should_dismiss(Worker, State),
              State1 = poolboy_working_status:del_worker(Worker, State),
              {ShouldDismiss, State1}
      end).
