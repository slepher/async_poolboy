%%%-------------------------------------------------------------------
%%% @author Chen Slepher <slepheric@gmail.com>
%%% @copyright (C) 2019, Chen Slepher
%%% @doc
%%%
%%% @end
%%% Created : 11 Apr 2019 by Chen Slepher <slepheric@gmail.com>
%%%-------------------------------------------------------------------
-module(poolboy_working_status).


-record(working_status, {working = ordsets:new() :: ordsets:ordsets(pid()),
                         parents = maps:new() :: maps:map(pid(), pid()),
                         children = maps:new() :: maps:map(pid(), ordsets:ordsets(pid()))}).

%% API
-export([new/0, should_spawn/2, should_dismiss/2, add_worker/3, del_worker/2]).

-export([working/1, parents/1, children/1]).

%%%===================================================================
%%% API
%%%===================================================================
new() ->
    #working_status{}.

working(WorkingStatus) ->
    WorkingStatus#working_status.working.

parents(WorkingStatus) ->
    WorkingStatus#working_status.parents.

children(WorkingStatus) ->
    WorkingStatus#working_status.children.

should_spawn(CallerPid, #working_status{working = Working, children = Children}) ->
    %% if callerpid is one of working process, and no child is spawned, worker process should spawn new.
    case ordsets:is_element(CallerPid, Working) of
        true ->
            not maps:is_key(CallerPid, Children);
        false ->
            false
    end.

should_dismiss(WorkerPid, #working_status{parents = Parents, children = Children}) ->
    %% if worker still have children, but checked in first, worker process should dismiss.
    case maps:is_key(WorkerPid, Children) of
        true ->
            true;
        false ->
            case maps:find(WorkerPid, Parents) of
                {ok, ParentPid} ->
                    %% if worker is the last child of it's parent, worker process should dismiss.
                    case maps:find(ParentPid, Children) of
                        {ok, [WorkerPid]} ->
                            true;
                        _ ->
                            false
                    end;
                error ->
                    false
            end
    end.

add_worker(CallerPid, WorkerPid, #working_status{working = Working, parents = Parents, children = Children} = Status) ->
    Working1 = ordsets:add_element(WorkerPid, Working),
    Status1 = Status#working_status{working = Working1},
    case ordsets:is_element(CallerPid, Working) of
        true ->
            CallerChildren = maps:get(CallerPid, Children, ordsets:new()),
            CallerChildren1 = ordsets:add_element(WorkerPid, CallerChildren),
            Children1 = maps:put(CallerPid, CallerChildren1, Children),
            Parents1 = maps:put(WorkerPid, CallerPid, Parents),
            Status1#working_status{working = Working1, parents = Parents1, children = Children1};
        false ->
            Status1
    end.

del_worker(WorkerPid, #working_status{working = Working, parents = Parents} = Status) ->
    Working1 = ordsets:del_element(WorkerPid, Working),
    Status1 = Status#working_status{working = Working1},
    case maps:find(WorkerPid, Parents) of
        {ok, CallerPid} ->
            Status2 = update_releation(WorkerPid, CallerPid, Status1),
            #working_status{parents = Parents1, children = Children} = Status2,
            Parents2 = maps:remove(WorkerPid, Parents1),
            Children1 = 
                case maps:find(CallerPid, Children) of
                    {ok, CallerChildren} ->
                        case ordsets:del_element(WorkerPid, CallerChildren) of
                            [] ->
                                maps:remove(CallerPid, Children);
                            CallerChildren1 ->
                                maps:put(CallerPid, CallerChildren1, Children)
                        end;
                    error ->
                        Children
                end,
            Status2#working_status{parents = Parents2, children = Children1};
        error ->
            clear_releation(WorkerPid, Status1)
    end.

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------
            

%%%===================================================================
%%% Internal functions
%%%===================================================================
clear_releation(WorkerPid, #working_status{parents = Parents, children = Children} = State) ->
    case maps:find(WorkerPid, Children) of
        {ok, WorkerChildren} ->
            Children1 = maps:remove(WorkerPid, Children),
            Parents1 = 
                lists:foldl(
                  fun(WorkerChild, Acc) ->
                          maps:remove(WorkerChild, Acc)
                  end, Parents, WorkerChildren),
            State#working_status{parents = Parents1, children = Children1};
        error ->
            State
    end.

update_releation(WorkerPid, WorkerParentPid, #working_status{parents = Parents, children = Children} = State) ->
    case maps:find(WorkerPid, Children) of
        {ok, WorkerChildren} ->
            Children1 = maps:remove(WorkerPid, Children),
            WorkerParentChildren = maps:get(WorkerParentPid, Children1, []),
            Children2 = maps:put(WorkerParentPid, ordsets:union(WorkerChildren, WorkerParentChildren), Children1),
            Parents1 = 
                lists:foldl(
                  fun(WorkerChild, Acc) ->
                          maps:put(WorkerChild, WorkerParentPid, Acc)
                  end, Parents, WorkerChildren),
            State#working_status{parents = Parents1, children = Children2};
        error ->
            State
    end.
