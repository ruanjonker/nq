-module(nqueue_consumer).

-include("nq.hrl").

-behavior(gen_server).

-export([
        init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3
        ]).

-export([
        start_link/7,
        get_state/1, set_state/2,
        set_fun/3, set_err_fun/3
        ]).

-record(state, {queue, proc_fun, proc_args, err_fun, err_args, proc_state = paused}).

-define(NAME(X), {global, {?MODULE, X}}).

start_link(ConsId, Queue, Fun, Args, ErrFun, ErrArgs, ProcState) when is_function(Fun, 3) and is_function(ErrFun, 4) and ((ProcState == paused) or (ProcState == unpaused)) ->
    gen_server:start_link(?NAME(ConsId), ?MODULE, [Queue, Fun, Args, ErrFun, ErrArgs, ProcState], []).

set_fun(ConsId, Fun, Args) when is_function(Fun, 3) ->
    gen_server:call(?NAME(ConsId), {set_fun, Fun, Args}, infinity).
    
set_err_fun(ConsId, ErrFun, ErrArgs) when is_function(ErrFun, 4) ->
    gen_server:call(?NAME(ConsId), {set_err_fun, ErrFun, ErrArgs}, infinity).
    

get_state(ConsId) ->
    gen_server:call(?NAME(ConsId), get_state, infinity).
set_state(ConsId, S) when ((S == paused) or (S == unpaused)) ->
    gen_server:call(?NAME(ConsId), {set_state, S}, infinity).



init([Queue, Fun, Args, ErrFun, ErrArgs, ProcState]) ->

    if (ProcState == unpaused) ->
        ok = nqueue:subscribe(Queue);

    true ->
        ok
    end,

    {ok, #state{queue = Queue, proc_fun = Fun, proc_args = Args, err_fun = ErrFun, err_args = ErrArgs, proc_state = ProcState}}.


handle_call({set_state, S}, _, #state{queue = Queue} = State) ->
    case S of
    unpaused ->
        ok = nqueue:subscribe(Queue);
    _ ->
        ok = nqueue:unsubscribe(Queue)
    end,
    {reply, ok, State#state{proc_state = S}, 0};
handle_call(get_state, _, #state{proc_state = P} = State) ->
    {reply, P, State, 0};
handle_call({set_fun, Fun, Args}, _, State) ->
    {reply, ok,  State#state{proc_fun = Fun, proc_args = Args}, 0};
handle_call({set_err_fun, Fun, Args}, _, State) ->
    {reply, ok, State#state{err_fun = Fun, err_args = Args}, 0}.

handle_cast(_, State) ->
    {noreply, State, 0}.

handle_info(timeout, #state{queue = Queue, proc_fun = Fun, proc_args = Args, err_fun = ErrFun, err_args = ErrArgs, proc_state = unpaused} = State) ->

    T0 = now(),
    Self = self(),

    ErrRetryIntervalMs = 5000,

    Acquisition = 

    %See if there is something in the cache already
    case ?dbget("consumer_cache", Self) of
    {ok, {_, CachedMsg, Ts, Count}} ->
        {ok, CachedMsg, Ts, Count};
        
    {error, not_found} ->

        CacheFun = fun(Q, M, _) ->
            ok = ?dbset("consumer_cache", Self, {Q, M, T0, 0}) 
        end,

        case nqueue:deq(Queue, CacheFun, undefined) of
        {ok, CachedMsg} ->
            {ok, CachedMsg, T0, 0};

        Error ->
            Error

        end

    end,

    case Acquisition of
    {ok, Message, ProcTs, ProcCount} ->

        {MustSleep, SleepTimeMs} = 
        if (ProcCount > 0) ->
            DiffMs = trunc(timer:now_diff(T0, ProcTs)/1000),

            if (DiffMs > ErrRetryIntervalMs) ->
                {false, 0};
            true ->
                {true, ErrRetryIntervalMs - DiffMs}
            end;

        true ->
            {false, 0}
        end,

        if (not MustSleep) ->

            case catch(Fun(Queue, Message, Args)) of
            ok ->
    
                ok = ?dbdel("consumer_cache", Self),
    
                {noreply, State, 0};
    
            DeqError ->
            
                case catch (ErrFun(Queue, Message, DeqError, ErrArgs)) of
                ok ->
                    ok = ?dbdel("consumer_cache", Self);
    
                ErrFunError ->
    
                    ok = ?dbset("consumer_cache", Self, {Queue, Message, ProcTs, ProcCount + 1}),                
    
                    error_logger:error_msg("Call to ~p(~p,~p,~p,~p) to handle message handling error (~p) failed with ~p~n", [ErrFun, Queue, Message, DeqError, ErrArgs, DeqError, ErrFunError])
    
                end,
    
                {noreply, State, ErrRetryIntervalMs}
    
            end;

        true ->
            {noreply, State, SleepTimeMs}
        end;

    {error, empty} ->

        ok = nqueue:subscribe(Queue),

        {noreply, State}
    end;

handle_info(timeout, #state{proc_state = paused} = State) ->
    {noreply, State};
handle_info({nqueue, QName, ready}, #state{proc_state = paused} = State) ->
    ok = nqueue:unsubscribe(QName),
    {noreply, State};

handle_info(_, State) ->
    {noreply, State, 0}.
terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%EOF
