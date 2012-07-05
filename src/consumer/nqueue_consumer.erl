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
        get_state/1, set_state/2, pause/1, unpause/1,
        set_fun/3, set_err_fun/3,
        process_n/2
        ]).

-type queue_name() :: string().
-type consumer() :: string().
-type proc_args() :: any().
-type err_args() :: any().
-type proc_error() :: any().
-type message() :: any().
-type consumer_state() :: paused | unpaused.
-type proc_fun() :: fun((queue_name(),message(), proc_args()) -> ok).
-type err_fun() :: fun((queue_name(),message(), proc_error(), err_args()) -> ok).

-record(state, {queue, proc_fun, proc_args, err_fun, err_args, proc_state = paused, messages_to_go = 0, is_subscribed = false}).

-define(NAME(X), {global, {?MODULE, X}}).

%% @doc Start and link a consumer process<br/><br/>
%% Use this to hook the consumer into a supervisory tree.
%%
-spec start_link(consumer(), queue_name(), proc_fun(), proc_args(), err_fun(), err_args(), consumer_state()) -> {ok, pid()}.
start_link(ConsId, Queue, Fun, Args, ErrFun, ErrArgs, ProcState) when is_function(Fun, 3) and is_function(ErrFun, 4) and ((ProcState == paused) or (ProcState == unpaused)) ->
    gen_server:start_link(?NAME(ConsId), ?MODULE, [Queue, Fun, Args, ErrFun, ErrArgs, ProcState], []).

%% @doc Set the procccesing function<br/><br/>
%% Use this call to define how the message should be processed<br/>
%%
-spec set_fun(consumer(), proc_fun(), proc_args()) -> ok.
set_fun(ConsId, Fun, Args) when is_function(Fun, 3) ->
    gen_server:call(?NAME(ConsId), {set_fun, Fun, Args}, infinity).
    
%% @doc Set the error handling function<br/><br/>
%% Use this call to define how a failed message should be processed<br/>
%%
-spec set_err_fun(consumer(), err_fun(), err_args()) -> ok.
set_err_fun(ConsId, ErrFun, ErrArgs) when is_function(ErrFun, 4) ->
    gen_server:call(?NAME(ConsId), {set_err_fun, ErrFun, ErrArgs}, infinity).

%% @doc Get consumer state<br/><br/>
%% Returns the consumer's processing state
%% <b>NOTE:</b>This function is synchronous and will block until<br/>
%% the consumer state has been updated.
%% 
-spec get_state(string()) -> 'paused' | unpaused.
get_state(ConsId) ->
    gen_server:call(?NAME(ConsId), get_state, infinity).

%% @doc Pause processing on associated message queue.<br/><br/>
%% The consumer will actually just unsubscribe from the<br/>
%% associated queue, and makr the internal state as "paused"<br/>
%% <b>NOTE:</b>This function is synchronous and will block until<br/>
%% the consumer state has been updated.
%% 
-spec pause(string()) -> ok.
pause(ConsId) ->
    set_state(ConsId, paused).

%% @doc Resume processing on associated message queue.<br/><br/>
%% The consumer will actually just subscribe to the<br/>
%% associated queue, so that when messages are available, the<br/>
%% queue will notify the consumer so that processing can resume.<br/><br/>
%% <b>NOTE:</b>This function is synchronous and will block until<br/>
%% the consumer state has been updated.
%% 
-spec unpause(string()) -> ok.
unpause(ConsId) ->
    set_state(ConsId, unpaused).

%% @hidden
set_state(ConsId, S) when ((S == paused) or (S == unpaused)) ->
    gen_server:call(?NAME(ConsId), {set_state, S}, infinity).

%% @doc Process upto N messages on queue.
%% The consumer will <b>try</b> to process N messages and then go to a "paused" state when:<br/><br/>
%% <b>1.</b> N messages were processed or<br/>
%% <b>2.</b> A message processing error was encountered or<br/>
%% <b>3.</b> The queue is empty
%% 
-spec process_n(string(), pos_integer()) -> ok.
process_n(ConsId, N) when ((is_integer(N) and N > 0) or (N =:= all)) ->
    gen_server:call(?NAME(ConsId), {process_n, N}, infinity).

%% @hidden
init([Queue, Fun, Args, ErrFun, ErrArgs, ProcState]) ->

    IsSubscribed =
    if (ProcState == unpaused) ->
        ok = nqueue:subscribe(Queue),
        true;

    true ->
        false

    end,

    {ok, #state{queue = Queue, proc_fun = Fun, proc_args = Args, err_fun = ErrFun, err_args = ErrArgs, proc_state = ProcState, is_subscribed = IsSubscribed}}.


%% @hidden
handle_call({process_n, N}, _, #state{queue = Queue} = State) ->

    NumMsgs =
    if (N =:= all) ->
        nqueue:size(Queue);
    true ->
        QSize = nqueue:size(Queue),

        if (QSize < N) ->
            QSize;
        true ->
            N
        end
    
    end,

    if (NumMsgs > 0) ->

        %Unsubscribe from queue, we are actively going to process it
        ok = nqueue:unsubscribe(Queue),

        {reply, ok, State#state{proc_state = unpaused, messages_to_go = NumMsgs + 1, is_subscribed = false}, 0};

    true ->
        %Nothing to do ...
        {reply, ok, State}
    end;

handle_call({set_state, S}, _, #state{queue = Queue} = State) ->
    
    case S of
    unpaused ->
        ok = nqueue:subscribe(Queue);
    _ ->
        ok = nqueue:unsubscribe(Queue)
    end,
    {reply, ok, State#state{proc_state = S, messages_to_go = 0, is_subscribed = (S =:= unpaused)}, 0};
handle_call(get_state, _, #state{proc_state = P} = State) ->
    {reply, P, State, 0};
handle_call({set_fun, Fun, Args}, _, State) ->
    {reply, ok,  State#state{proc_fun = Fun, proc_args = Args}, 0};
handle_call({set_err_fun, Fun, Args}, _, State) ->
    {reply, ok, State#state{err_fun = Fun, err_args = Args}, 0}.

%% @hidden
handle_cast(_, State) ->
    {noreply, State, 0}.

%% @hidden
handle_info(timeout, #state{queue = Queue, proc_fun = Fun, proc_args = Args, err_fun = ErrFun, err_args = ErrArgs, proc_state = unpaused, messages_to_go = MsgsToGo} = State) ->

    T0 = now(),
    Self = self(),

    ErrRetryIntervalMs = 5000,

    Acquisition = aqcuire_message(Self, T0, Queue),

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

        if (MustSleep) ->
            {noreply, State, SleepTimeMs};

        true ->

            case catch(Fun(Queue, Message, Args)) of
            ok ->
    
                ok = ?dbdel("nq_consumer_cache", Self),
    
                if (MsgsToGo > 0) ->

                    NewMsgsToGo = MsgsToGo -1,

                    if (NewMsgsToGo == 0) ->
                        
                        {noreply, State#state{messages_to_go = 0, is_subscribed = false, proc_state = paused}};

                    true ->
                        {noreply, State#state{messages_to_go = NewMsgsToGo}, 0}

                    end;

                true ->
                    {noreply, State, 0}
                end;

            DeqError ->
            
                case catch (ErrFun(Queue, Message, DeqError, ErrArgs)) of
                ok ->
                    ok = ?dbdel("nq_consumer_cache", Self);
    
                ErrFunError ->
    
                    ok = ?dbset("nq_consumer_cache", Self, {Queue, Message, ProcTs, ProcCount + 1}),                
    
                    error_logger:error_msg("Call to ~p(~p,~p,~p,~p) to handle message handling error (~p) failed with ~p~n", [ErrFun, Queue, Message, DeqError, ErrArgs, DeqError, ErrFunError])
    
                end,
    
                {noreply, State, ErrRetryIntervalMs}
    
            end

        end;

    {error, empty} ->

        if (MsgsToGo == 0) ->

            ok = nqueue:subscribe(Queue),

            {noreply, State#state{messages_to_go = 0, is_subscribed = true}};

        true ->

            {noreply, State#state{messages_to_go = 0, is_subscribed = false, proc_state = paused}}

        end

    end;

handle_info(timeout, #state{proc_state = paused} = State) ->
    {noreply, State};
handle_info({nqueue, QName, ready}, #state{proc_state = paused} = State) ->
    ok = nqueue:unsubscribe(QName),
    {noreply, State};

handle_info({nqueue, _QName, ready}, #state{proc_state = unpaused} = State) ->

    {noreply, State, 0};

handle_info(_, State) ->
    {noreply, State, 0}.

%% @hidden
terminate(_, _) ->
    ok.

%% @hidden
code_change(_, State, _) ->
    {ok, State}.

%% @hidden
aqcuire_message(Self, T0, Queue) ->

    %See if there is something in the cache already
    case ?dbget("nq_consumer_cache", Self) of
    {ok, {_, CachedMsg, Ts, Count}} ->
        {ok, CachedMsg, Ts, Count};

    {error, not_found} ->

        CacheFun = fun(Q, M, _) ->
            ok = ?dbset("nq_consumer_cache", Self, {Q, M, T0, 0})
        end,

        case nqueue:deq(Queue, CacheFun, undefined) of
        {ok, CachedMsg} ->
            {ok, CachedMsg, T0, 0};

        Error ->
            Error

        end

    end.



%EOF
