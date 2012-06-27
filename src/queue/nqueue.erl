-module(nqueue).
-behaviour(gen_server).

-compile(export_all).

-include("nq.hrl").
-include("nq_queue.hrl").

-export([
        start_link/1,

        init/1,
        handle_call/3,
        handle_info/2,
        handle_cast/2,
        code_change/3,
        terminate/2,

        stop/1,

        enq/2,
        deq/1,
        deq/3,
        sync/1,

        size/1,

        peek/1,

        purge/1,

        get_meta/1,

        subscribe/1,
        unsubscribe/1,
        subscriber_count/1

        ]).

-define(NAME(X), {global, {?MODULE, X}}).

start_link(QName) when is_list(QName) ->
    start_link(QName, [{storage_mod, nq_file}, {storage_mod_params, "./nqdata/"}]).

start_link(QName, Options)->
    gen_server:start_link(?NAME(QName), ?MODULE, [QName, Options], []).

enq(QName, Msg) ->
    gen_server:call(?NAME(QName), {enq, Msg}, infinity).

deq(QName) ->
    deq(QName, undefined, undefined).

deq(QName, Fun, Args) when is_function(Fun, 3) or ((Fun == undefined) and (Args == undefined)) ->
    gen_server:call(?NAME(QName), {deq, Fun, Args}, infinity).

peek(QName) ->

    case deq(QName, fun (_, M, _) -> {peek, M} end, undefined) of
    {error, {peek, Msg}} ->
        {ok, Msg};

    Error ->
        Error

    end.

purge(QName) ->
    gen_server:call(?NAME(QName), purge, infinity).

get_meta(QName) ->
    gen_server:call(?NAME(QName), get_meta, infinity).


sync(QName) ->
    gen_server:call(?NAME(QName), sync, infinity).

size(QName) ->
    gen_server:call(?NAME(QName), size, infinity).

stop(QName) ->
    gen_server:call(?NAME(QName), stop, infinity).

subscribe(QName) ->
    gen_server:call(?NAME(QName), {subscribe, self()}, infinity).
    
unsubscribe(QName) ->
    gen_server:call(?NAME(QName), {unsubscribe, self()}, infinity).

subscriber_count(QName) ->
    gen_server:call(?NAME(QName), subscriber_count, infinity).


init([QName, Options]) ->

    process_flag(trap_exit, true),

    {ok, DefaultSyncIntervalMs} = application:get_env(nq, sync_interval_ms),
    {ok, DefaultMaxFragSize}    = application:get_env(nq, max_frag_size),

    StorageMod          = proplists:get_value(storage_mod, Options),
    StorageModParams    = proplists:get_value(storage_mod_params, Options),
    SyncIntervalMs      = proplists:get_value(sync_interval_ms, Options, DefaultSyncIntervalMs),
    MaxFragSize         = proplists:get_value(max_frag_size, Options, DefaultMaxFragSize),

    {ok, UpdatedStorageModParams} = StorageMod:init(QName, StorageModParams),

    {ok, {QSize, TRFragIdx, TRFragRecno, TWFragIdx}} = StorageMod:read_meta(QName, UpdatedStorageModParams),
    
    {ok, RData} = read_frag(QName, TRFragIdx, 0, StorageMod, UpdatedStorageModParams),

    {RFragIdx, RFragRecno, WFragIdx, WData} = 
    if (TRFragIdx =/= TWFragIdx) ->

        WFragSize = StorageMod:frag_size(QName, TWFragIdx, UpdatedStorageModParams),

        if (WFragSize >= MaxFragSize) ->
            {TRFragIdx, TRFragRecno, TWFragIdx + 1, []};
        true ->

            {ok, WD} = read_frag(QName, TWFragIdx, 0, StorageMod, UpdatedStorageModParams),

            {TRFragIdx, TRFragRecno, TWFragIdx, WD}
        end;

    true ->
        {TRFragIdx, TRFragRecno, TWFragIdx, []}

    end,

    RC = lists:sum([erlang:size(RE) || RE <- RData]),
    WC = lists:sum([erlang:size(WE) || WE <- WData]),

    AutoSync = proplists:get_value(auto_sync, Options, true),

    TRef = 
    if (AutoSync) ->
        {ok, Ref} = timer:apply_interval(SyncIntervalMs, ?MODULE, sync, [QName]),
        Ref;

    true ->
        undefined
    end,

    {ok, #state{qname = QName, size = QSize, tref = TRef, rfrag_idx = RFragIdx, rfrag_recno = RFragRecno, rfrag_cache = RData, rfrag_cache_size = RC, wfrag_cache_size = WC,
                wfrag_cache = WData, wfrag_idx = WFragIdx, storage_mod = StorageMod, storage_mod_params = UpdatedStorageModParams, max_frag_size = MaxFragSize, auto_sync = AutoSync}}.


handle_call({enq, Msg}, _, #state{  
                                qname = QName, size = Qsize,
                                storage_mod = StorageMod, storage_mod_params = StorageModParams,
                                rfrag_cache = RData, rfrag_idx = RFragIdx, rfrag_cache_size = RFragCacheSize, rfrag_recno = RFragRecNo,
                                wfrag_cache = WData, wfrag_idx = WFragIdx, wfrag_cache_size = WFragCacheSize, max_frag_size = MaxFragCacheSize} = State) ->

    BinMsg = term_to_binary(Msg),

    Size = byte_size(BinMsg),

    {WriteBuffer, FragCache, FragCacheSize, FragIdx} =

    if (RFragIdx == WFragIdx) ->
        {r, RData, RFragCacheSize, RFragIdx};
    true ->
        {w, WData, WFragCacheSize, WFragIdx}
    end,

    NewFragCacheSize = 8 + Size + FragCacheSize,

    NewFragCache = lists:append(FragCache, [<<Size:64/big-unsigned-integer,BinMsg/binary>>]),

    {Reply, ReplyState} = 
    if (NewFragCacheSize >= MaxFragCacheSize) ->

        case WriteBuffer of
        w ->

            case catch(StorageMod:write_frag(QName, FragIdx, NewFragCache, Qsize + 1, RFragIdx, RFragRecNo, WFragIdx + 1, StorageModParams)) of
            ok ->
                {ok, State#state{wfrag_cache = [], size = Qsize + 1, wfrag_dirty = false, wfrag_idx = WFragIdx + 1, wfrag_cache_size = 0, meta_dirty = false}};

            Error ->
                {Error, State}
            end;

        r ->

            case catch(StorageMod:write_frag(QName, FragIdx, NewFragCache, Qsize + 1, RFragIdx, RFragRecNo, WFragIdx + 1, StorageModParams)) of
            ok ->
                {ok, State#state{rfrag_cache = NewFragCache, rfrag_cache_size = NewFragCacheSize, 
                                        size = Qsize + 1, rfrag_dirty = false, 
                                        wfrag_idx = WFragIdx + 1, wfrag_cache = [], wfrag_cache_size = 0, meta_dirty = false, wfrag_dirty = false}};

            Error ->
                {Error, State}
            end

        end;

    true ->

        case WriteBuffer of
        w ->
            {ok, State#state{wfrag_cache = NewFragCache, size = Qsize + 1, wfrag_cache_size = NewFragCacheSize, wfrag_dirty = true}};

        r ->
            {ok, State#state{rfrag_cache = NewFragCache, size = Qsize + 1, rfrag_cache_size = NewFragCacheSize, rfrag_dirty = true, meta_dirty = true}}

        end

    end,

    HasSubscibers = dict:size(ReplyState#state.subs_dict) > 0,

    if HasSubscibers -> 
        {reply, Reply, ReplyState, 0};
    true ->
        {reply, Reply, ReplyState}
    end;

handle_call({deq, Fun, Args}, _, #state{ qname = QName, size = Qsize,
                            storage_mod = StorageMod, storage_mod_params = StorageModParams,
                            wfrag_cache_size = WFragCacheSize, rfrag_cache_size = RFragCacheSize,
                            wfrag_idx = WIdx, rfrag_idx = RIdx, rfrag_cache = RData, wfrag_cache = WData, rfrag_recno = RFragRecno, wfrag_dirty = WDirty} = State) ->

    if (Qsize == 0) ->
        {reply, {error, empty}, State};

    true ->

        case lists:nthtail(RFragRecno, RData) of
        [<<BinMsgSize:64/big-unsigned-integer,BinMsg/binary>> | Tail] ->

            if (WIdx =/= RIdx) ->
                do_deq(QName, BinMsg, Fun, Args, State#state{size = Qsize - 1, meta_dirty = true, rfrag_recno = RFragRecno + 1}, State);
            true ->
                do_deq(QName, BinMsg, Fun, Args, State#state{rfrag_cache = Tail, rfrag_cache_size = RFragCacheSize - (8 + BinMsgSize),  size = Qsize - 1, meta_dirty = true, rfrag_recno = 0, rfrag_dirty = true}, State)
            end;

        [] ->
            %If the read buffer has been depleted, we need to either load from 
            %disk or seed from write buffer if the write buffer index is the next
            %after the read buffer index.


            %Seed from the write buffer iff the write buffer index = read buffer index + 1
            if (WIdx == (RIdx + 1)) ->
 
                WriteRes = 
                if (WDirty) ->
                    catch(StorageMod:write_frag(QName, WIdx, WData, Qsize, WIdx, 0, WIdx, StorageModParams));
                true ->
                    catch(StorageMod:write_meta(QName, Qsize, WIdx, 0, WIdx, StorageModParams))
                end,

                case WriteRes of
                ok ->

                    catch(StorageMod:trash_frag(QName, RIdx, StorageModParams)),

                    [<<_:64/big-unsigned-integer,BinMsg/binary>> | _] = WData,

                    do_deq( QName, BinMsg, Fun, Args, 

                            State#state{ rfrag_idx = WIdx, rfrag_cache_size = WFragCacheSize, size = Qsize - 1, rfrag_recno = 1, rfrag_cache = WData, 
                                         wfrag_cache = [], wfrag_cache_size = 0, meta_dirty = true,  wfrag_dirty = false, rfrag_dirty = false},


                            State#state{ rfrag_idx = WIdx, rfrag_cache_size = WFragCacheSize, size = Qsize,     rfrag_recno = 0, rfrag_cache = WData, 
                                         wfrag_cache = [], wfrag_cache_size = 0, meta_dirty = false, wfrag_dirty = false, rfrag_dirty = false} );


                Error ->
                    {reply, Error, State}

                end;

            true ->

                case read_next_frag_for_deq(QName, Qsize, RIdx, WIdx, StorageMod, StorageModParams) of
                {ok, [<<_:64/big-unsigned-integer,BinMsg/binary>> | _] = Frag} ->

                    do_deq(QName, BinMsg, Fun, Args, 
                        State#state{ size = Qsize - 1, rfrag_idx = RIdx + 1, rfrag_recno = 1, rfrag_cache = Frag, meta_dirty = true, rfrag_dirty = false},
                        State#state{ size = Qsize, rfrag_idx = RIdx + 1, rfrag_recno = 0, rfrag_cache = Frag, meta_dirty = false, rfrag_dirty = false});

                Error ->
                    {reply, Error, State}

                end

            end

        end

    end;

handle_call(size, _, #state{size = Qsize} = State) ->
    {reply, Qsize, State};

handle_call(stop, _, State) ->

    case do_sync(State) of
    {ok, NewState} ->
        {stop, normal, ok, NewState};

    {Error, _} ->
        {reply, Error, State}

    end;

handle_call(sync, _, State) ->

    {Res, NewState} = do_sync(State),

    {reply, Res, NewState};
        
handle_call(purge, _, #state{qname = QName, storage_mod = StorageMod, storage_mod_params = StorageModParams, rfrag_idx = RIdx, wfrag_idx = WIdx} = State) ->

    {Res, NewState} =
    case StorageMod:write_meta(QName, 0, 0, 0, 0, StorageModParams) of
    ok ->

        [StorageMod:trash_frag(QName, I, StorageModParams) || I <- lists:seq(RIdx, WIdx)],

        {ok, State#state    {
                            size = 0, meta_dirty = false, last_sync = now(),
                            rfrag_idx = 0, rfrag_recno = 0, rfrag_cache_size = 0, rfrag_cache = [], rfrag_dirty = false, 
                            wfrag_idx = 0,                  wfrag_cache_size = 0, wfrag_cache = [], wfrag_dirty = false
                            }};

    Error ->
        {Error, State}

    end,

    {reply, Res, NewState};

handle_call(subscriber_count, _, #state{subs_dict = SDict} = State) ->
    {reply, dict:size(SDict), State};

handle_call({subscribe, Pid}, _, #state{subs_dict = SDict} = State) ->

    NewSDict =
    case dict:find(Pid, SDict) of
    {ok, _} ->
        SDict;

    _ ->
        MonitorRef = erlang:monitor(process, Pid),

        dict:store(Pid, {MonitorRef, now()}, SDict)

    end,

    {reply, ok, State#state{subs_dict = NewSDict}, 0};

handle_call({unsubscribe, Pid}, _, #state{subs_dict = SDict} = State) ->

    NewSDict =
    case dict:find(Pid, SDict) of
    {ok, {MonRef, _}} ->

        erlang:demonitor(MonRef),

        dict:erase(Pid, SDict);

    _ ->
        SDict
    end,

    {reply, ok, State#state{subs_dict = NewSDict}};

handle_call(get_meta, _, State) ->

    #state  {
            size = S, 
            meta_dirty = MD,
            rfrag_idx = RI, rfrag_recno = RR, rfrag_cache_size = RS, rfrag_cache = RC, rfrag_dirty = RD,
            wfrag_idx = WI,                  wfrag_cache_size = WS,  wfrag_cache = WC, wfrag_dirty = WD,

            last_sync = L

            } = State,

    {reply, {S, MD, RI, RR, RS, RC, RD, WI, WS, WC, WD, L}, State};

handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', MonRef, process, Pid, _Info}, #state{subs_dict = SDict} = State) ->

    erlang:demonitor(MonRef),

    {noreply, State#state{subs_dict = dict:erase(Pid, SDict)}};

handle_info(timeout, #state{qname = QName, size = Qsize, subs_dict = SDict} = State) ->
    
    if (Qsize > 0) ->

        [Pid ! {nqueue, QName, ready} || {Pid, _} <- dict:to_list(SDict)],

        {noreply, State#state{last_broadcast = now()}};

    true ->
        {noreply, State}

    end;


handle_info(_, State) ->
    {noreply, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_, State) -> do_sync(State).

do_sync(#state{ qname = QName, size = Qsize,
                storage_mod = StorageMod, storage_mod_params = StorageModParams,
                meta_dirty = MDirty, wfrag_dirty = WDirty, rfrag_dirty = RDirty, rfrag_recno = RFragRecNo, rfrag_idx = RFragIdx, wfrag_idx = WFragIdx, wfrag_cache = WData, rfrag_cache = RData} = State) ->

    %?info({QName, sync}),

    if (MDirty and not (WDirty or RDirty)) ->

        %Example: Only the head pointer has moved ...

        case catch(StorageMod:write_meta(QName, Qsize, RFragIdx, RFragRecNo, WFragIdx, StorageModParams)) of
        ok ->
            {ok, State#state{meta_dirty = false, last_sync = now()}};

        Error ->
            {Error, State}

        end;

    RDirty ->

        case catch(StorageMod:write_frag(QName, RFragIdx, RData, Qsize, RFragIdx, RFragRecNo, WFragIdx, StorageModParams)) of
        ok ->
            {ok, State#state{rfrag_dirty = false, meta_dirty = false, last_sync = now()}};

        Error ->
            {Error, State}
        end;


    WDirty ->

        case catch(StorageMod:write_frag(QName, WFragIdx, WData, Qsize, RFragIdx, RFragRecNo, WFragIdx, StorageModParams)) of
        ok ->
            {ok, State#state{wfrag_dirty = false, meta_dirty = false, last_sync = now()}};
        Error ->
            {Error, State}
        end;


    true ->
        {ok, State}

    end.

parse_frag(<<MsgSize:64/big-unsigned-integer,Rest/binary>>, MsgList, MsgIdx, RecNo) -> 

    if (MsgIdx < RecNo) ->

        parse_frag(erlang:binary_part(Rest, MsgSize, byte_size(Rest) - MsgSize), MsgList, MsgIdx + 1, RecNo);

    true ->

        MsgBody= erlang:binary_part(Rest, 0, MsgSize),

        parse_frag(erlang:binary_part(Rest, MsgSize, byte_size(Rest) - MsgSize), lists:append(MsgList, [<<MsgSize:64/big-unsigned-integer, MsgBody/binary>>]), MsgIdx + 1, RecNo)


    end;

parse_frag(<<>>, MsgList, _, _) -> {ok, MsgList}.


read_frag(QName, FragIdx, RecNo, StorageMod, StorageModParams) ->

    case catch(StorageMod:read_frag(QName, FragIdx, StorageModParams)) of
    {ok, Data} when is_binary(Data) ->
        catch(parse_frag(Data, [], 0, RecNo));

    Error ->
        Error

    end.

do_deq(_, BinMessage, undefined, _, NewStateSuccess, _) -> {reply, {ok, binary_to_term(BinMessage)}, NewStateSuccess};
do_deq(QName, BinMessage, Fun, Args, NewStateSuccess, NewStateFail) ->

    Message = binary_to_term(BinMessage),

    case catch(Fun(QName,Message,Args)) of
    ok ->
        {reply, {ok, Message}, NewStateSuccess};
    Error ->
        {reply, {error, Error}, NewStateFail}
    end.
 
read_next_frag_for_deq(QName, Qsize, RIdx, WIdx, StorageMod, StorageModParams) ->

    case read_frag(QName, RIdx + 1, 0, StorageMod, StorageModParams) of
    {ok, []} ->
        {error, empty_frag};

    {ok, Frag} ->

        case catch(StorageMod:write_meta(QName, Qsize, RIdx + 1, 0, WIdx, StorageModParams)) of
        ok ->

            catch(StorageMod:trash_frag(QName, RIdx, StorageModParams)),

            {ok, Frag};

       Error ->
           {error, Error}

       end;

   Error ->
       {error, Error}

   end.


%EOF
