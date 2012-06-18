-module(nqueue).
-behaviour(gen_server).

-compile(export_all).

-include("nq.hrl").

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
        sync/1,

        subscribe/1,
        unsubscribe/1

        ]).

-record(state, {tref, qname, meta_dirty = false, rfrag_idx = 0, rfrag_recno = 0, rfrag_cache_size = 0, rfrag_cache = [], rfrag_dirty = false, wfrag_cache = [], wfrag_dirty = false, wfrag_cache_size = 0, wfrag_idx = 0, last_sync = now(), subs_dict = dict:new(), storage_mod = nq_file, storage_mod_params = "./nqdata/", max_frag_size = 64000}).

-define(NAME(X), {global, {?MODULE, X}}).

start_link(QName) when is_list(QName) ->
    start_link(QName, [{storage_mod, nq_file}, {storage_mod_params, "./nqdata/"}]).

start_link(QName, Options)->
    gen_server:start_link(?NAME(QName), ?MODULE, [QName, Options], []).

enq(QName, Msg) ->
    gen_server:call(?NAME(QName), {enq, Msg}, infinity).

deq(QName) ->
    gen_server:call(?NAME(QName), deq, infinity).

sync(QName) ->
    gen_server:call(?NAME(QName), sync, infinity).

stop(QName) ->
    gen_server:call(?NAME(QName), stop, infinity).

subscribe(QName) ->
    gen_server:call(?NAME(QName), {subscribe, self()}, infinity).
    

unsubscribe(QName) ->
    gen_server:call(?NAME(QName), {unsubscribe, self()}, infinity).

init([QName, Options]) ->

    {ok, DefaultSyncIntervalMs} = application:get_env(nq, sync_interval_ms),
    {ok, DefaultMaxFragSize}    = application:get_env(nq, max_frag_size),

    StorageMod          = proplists:get_value(storage_mod, Options),
    StorageModParams    = proplists:get_value(storage_mod_params, Options),
    SyncIntervalMs      = proplists:get_value(sync_interval_ms, Options, DefaultSyncIntervalMs),
    MaxFragSize         = proplists:get_value(max_frag_size, Options, DefaultMaxFragSize),

    {ok, UpdatedStorageModParams} = StorageMod:init(QName, StorageModParams),

    {ok, {TRFragIdx, TRFragRecno, TWFragIdx}} = StorageMod:read_meta(QName, UpdatedStorageModParams),
    
    {ok, RData} = read_frag(QName, TRFragIdx, TRFragRecno, StorageMod, UpdatedStorageModParams),

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

    {ok, TRef} = timer:apply_interval(SyncIntervalMs, ?MODULE, sync, [QName]),

    {ok, #state{qname = QName, tref = TRef, rfrag_idx = RFragIdx, rfrag_recno = RFragRecno, rfrag_cache = RData, 
                wfrag_cache = WData, wfrag_idx = WFragIdx, storage_mod = StorageMod, storage_mod_params = UpdatedStorageModParams, max_frag_size = MaxFragSize}}.


handle_call({enq, Msg}, _, #state{  
                                qname = QName, 
                                storage_mod = StorageMod, storage_mod_params = StorageModParams,
                                rfrag_cache = RData, rfrag_idx = RFragIdx, rfrag_cache_size = RFragCacheSize, rfrag_recno = RFragRecNo,
                                wfrag_cache = WData, wfrag_idx = WFragIdx, wfrag_cache_size = WFragCacheSize} = State) ->

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

    {ok, MaxFragCacheSize} = application:get_env(nq, max_frag_size),
    {ok, SubsNotificationSleepMs} = application:get_env(nq, subs_notification_sleep_ms),

    if (NewFragCacheSize >= MaxFragCacheSize) ->

        case WriteBuffer of
        w ->

            case StorageMod:write_frag(QName, FragIdx, NewFragCache, RFragIdx, RFragRecNo, WFragIdx + 1, StorageModParams) of
            ok ->
                {reply, ok, State#state{wfrag_cache = [] , wfrag_dirty = false, wfrag_idx = WFragIdx + 1, wfrag_cache_size = 0, meta_dirty = false}, SubsNotificationSleepMs};

            Error ->
                {reply, Error, State, 5000}
            end;

        r ->

            case StorageMod:write_frag(QName, FragIdx, NewFragCache, RFragIdx, RFragRecNo, WFragIdx + 1, StorageModParams) of
            ok ->
                {reply, ok, State#state{rfrag_cache = NewFragCache, rfrag_dirty = false, wfrag_idx = WFragIdx + 1, wfrag_cache = [], wfrag_cache_size = 0, meta_dirty = false}, SubsNotificationSleepMs};

            Error ->
                {reply, Error, State, 5000}
            end

        end;

    true ->

        case WriteBuffer of
        w ->
            {reply, ok, State#state{wfrag_cache = NewFragCache, wfrag_cache_size = NewFragCacheSize, wfrag_dirty = true}, 5000};

        r ->
            {reply, ok, State#state{rfrag_cache = NewFragCache, rfrag_cache_size = NewFragCacheSize, rfrag_dirty = true}, 5000}

        end

    end;


handle_call(deq, _, #state{ qname = QName, 
                            storage_mod = StorageMod, storage_mod_params = StorageModParams,
                            wfrag_idx = WIdx, rfrag_idx = RIdx, rfrag_cache = RData, wfrag_cache = WData, rfrag_recno = RFragRecno, wfrag_dirty = WDirty} = State) ->

    case RData of
    [<<_:64/big-unsigned-integer,BinMsg/binary>> | Tail] ->
        {reply, {ok, binary_to_term(BinMsg)}, State#state{rfrag_cache = Tail, meta_dirty = true, rfrag_recno = RFragRecno + 1}};

    [] ->

        if (WIdx == RIdx) ->
            {reply, {error, empty}, State};

        ((WIdx - RIdx) == 1) ->

            case WData of
            [<<_:64/big-unsigned-integer,BinMsg/binary>> | Tail] ->
            
                case StorageMod:trash_frag(QName, RIdx, StorageModParams) of
                ok ->

                    if WDirty ->

                        case StorageMod:write_frag(QName, WIdx, WData, WIdx, 1, WIdx, StorageModParams) of
                        ok ->
                            {reply, {ok, binary_to_term(BinMsg)}, State#state{ rfrag_idx = WIdx, rfrag_recno = 1, rfrag_cache = Tail, meta_dirty = true, wfrag_dirty = false, rfrag_dirty = false}};

                        Error ->
                            {reply, Error, State}

                        end;

                    true ->

                        case StorageMod:write_meta(QName, WIdx, 1, WIdx, StorageModParams) of
                        ok ->
                            {reply, {ok, binary_to_term(BinMsg)}, State#state{ rfrag_idx = WIdx, rfrag_recno = 1, rfrag_cache = Tail, meta_dirty = false, wfrag_dirty = false, rfrag_dirty = false}};

                        Error ->
                            {reply, Error, State}

                        end

                    end;

                Error ->
                    {reply, Error, State}

                end;

            [] ->
                {reply, {error, empty}, State}

            end;

        true ->

            case read_frag(QName, RIdx + 1, 0, StorageMod, StorageModParams) of
            {ok, [<<_:64/big-unsigned-integer,BinMsg/binary>> | Tail]} ->

                case StorageMod:trash_frag(QName, RIdx, StorageModParams) of
                ok ->
                    {reply, {ok, binary_to_term(BinMsg)}, State#state{ rfrag_idx = RIdx + 1, rfrag_recno = 1, rfrag_cache = Tail, meta_dirty = true, rfrag_dirty = false}};

                Error ->
                    {reply, Error, State}

                end;

            {ok, []} ->
                {reply, {error, empty_frag}, State};

            Error ->
                {reply, Error, State}
            end

        end

    end;

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

handle_call({subscribe, Pid}, _, #state{subs_dict = SDict} = State) ->

    NewSDict =
    case dict:find(Pid, SDict) of
    {ok, _} ->
        SDict;

    _ ->
        MonitorRef = erlang:monitor(process, Pid),

        dict:store(Pid, {MonitorRef, now()}, SDict)

    end,

    {reply, ok, State#state{subs_dict = NewSDict}};

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

handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', MonRef, Pid, process, _Info}, State) ->

    erlang:demonitor(MonRef),

    {noreply, State#state{subs_dict = dict:erase(Pid)}};

handle_info(timeout, #state{qname = QName, subs_dict = SDict} = State) ->
    
    [Pid ! {nqueue, QName, ready} || {Pid, _} <- dict:to_list(SDict)],

    {noreply, State};

handle_info(_, State) ->
    {noreply, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_, _) -> ok.

do_sync(#state{ qname = QName, 
                storage_mod = StorageMod, storage_mod_params = StorageModParams,
                meta_dirty = MDirty, wfrag_dirty = WDirty, rfrag_dirty = RDirty, rfrag_recno = RFragRecNo, rfrag_idx = RFragIdx, wfrag_idx = WFragIdx, wfrag_cache = WData, rfrag_cache = RData} = State) ->

    %?info({QName, sync}),

    if (MDirty and not (WDirty or RDirty)) ->

        %Example: Only the head pointer has moved ...

        case StorageMod:write_meta(QName, RFragIdx, RFragRecNo, WFragIdx, StorageModParams) of
        ok ->

            {ok, State#state{meta_dirty = false, last_sync = now()}};

        Error ->
            {Error, State}

        end;

    RDirty ->

        case StorageMod:write_frag(QName, RFragIdx, RData, StorageModParams) of
        ok ->

            case StorageMod:write_meta(QName, RFragIdx, RFragRecNo, WFragIdx, StorageModParams) of
            ok ->

                {ok, State#state{wfrag_dirty = false, last_sync = now()}};

            Error ->
                {Error, State}

            end;

        Error ->
            {Error, State}
        end;


    WDirty ->

        case StorageMod:write_frag(QName, WFragIdx, WData, StorageModParams) of
        ok ->

            case StorageMod:write_meta(QName, RFragIdx, RFragRecNo, WFragIdx, StorageModParams) of
            ok ->

                {ok, State#state{wfrag_dirty = false, last_sync = now()}};

            Error ->
                {Error, State}

            end;

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

    case StorageMod:read_frag(QName, FragIdx, StorageModParams) of
    {ok, Data} when is_binary(Data) ->
        parse_frag(Data, [], 0, RecNo);

    Error ->
        Error

    end.

%EOF
