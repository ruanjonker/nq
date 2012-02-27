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
        sync/1

        ]).

-record(state, {tref, qname, meta_dirty = false, rfrag_idx = 0, rfrag_recno = 0, rfrag_cache_size = 0, rfrag_cache = [], rfrag_dirty = false, wfrag_cache = [], wfrag_dirty = false, wfrag_cache_size = 0, wfrag_idx = 0, last_sync = now()}).

-define(NAME(X), {global, {?MODULE, X}}).

start_link(QName) when is_list(QName) ->
    gen_server:start_link(?NAME(QName), ?MODULE, [QName], []).

enq(QName, Msg) ->
    gen_server:call(?NAME(QName), {enq, Msg}, infinity).

deq(QName) ->
    gen_server:call(?NAME(QName), deq, infinity).

sync(QName) ->
    gen_server:call(?NAME(QName), sync, infinity).

stop(QName) ->
    gen_server:call(?NAME(QName), stop, infinity).

init([QName]) ->

    {ok, {TRFragIdx, TRFragRecno, TWFragIdx}} = read_meta(QName),
    
    {ok, RData} = read_frag(QName, TRFragIdx, TRFragRecno),


    {RFragIdx, RFragRecno, WFragIdx, WData} = 
    if (TRFragIdx =/= TWFragIdx) ->

        WFragSize = frag_size(QName, TWFragIdx),

        {ok, MaxFragSize} =  application:get_env(nq, max_frag_size),

        if (WFragSize >= MaxFragSize) ->
            {TRFragIdx, TRFragRecno, TWFragIdx + 1, []};
        true ->

            {ok, WD} = read_frag(QName, TWFragIdx, 0),

            {TRFragIdx, TRFragRecno, TWFragIdx, WD}
        end;

    true ->
        {TRFragIdx, TRFragRecno, TWFragIdx, []}

    end,

    {ok, SyncIntervalMs} = application:get_env(nq, sync_interval_ms),

    {ok, TRef} = timer:apply_interval(SyncIntervalMs, ?MODULE, sync, [QName]),

    {ok, #state{qname = QName, tref = TRef, rfrag_idx = RFragIdx, rfrag_recno = RFragRecno, rfrag_cache = RData, wfrag_cache = WData, wfrag_idx = WFragIdx}}.

    


handle_call({enq, Msg}, _, #state{  
                                qname = QName, 

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

    if (NewFragCacheSize >= MaxFragCacheSize) ->

        case WriteBuffer of
        w ->

            case write_frag(QName, FragIdx, NewFragCache, RFragIdx, RFragRecNo, WFragIdx + 1) of
            ok ->
                {reply, ok, State#state{wfrag_cache = [] , wfrag_dirty = false, wfrag_idx = WFragIdx + 1, wfrag_cache_size = 0, meta_dirty = false}};

            Error ->
                {reply, Error, State, 5000}
            end;

        r ->

            case write_frag(QName, FragIdx, NewFragCache, RFragIdx, RFragRecNo, WFragIdx + 1) of
            ok ->
                {reply, ok, State#state{rfrag_cache = NewFragCache, rfrag_dirty = false, wfrag_idx = WFragIdx + 1, wfrag_cache = [], wfrag_cache_size = 0, meta_dirty = false}};

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


handle_call(deq, _, #state{qname = QName, wfrag_idx = WIdx, rfrag_idx = RIdx, rfrag_cache = RData, wfrag_cache = WData, rfrag_recno = RFragRecno, wfrag_dirty = WDirty} = State) ->

    case RData of
    [<<_:64/big-unsigned-integer,BinMsg/binary>> | Tail] ->
        {reply, {ok, binary_to_term(BinMsg)}, State#state{rfrag_cache = Tail, meta_dirty = true, rfrag_recno = RFragRecno + 1}};

    [] ->

        if (WIdx == RIdx) ->
            {reply, {error, empty}, State};

        ((WIdx - RIdx) == 1) ->

            case WData of
            [<<_:64/big-unsigned-integer,BinMsg/binary>> | Tail] ->
            
                case trash_frag(QName, RIdx) of
                ok ->

                    if WDirty ->

                        case write_frag(QName, WIdx, WData, WIdx, 1, WIdx) of
                        ok ->
                            {reply, {ok, binary_to_term(BinMsg)}, State#state{ rfrag_idx = WIdx, rfrag_recno = 1, rfrag_cache = Tail, meta_dirty = true, wfrag_dirty = false, rfrag_dirty = false}};

                        Error ->
                            {reply, Error, State}

                        end;

                    true ->

                        case write_meta(QName, WIdx, 1, WIdx) of
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

            case read_frag(QName, RIdx + 1, 0) of
            {ok, [<<_:64/big-unsigned-integer,BinMsg/binary>> | Tail]} ->

                case trash_frag(QName, RIdx) of
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

handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

code_change(_, State, _) ->

    {ok, State}.

terminate(_, _) -> ok.

do_sync(#state{qname = QName, meta_dirty = MDirty, wfrag_dirty = WDirty, rfrag_dirty = RDirty, rfrag_recno = RFragRecNo, rfrag_idx = RFragIdx, wfrag_idx = WFragIdx, wfrag_cache = WData, rfrag_cache = RData} = State) ->

    %?info({QName, sync}),

    if (MDirty and not (WDirty or RDirty)) ->

        %Example: Only the head pointer has moved ...

        case write_meta(QName, RFragIdx, RFragRecNo, WFragIdx) of
        ok ->

            {ok, State#state{meta_dirty = false, last_sync = now()}};

        Error ->
            {Error, State}

        end;

    RDirty ->

        case write_frag(QName, RFragIdx, RData) of
        ok ->

            case write_meta(QName, RFragIdx, RFragRecNo, WFragIdx) of
            ok ->

                {ok, State#state{wfrag_dirty = false, last_sync = now()}};

            Error ->
                {Error, State}

            end;

        Error ->
            {Error, State}
        end;


    WDirty ->

        case write_frag(QName, WFragIdx, WData) of
        ok ->

            case write_meta(QName, RFragIdx, RFragRecNo, WFragIdx) of
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


read_frag(QName, FragIdx, RecNo) ->

    {ok, BaseDir} = application:get_env(nq, base_dir),

    Filename = BaseDir ++ QName ++ ".frag." ++ integer_to_list(FragIdx),

    case file:read_file(Filename) of
    {ok, Data} ->
        parse_frag(Data, [], 0, RecNo);

    {error, enoent} ->

        {ok, []};

    Error ->
        Error
    end.

parse_frag(<<MsgSize:64/big-unsigned-integer,Rest/binary>>, MsgList, MsgIdx, RecNo) -> 

    if (MsgIdx < RecNo) ->

        parse_frag(erlang:binary_part(Rest, MsgSize, byte_size(Rest) - MsgSize), MsgList, MsgIdx + 1, RecNo);

    true ->

        MsgBody= erlang:binary_part(Rest, 0, MsgSize),

        parse_frag(erlang:binary_part(Rest, MsgSize, byte_size(Rest) - MsgSize), lists:append(MsgList, [<<MsgSize:64/big-unsigned-integer, MsgBody/binary>>]), MsgIdx + 1, RecNo)


    end;

parse_frag(<<>>, MsgList, _, _) -> {ok, MsgList}.

trash_frag(QName, FragIdx) ->

    {ok, BaseDir} = application:get_env(nq, base_dir),

    Filename = BaseDir ++ QName ++ ".frag." ++ integer_to_list(FragIdx),

 %   ?info("Trashing: " ++ Filename),

    case file:delete(Filename) of
    ok ->
        ok;       

    {error,enoent} ->
        ok;

    Error ->
        Error
    end.


write_frag(QName, FragIdx, Data) ->

    {ok, BaseDir} = application:get_env(nq, base_dir),

    Filename = BaseDir ++ QName ++ ".frag." ++ integer_to_list(FragIdx),

    case file:write_file(Filename, Data, [raw, binary]) of
    ok ->
        ok;       

    Error ->
        Error
    end.


write_frag(QName, FragIdx, Data, RFragIdx, RFragRecNo, WFragIdx) ->

    case write_frag(QName, FragIdx, Data) of
    ok ->
        
        write_meta(QName, RFragIdx, RFragRecNo, WFragIdx); 

    Error ->
        Error
    end.

frag_size(QName, FragIdx) ->

    {ok, BaseDir} = application:get_env(nq, base_dir),

    Filename = BaseDir ++ QName ++ ".frag." ++ integer_to_list(FragIdx),

    filelib:file_size(Filename).

read_meta(QName) ->

    {ok, BaseDir} = application:get_env(nq, base_dir),

    Filename = BaseDir ++ QName ++ ".meta",

    case file:read_file(Filename) of
    {ok, <<RFragIdx:64/big-unsigned-integer,RFragRecNo:64/big-unsigned-integer,WFragIdx:64/big-unsigned-integer>>} ->
        {ok, {RFragIdx, RFragRecNo, WFragIdx}};

    {error, enoent} ->
        {ok, {0,0,0}};

    Error ->
        Error
    end.

write_meta(QName, RFragIdx, RFragRecNo, WFragIdx) ->

    {ok, BaseDir} = application:get_env(nq, base_dir),

    Filename = BaseDir ++ QName ++ ".meta",

    file:rename(Filename, Filename ++ ".prev"),

    case file:write_file(Filename ++ ".tmp", <<RFragIdx:64/big-unsigned-integer,RFragRecNo:64/big-unsigned-integer,WFragIdx:64/big-unsigned-integer>>, [raw, binary, write]) of
    ok ->
        file:rename(Filename ++ ".tmp", Filename);

    Error ->

        %Try restoring previous file ...
        file:rename(Filename ++ ".prev", Filename),

        Error

    end.


%EOF
