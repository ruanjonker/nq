-module(nqueue_consumer_tests).

-include("nq.hrl").

-include_lib("eunit/include/eunit.hrl").

setup_test() ->

    error_logger:tty(false),

    ?assertCmd("rm -fr nqdata"),

    ?assertEqual(ok, application:start(nq)),

    ?assertMatch({ok, P} when is_pid(P), nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nqdata/"}, {max_frag_size, 4096 * 1000}])),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual({ok, 0}, bdb_store:count("nq_consumer_cache")),

%    error_logger:tty(true),

    ok.


start_link_test() ->

    F = fun(_Q,M,A) -> {ok, S} =  bdb_store:count("nq_consumer_cache"),  A ! {M, S}, ok end,

    Fe = fun(_Q,_M,_E,_A) -> ok end,

    ?assertMatch({ok, P} when is_pid(P),  nqueue_consumer:start_link("test", "test", F, self(), Fe, self(), unpaused)),

    ?assertEqual(1, nqueue:subscriber_count("test")),

    ok.

consume_1_test() ->

    ?assertEqual(0, nqueue:size("test")),
    ?assertEqual(ok, nqueue:enq("test", hello12345)),

    receive M0 ->
        ?assertEqual(M0, {hello12345, 1})

    end,

    ?assertEqual(unpaused, nqueue_consumer:get_state("test")),

    ?assertEqual({ok, 0}, bdb_store:count("nq_consumer_cache")),
    ?assertEqual(0, nqueue:size("test")),

    ok.

set_fun_test() ->

    F = fun(_,_,A) -> A end,

    ?assertEqual(ok, nqueue_consumer:set_fun("test", F, 'some error')),

    ok.

set_err_fun_test() ->

    Fe = fun(_,M,E, A) -> A ! {M, E}, ok end,

    ?assertEqual(ok, nqueue_consumer:set_err_fun("test", Fe, self())),

    ok.

consume_err_test() ->

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:enq("test", bla)),

    receive M0 ->
        ?assertEqual({bla, 'some error'}, M0)
    end,

    ?assertEqual(0, nqueue:size("test")),

    ok.

consume_err_err_fun_test_() ->

    {timeout, 20, [fun() ->

    ?assertMatch({ok, {Mega,Secs,Micro}} when is_integer(Mega) and is_integer(Secs) and is_integer(Micro), application:get_env(nq, session)),

    {ok, AppSessionId} = application:get_env(nq, session),

    Fe = fun(_,M,E, A) -> A ! {M, E}, error end,

    ?assertEqual(ok, nqueue_consumer:set_err_fun("test", Fe, self())),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:enq("test", bla)),


    receive M0 ->
        ?assertEqual({bla, 'some error'}, M0)
    end,

    ?assertEqual(ok, nqueue_consumer:set_state("test", paused)),

    ?assertEqual(0, nqueue:size("test")),
    ?assertEqual({ok, 1}, bdb_store:count("nq_consumer_cache")),
    
    P1 = global:whereis_name({nqueue_consumer, "test"}),

    ?assertMatch({ok, {"test", bla, {_,_,_}, C, 5000}} when (C >= 1), ?dbget("nq_consumer_cache", {P1, AppSessionId})),

    ?assertEqual(ok, nqueue_consumer:set_state("test", unpaused)),

    receive M1 ->
        ?assertEqual({bla, 'some error'}, M1)
    end,

    ?assertEqual(ok, nqueue_consumer:set_state("test", paused)),

    ?assertMatch({ok, {"test", bla, {_,_,_}, C, 5000}} when (C >= 2), ?dbget("nq_consumer_cache", {P1, AppSessionId})),
    ?assertEqual({ok, 1}, bdb_store:count("nq_consumer_cache")),

    Fe2 = fun(_,M,E, A) -> A ! {M, E}, ok end,

    ?assertEqual(ok, nqueue_consumer:set_err_fun("test", Fe2, self())),

    ?assertEqual(ok, nqueue_consumer:set_state("test", unpaused)),

    receive M2 ->
        ?assertEqual({bla, 'some error'}, M2)
    end,

    ?assertEqual(unpaused, nqueue_consumer:get_state("test")),

    ?assertEqual({ok, 0}, bdb_store:count("nq_consumer_cache")),

    F1 = fun(_,M,A) -> A ! M, ok end,
    Fe3 = fun(_,_,_,_) -> ok end,

    ?assertEqual(ok, nqueue_consumer:set_fun("test", F1, self())),
    ?assertEqual(ok, nqueue_consumer:set_err_fun("test", Fe3, self())),

    ?assertEqual(ok, nqueue:enq("test", hello12345)),

    receive M3 ->
        ?assertEqual(M3, hello12345)

    end,

    ?assertEqual(unpaused, nqueue_consumer:get_state("test")),

    ?assertEqual({ok, 0}, bdb_store:count("nq_consumer_cache")),

    ok

    end]}.

receive_many_test_() ->

    {timeout, 60, [fun() ->

        ?assertEqual(ok, nqueue_consumer:set_state("test", paused)),
        ?assertEqual(paused, nqueue_consumer:get_state("test")),

        ?assertEqual(ok, nqueue_consumer:unpause("test")),
        ?assertEqual(unpaused, nqueue_consumer:get_state("test")),

        ?assertEqual(ok, nqueue_consumer:pause("test")),
        ?assertEqual(paused, nqueue_consumer:get_state("test")),

        ?assertEqual(0, nqueue:size("test")),
        ?assertEqual({ok, 0}, bdb_store:count("nq_consumer_cache")),

        ?assertEqual(ok, enqueue_many("test", "this is a test message", 10000)),

        ?assertEqual(10000, nqueue:size("test")),

        F1 = fun(_,M,A) -> A ! M, ok end,

        ?assertEqual(ok, nqueue_consumer:set_fun("test", F1, self())),
 
        ?assertEqual(ok, nqueue_consumer:set_state("test", unpaused)),

        ?assertEqual(ok, receive_many("this is a test message", 10000)),

        ?assertEqual(unpaused, nqueue_consumer:get_state("test")),

        ?assertEqual(0, nqueue:size("test")),

        ?assertEqual({ok, 0}, bdb_store:count("nq_consumer_cache")),

        ok

    end]}.

process_n_test_() ->

    {timeout, 30, [ fun() ->

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual({ok, 0}, bdb_store:count("nq_consumer_cache")),

    ?assertEqual(ok, nqueue_consumer:pause("test")),

    ?assertEqual(ok, enqueue_many("test", "this is a test message", 100)),

    F1 = fun(_,M,A) -> A ! M, ok end,

    ?assertEqual(ok, nqueue_consumer:set_fun("test", F1, self())),
 
    ?assertEqual(ok, nqueue_consumer:process_n("test", 5)),

    ?assertEqual(ok, receive_many("this is a test message", 5)),

    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    ?assertEqual(95, nqueue:size("test")),

    ?assertEqual(ok, nqueue_consumer:process_n("test", 55)),

    ?assertEqual(ok, receive_many("this is a test message", 55)),

    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    ?assertEqual(40, nqueue:size("test")),

    F2 = fun(_,M,A) -> timer:sleep(1000), A ! M, ok end,

    ?assertEqual(ok, nqueue_consumer:set_fun("test", F2, self())),

    ?assertEqual(ok, nqueue_consumer:process_n("test", 10)),

    ?assertEqual(ok, nqueue_consumer:pause("test")),

    ?assertEqual(ok, receive_many("this is a test message", 1)),

    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    ?assertEqual(39, nqueue:size("test")),

    ?assertEqual(ok, nqueue_consumer:set_fun("test", F1, self())),

    ?assertEqual(ok, nqueue_consumer:process_n("test", all)),

    ?assertEqual(ok, receive_many("this is a test message", 39)),

    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue_consumer:process_n("test", 100)),

    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    ?assertEqual(ok, enqueue_many("test", "this is a test message", 100)),

    F3 = fun(_,M,A) -> timer:sleep(1), A ! M, ok end,

    ?assertEqual(ok, nqueue_consumer:set_fun("test", F3, self())),
 
    ?assertEqual(ok, nqueue_consumer:process_n("test", all)),

    ?assertEqual({ok, "this is a test message"}, nqueue:deq("test")),

    ?assertEqual(ok, receive_many("this is a test message", 99)),

    ?assertEqual(ok, nqueue_consumer:set_fun("test", F1, self())),

    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    ?assertEqual(0, nqueue:size("test")),

    ok

    end]}.

consumer_proc_fun_test_() ->

    {timeout, 5, [fun() ->

    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    ?assertEqual(ok, enqueue_many("test", "this is a test message", 2)),

    F3 = fun(_,M,A) -> A ! {now(), M}, {ok, 1000, A} end,

    ?assertEqual(ok, nqueue_consumer:set_fun("test", F3, self())),
 
    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    ?assertEqual(ok, nqueue_consumer:unpause("test")),

    receive {T0, M0} ->
        ?assertEqual(M0, "this is a test message")

    end,

    receive {T1, M1} ->
        ?assertEqual(M1, "this is a test message")

    end,

    DiffMs = timer:now_diff(T1, T0)/1000,

    ?assert(DiffMs >= 1000),

    ?assertEqual(ok, nqueue_consumer:pause("test")),

    ok

    end]}.

consumer_proc_fun_retry_test_() ->

    {timeout, 5, [fun() ->

    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    ?assertEqual(ok, enqueue_many("test", "this is a test message", 2)),

    F3 = fun(_,M, {A, C}) -> if (C >= 1) ->  A ! {now(), M, C}, {ok, 0, {A, 0}}; true -> {retry, 1000, {A, C + 1}} end end,

    ?assertEqual(ok, nqueue_consumer:set_fun("test", F3, {self(), 0})),
 
    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    T0 = now(),
    ?assertEqual(ok, nqueue_consumer:unpause("test")),

    receive {T1, M0, 1} ->
        ?assertEqual(M0, "this is a test message")

    end,

    ?assert((timer:now_diff(T1, T0)/1000) > 1000),

    receive {T2, M1, 1} ->
        ?assertEqual(M1, "this is a test message")

    end,

    DiffMs = timer:now_diff(T2, T0)/1000,

    ?assert(DiffMs > 2000),

    ?assertEqual(ok, nqueue_consumer:pause("test")),

    ?assertEqual(0, nqueue:size("test")),

    ok

    end]}.


handle_info_queue_ready_notification_test() ->

    P1 = global:whereis_name({nqueue_consumer, "test"}),

    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    P1 ! {nqueue, "test", ready},

    ?assertEqual(paused, nqueue_consumer:get_state("test")),

    ok.

teardown_test() ->

    P1 = global:whereis_name({nqueue_consumer, "test"}),
    P2 = global:whereis_name({nqueue, "test"}),

    unlink(P1),
    unlink(P2),

    exit(P1, kill),
    exit(P2, kill),

    ?assertEqual(ok, application:stop(nq)),

    ?assertEqual(ok, application:unload(nq)),

    ok.


enqueue_many(QName, Msg, Count) when Count > 0 ->

    ok = nqueue:enq("test", Msg),

    enqueue_many(QName, Msg, Count -1);

enqueue_many(_, _, 0) -> ok.

receive_many(Msg, Count) when Count > 0 ->
    receive M3 ->
        ?assertEqual(M3, Msg)

    end,

    receive_many(Msg, Count - 1);

receive_many(_, 0) -> ok.



