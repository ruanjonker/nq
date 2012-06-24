-module(nqueue_consumer_tests).

-include("nq.hrl").

-include_lib("eunit/include/eunit.hrl").

setup_test() ->

    error_logger:tty(false),

    ?assertCmd("rm -fr nqdata"),

    ?assertEqual(ok, application:start(nq)),

    ?assertMatch({ok, P} when is_pid(P), nqueue:start_link("test")),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual({ok, 0}, bdb_store:count("consumer_cache")),

%    error_logger:tty(true),

    ok.


start_link_test() ->

    F = fun(_Q,M,A) -> {ok, S} =  bdb_store:count("consumer_cache"),  A ! {M, S}, ok end,

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

    ?assertEqual({ok, 0}, bdb_store:count("consumer_cache")),
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

    {timeout, 10000, [fun() ->

    Fe = fun(_,M,E, A) -> A ! {M, E}, error end,

    ?assertEqual(ok, nqueue_consumer:set_err_fun("test", Fe, self())),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:enq("test", bla)),

    receive M0 ->
        ?assertEqual({bla, 'some error'}, M0)
    end,

    ?assertEqual(ok, nqueue_consumer:set_state("test", paused)),

    ?assertEqual(0, nqueue:size("test")),
    ?assertEqual({ok, 1}, bdb_store:count("consumer_cache")),
    
    P1 = global:whereis_name({nqueue_consumer, "test"}),

    ?assertMatch({ok, {"test", bla, {_,_,_}, C}} when (C >= 1), ?dbget("consumer_cache", P1)),

    ?assertEqual(ok, nqueue_consumer:set_state("test", unpaused)),

    receive M1 ->
        ?assertEqual({bla, 'some error'}, M1)
    end,

    ?assertEqual(ok, nqueue_consumer:set_state("test", paused)),

    ?assertMatch({ok, {"test", bla, {_,_,_}, C}} when (C >= 2), ?dbget("consumer_cache", P1)),
    ?assertEqual({ok, 1}, bdb_store:count("consumer_cache")),

    Fe2 = fun(_,M,E, A) -> A ! {M, E}, ok end,

    ?assertEqual(ok, nqueue_consumer:set_err_fun("test", Fe2, self())),

    ?assertEqual(ok, nqueue_consumer:set_state("test", unpaused)),

    receive M2 ->
        ?assertEqual({bla, 'some error'}, M2)
    end,

    ?assertEqual({ok, 0}, bdb_store:count("consumer_cache")),

    F1 = fun(_,M,A) -> A ! M, ok end,
    Fe3 = fun(_,_,_,_) -> ok end,

    ?assertEqual(ok, nqueue_consumer:set_fun("test", F1, self())),
    ?assertEqual(ok, nqueue_consumer:set_err_fun("test", Fe3, self())),

    ?assertEqual(ok, nqueue:enq("test", hello12345)),

    receive M3 ->
        ?assertEqual(M3, hello12345)

    end,

    ?assertEqual(unpaused, nqueue_consumer:get_state("test")),

    ?assertEqual({ok, 0}, bdb_store:count("consumer_cache")),

    ok

    end]}.

receive_many_test_() ->

    {timeout, 60000, [fun() ->

        ?assertEqual(ok, nqueue_consumer:set_state("test", paused)),
        ?assertEqual(paused, nqueue_consumer:get_state("test")),
        ?assertEqual(0, nqueue:size("test")),
        ?assertEqual({ok, 0}, bdb_store:count("consumer_cache")),


        ?assertEqual(ok, enqueue_many("test", "this is a test message", 100000)),

        ?assertEqual(100000, nqueue:size("test")),

        F1 = fun(_,M,A) -> A ! M, ok end,

        ?assertEqual(ok, nqueue_consumer:set_fun("test", F1, self())),
 
        ?assertEqual(ok, nqueue_consumer:set_state("test", unpaused)),

        ?assertEqual(ok, receive_many("this is a test message", 100000)),

        ?assertEqual(0, nqueue:size("test")),

        ?assertEqual({ok, 0}, bdb_store:count("consumer_cache")),

        ok

    end]}.

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



