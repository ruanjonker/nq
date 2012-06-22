-module(nqueue_consumer_tests).

-include_lib("eunit/include/eunit.hrl").

setup_test() ->

    error_logger:tty(false),

    ?assertCmd("rm -fr nqdata"),

    ?assertEqual(ok, application:start(nq)),

    ?assertMatch({ok, P} when is_pid(P), nqueue:start_link("test")),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual({ok, 0}, bdb_store:count("consumer_cache")),

    ok.


start_link_test() ->

    F = fun(_Q,M,A) -> A ! M, ok end,

    Fe = fun(_Q,_M,_E,_A) -> ok end,

    ?assertMatch({ok, P} when is_pid(P),  nqueue_consumer:start_link("test", "test", F, self(), Fe, self(), unpaused)),

    ?assertEqual(1, nqueue:subscriber_count("test")),

    ok.

consume_1_test() ->

    ?assertEqual(ok, nqueue:enq("test", hello12345)),

    receive M0 ->
        ?assertEqual(M0, hello12345)

    end,

    ?assertEqual({ok, 0}, bdb_store:count("consumer_cache")),


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


