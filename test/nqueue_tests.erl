-module(nqueue_tests).

-include_lib("eunit/include/eunit.hrl").

setup_test() -> 

    error_logger:tty(false),

    ?assertCmd("rm -fr ./nq_unit_test_data/"),

    ?assertEqual(ok, application:load(nq)),

    ?assertEqual(ok, application:set_env(nq, max_frag_size, 128)),

    ?assertEqual(ok, application:set_env(nq, sync_interval_ms, 5000)),

    ?assertEqual(ok, application:set_env(nq, subs_notification_sleep_ms, 1000)),

    ?assertEqual(ok, application:start(nq)).


start_link_test() ->

    {ok, Pid} = nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}]),

    ?assert(is_pid(Pid)),

    ?assertEqual(Pid, global:whereis_name({nqueue, "test"})).

enq_test() ->

    ?assertEqual(ok, nqueue:enq("test", "12345678")).

size_1_test() ->

    ?assertEqual(1, nqueue:size("test")).

deq_test() ->
    
    ?assertEqual({ok, "12345678"},  nqueue:deq("test")).

deq_fun_args_test() ->

    F = fun(Q, M, A) ->

        ?assertEqual("test", Q),
        ?assertEqual("abcd", M),
        ?assertEqual("myargs", A),

        ok

    end,
    
    ?assertEqual(ok, nqueue:enq("test", "abcd")),
    
    ?assertEqual({ok, "abcd"},  nqueue:deq("test", F, "myargs")),

    ?assertEqual({error, empty},  nqueue:deq("test", F, "myargs")),

    ?assertEqual(ok, nqueue:enq("test", "abcd")),
    
    ?assertMatch({error, _}, nqueue:deq("test", F, "myNrgs")),

    ?assertEqual({ok, "abcd"},  nqueue:deq("test", F, "myargs")),

    ok.


size_2_test() ->

    ?assertEqual(0, nqueue:size("test")).

stop_test() ->

    ?assertEqual(ok, nqueue:stop("test")).


buffer_test() ->

    ?assertEqual(ok, application:set_env(nq, max_frag_size, 128)),

    ?assertCmd("rm -fr ./nq_unit_test_data/"),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}])),
    ?assertEqual(0, nqueue:size("test")),

    [ ?assertEqual(ok, nqueue:enq("test", L)) || L <- lists:seq(1, 100)],

    ?assertEqual(100, nqueue:size("test")),

    [ ?assertEqual({ok, L}, nqueue:deq("test")) || L <- lists:seq(1, 100)],
    ?assertEqual(0, nqueue:size("test")),

    [ ?assertEqual(ok, nqueue:enq("test", L)) || L <- lists:seq(1, 1000)],

    ?assertEqual(1000, nqueue:size("test")),

    [ ?assertEqual({ok, L}, nqueue:deq("test")) || L <- lists:seq(1, 1000)],
    ?assertEqual(0, nqueue:size("test")),

    [ ?assertEqual(ok, nqueue:enq("test", L)) || L <- lists:seq(1, 10000)],

    ?assertEqual(10000, nqueue:size("test")),

    [ ?assertEqual({ok, L}, nqueue:deq("test")) || L <- lists:seq(1, 10000)],
    ?assertEqual(0, nqueue:size("test")),


    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 64)],

    ?assertEqual(64, nqueue:size("test")),

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 64)],
    ?assertEqual(0, nqueue:size("test")),

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 32)],

    ?assertEqual(32, nqueue:size("test")),

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 32)],
    ?assertEqual(0, nqueue:size("test")),

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 3)],

    ?assertEqual(3, nqueue:size("test")),

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 3)],
    ?assertEqual(0, nqueue:size("test")),

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 512)],

    ?assertEqual(512, nqueue:size("test")),

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 512)],

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:stop("test")).

buffer2_test() ->

    ?assertEqual(ok, application:set_env(nq, max_frag_size, 128)),

    ?assertCmd("rm -fr ./nq_unit_test_data/"),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}])),
    ?assertEqual(0, nqueue:size("test")),

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 16)],
    ?assertEqual(16, nqueue:size("test")),

    ?assertEqual(ok, nqueue:stop("test")),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}])),

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 16)],
    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:stop("test")),


    ?assertCmd("rm -fr ./nq_unit_test_data/"),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}])),

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 18)],
    ?assertEqual(18, nqueue:size("test")),

    ?assertEqual(ok, nqueue:enq("test", <<"**">>)),
    ?assertEqual(19, nqueue:size("test")),

    ?assertEqual(ok, nqueue:stop("test")),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}])),
    ?assertEqual(19, nqueue:size("test")),

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 18)],
    ?assertEqual(1, nqueue:size("test")),

    ?assertEqual({ok, <<"**">>},  nqueue:deq("test")),
    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:stop("test")),

    ?assertCmd("rm -fr ./nq_unit_test_data/"),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}])),

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 1)],
    ?assertEqual(1, nqueue:size("test")),

    ?assertEqual(ok, nqueue:enq("test", <<"**">>)),
    ?assertEqual(2, nqueue:size("test")),

    ?assertEqual(ok, nqueue:stop("test")),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}])),
    ?assertEqual(2, nqueue:size("test")),

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 1)],
    ?assertEqual(1, nqueue:size("test")),

    ?assertEqual({ok, <<"**">>},  nqueue:deq("test")),
    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:stop("test")).

benchmark1_test() ->

    ?assertEqual(ok, application:set_env(nq, max_frag_size, 128000)),

    ?assertCmd("rm -fr ./nq_unit_test_data/"),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}])),

    enqueue_many("test", {"username", "password", "27000000000", "499", "clientref", "123456789009876543211234567890123456", "Welcome this is a test message"}, 10000),

    ?assertEqual(10000, nqueue:size("test")),

    ?assertEqual(ok, nqueue:sync("test")),

    dequeue_many("test", {"username", "password", "27000000000", "499", "clientref", "123456789009876543211234567890123456", "Welcome this is a test message"}, 10000),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual({error, empty}, nqueue:deq("test")),

    ?assertEqual(ok, nqueue:sync("test")),

    ?assertEqual(ok, nqueue:stop("test")),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}])),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual({error, empty}, nqueue:deq("test")).

handle_test() ->

    ?assertEqual({noreply, state, 0}, nqueue:handle_call(crap, dontcare, state)),
    ?assertEqual({noreply, state, 0}, nqueue:handle_cast(crap, state)),
    ?assertEqual({noreply, state, 0}, nqueue:handle_info(crap, state)),
    ?assertEqual({ok, state}, nqueue:code_change(dontcare, state, dontcare)),
    ?assertEqual(ok, nqueue:terminate(dontcare, dontcare)).


subscribe_test() ->

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:subscribe("test")),

    ?assertEqual(ok, nqueue:enq("test", hello)),

    receive {nqueue, "test", ready} ->

        ?assertEqual({ok, hello}, nqueue:deq("test"))

    end,

    ?assertEqual(ok, nqueue:enq("test", hello2)),

    receive {nqueue, "test", ready} ->

        ?assertEqual({ok, hello2}, nqueue:deq("test"))

    end,

    ok.

unsubscribe_test() ->

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:enq("test", hello)),

    receive {nqueue, "test", ready} ->

        ?assertEqual({ok, hello}, nqueue:deq("test"))

    end,

    ?assertEqual(ok, nqueue:unsubscribe("test")),

    ?assertEqual(ok, nqueue:enq("test", hello)),

    GotEvent =
    receive {nqueue, "test", ready} ->
        true

    after 1000 ->
        false

    end,

    ?assert(not GotEvent),

    ?assertEqual(ok, nqueue:unsubscribe("test")),

    ?assertEqual(1, nqueue:size("test")),

    ?assertEqual(ok, nqueue:subscribe("test")),

    receive {nqueue, "test", ready} ->

        ?assertEqual({ok, hello}, nqueue:deq("test"))

    end,

    ?assertEqual(ok, nqueue:subscribe("test")),

    ?assertEqual(ok, nqueue:unsubscribe("test")),

    ok.


down_test() ->

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(0, nqueue:subscriber_count("test")),

    Caller = self(),

    F = fun () ->

        ?assertEqual(ok, nqueue:subscribe("test")),

        Caller ! the_pig_is_alive,

        receive M0 ->
            ?assertEqual(M0, die_pig_die)
        end

    end,

    Pid = erlang:spawn(F),

    ?assert(is_pid(Pid)),

    Ref = erlang:monitor(process, Pid),

    receive M1 ->
        ?assertEqual(M1, the_pig_is_alive)

    end,

    Pid ! die_pig_die,
 
    receive M2 ->   
        ?assertMatch( {'DOWN', Ref, process, Pid, normal}, M2)

    end,

    ?assert(not  erlang:is_process_alive(Pid)),

    ?assertEqual(0, nqueue:subscriber_count("test")),

    ok.
    

teardown_test() -> 
    ?assertEqual(ok, application:stop(nq)),
    ?assertEqual(ok, application:unload(nq)).
 

%Helper funcs
enqueue_many(QName, Msg, Count) when Count > 0 ->

    ok = nqueue:enq("test", Msg),

    enqueue_many(QName, Msg, Count -1);

enqueue_many(_, _, 0) -> ok.

dequeue_many(QName, Msg, Count) when Count > 0 ->

    {ok, Msg} = nqueue:deq("test"),

    dequeue_many(QName, Msg, Count -1);

dequeue_many(_, _, 0) -> ok.


%EOF
