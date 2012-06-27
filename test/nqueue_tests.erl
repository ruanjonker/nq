-module(nqueue_tests).

-include("nq_queue.hrl").

-include_lib("eunit/include/eunit.hrl").

setup_test() -> 

    error_logger:tty(false),

    ?assertCmd("rm -fr ./nq_unit_test_data/"),

    ?assertEqual(ok, application:load(nq)),

    ?assertEqual(ok, application:set_env(nq, max_frag_size, 128)),

    ?assertEqual(ok, application:set_env(nq, sync_interval_ms, 5000)),

    ?assertEqual(ok, application:start(nq)).


start_link_test() ->

    {ok, Pid} = nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}, {auto_sync, false}]),

    ?assert(is_pid(Pid)),

    ?assertEqual(Pid, global:whereis_name({nqueue, "test"})).

enq_test() ->

    ?assertEqual(ok, nqueue:enq("test", "12345678")).

get_meta_1_test() ->

    BinMsg = term_to_binary("12345678"),

    BinMsgSize = size(BinMsg),

    Ce = <<BinMsgSize:64/unsigned-integer-big, BinMsg/binary>>,

    RS1 = 8 + BinMsgSize,

    ?assertMatch({1, true, 0, 0, RS1, [Ce], true, 0, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual(ok, nqueue:enq("test", "12345678")),

    RS2 = 2 * RS1,

    ?assertMatch({2, true, 0, 0, RS2, [Ce, Ce], true, 0, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual(ok, nqueue:enq("test", "12345678")),

    RS3 = 3 * RS1,

    ?assertMatch({3, true, 0, 0, RS3, [Ce, Ce, Ce], true, 0, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual(ok, nqueue:enq("test", "12345678")),

    RS4 = 4 * RS1,

    ?assertMatch({4, true, 0, 0, RS4, [Ce, Ce, Ce, Ce], true, 0, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual({ok, "12345678"}, nqueue:deq("test")),
   
    ?assertMatch({3, true, 0, 0, RS3, [Ce, Ce, Ce], true, 0, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual({ok, "12345678"}, nqueue:deq("test")),
   
    ?assertMatch({2, true, 0, 0, RS2, [Ce, Ce], true, 0, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual({ok, "12345678"}, nqueue:deq("test")),
   
    ?assertMatch({1, true, 0, 0, RS1, [Ce], true, 0, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual({ok, "12345678"}, nqueue:deq("test")),
   
    ?assertMatch({0, true, 0, 0, 0, [], true, 0, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual({error, empty}, nqueue:deq("test")),
   
    ?assertMatch({0, true, 0, 0, 0, [], true, 0, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual(ok, nqueue:sync("test")),

    ?assertMatch({0, false, 0, 0, 0, [], false, 0, 0, [], false, _}, nqueue:get_meta("test")),

    ok.

size_1_test() ->

    ?assertEqual(ok, nqueue:enq("test", "12345678")),

    ?assertMatch(N when (is_integer(N) and (N >= 0)), nqueue:size("test")).

purge_test() ->

    ?assertMatch(N when N > 0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:purge("test")),

    ?assertEqual(0, nqueue:size("test")),

    ok.

deq_test() ->

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(ok, nqueue:enq("test", "12345678")),

    ?assertEqual(1, nqueue:size("test")),
    
    ?assertEqual({ok, "12345678"},  nqueue:deq("test")),

    ?assertEqual(0, nqueue:size("test")),

    ok.

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

    PX = global:whereis_name({nqueue, "test"}),

    ?assertEqual(ok, nqueue:stop("test")),

    ?assertEqual(ok, wait_to_die(PX)).


buffer_1_test() ->

    BinMsg = term_to_binary("12345678"),

    BinMsgSize = size(BinMsg),

    ?assertEqual(ok, application:set_env(nq, max_frag_size, (8 + BinMsgSize) * 2)),

    ?assertCmd("rm -fr ./nq_unit_test_data/"),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}, {auto_sync, false}])),

    ?assertEqual(0, nqueue:size("test")),

    Ce = <<BinMsgSize:64/unsigned-integer-big, BinMsg/binary>>,

    RS1 = 8 + BinMsgSize,

    ?assertEqual(ok, nqueue:enq("test", "12345678")),
    ?assertMatch({1, true, 0, 0, RS1, [Ce], true, 0, 0, [], false, _}, nqueue:get_meta("test")),

    RS2 = RS1 * 2,

    ?assertEqual(ok, nqueue:enq("test", "12345678")),
    ?assertMatch({2, false, 0, 0, RS2, [Ce, Ce], false, 1, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual(ok, nqueue:enq("test", "12345678")),
    ?assertMatch({3, false, 0, 0, RS2, [Ce, Ce], false, 1, RS1, [Ce], true, _}, nqueue:get_meta("test")),

    ?assertEqual(ok, nqueue:enq("test", "12345678")),
    ?assertMatch({4, false, 0, 0, RS2, [Ce, Ce], false, 2, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual(ok, nqueue:enq("test", "12345678")),
    ?assertMatch({5, false, 0, 0, RS2, [Ce, Ce], false, 2, RS1, [Ce], true, _}, nqueue:get_meta("test")),

    ?assertEqual(ok, nqueue:sync("test")),
    ?assertMatch({5, false, 0, 0, RS2, [Ce, Ce], false, 2, RS1, [Ce], false, _}, nqueue:get_meta("test")),


    ?assertEqual({ok, "12345678"}, nqueue:deq("test")),
    ?assertMatch({4, true, 0, 1, RS2, [Ce, Ce], false, 2, RS1, [Ce], false, _}, nqueue:get_meta("test")),

    ?assertEqual({ok, "12345678"}, nqueue:deq("test")),
    ?assertMatch({3, true, 0, 2, RS2, [Ce, Ce], false, 2, RS1, [Ce], false, _}, nqueue:get_meta("test")),

    ?assertEqual({ok, "12345678"}, nqueue:deq("test")),
    ?assertMatch({2, true, 1, 1, RS2, [Ce, Ce], false, 2, RS1, [Ce], false, _}, nqueue:get_meta("test")),

    ?assertEqual({ok, "12345678"}, nqueue:deq("test")),
    ?assertMatch({1, true, 1, 2, RS2, [Ce, Ce], false, 2, RS1, [Ce], false, _}, nqueue:get_meta("test")),

    ?assertEqual(ok, nqueue:stop("test")),

    P1 = global:whereis_name({nqueue, "test"}),

    ?assertEqual(ok, wait_to_die(P1)),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}, {auto_sync, false}])),

    ?assertEqual(1, nqueue:size("test")),
    ?assertMatch({1, false, 1, 2, RS2, [Ce, Ce], false, 2, RS1, [Ce], false, _}, nqueue:get_meta("test")),


    ?assertEqual({ok, "12345678"}, nqueue:deq("test")),
    ?assertMatch({0, true, 2, 1, RS1, [Ce], false, 2, 0, [], false, _}, nqueue:get_meta("test")),

    ?assertEqual(ok, nqueue:stop("test")),

    PX = global:whereis_name({nqueue, "test"}),

    ?assertEqual(ok, wait_to_die(PX)),

    ok.

buffer_2_test() ->


    ?assertEqual(ok, application:set_env(nq, max_frag_size, 128)),

    ?assertCmd("rm -fr ./nq_unit_test_data/"),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}, {auto_sync, false}])),
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

    enqueue_many("test", {"username", "password", "27000000000", "499", "clientref", "123456789009876543211234567890123456", "Welcome this is a test message"}, 1000),

    ?assertEqual(1000, nqueue:size("test")),

    ?assertEqual(ok, nqueue:sync("test")),

    dequeue_many("test", {"username", "password", "27000000000", "499", "clientref", "123456789009876543211234567890123456", "Welcome this is a test message"}, 1000),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual({error, empty}, nqueue:deq("test")),

    ?assertEqual(ok, nqueue:sync("test")),

    ?assertEqual(ok, nqueue:stop("test")),

    ?assertMatch({ok, _}, nqueue:start_link("test", [{storage_mod, nq_file}, {storage_mod_params, "./nq_unit_test_data/"}])),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual({error, empty}, nqueue:deq("test")).

handle_test() ->

    ?assertEqual({noreply, state}, nqueue:handle_call(crap, dontcare, state)),
    ?assertEqual({noreply, state}, nqueue:handle_cast(crap, state)),
    ?assertEqual({noreply, state}, nqueue:handle_info(crap, state)),
    ?assertEqual({ok, state}, nqueue:code_change(dontcare, state, dontcare)),

    StateIn = #state{meta_dirty = false, wfrag_dirty = false, rfrag_dirty = false},

    ?assertEqual({ok, StateIn}, nqueue:terminate(dontcare, StateIn)).


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
 
peek_test() ->

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual(0, nqueue:subscriber_count("test")),

    ?assertEqual(ok, nqueue:enq("test", hello)),

    ?assertEqual({ok, hello}, nqueue:peek("test")),

    ?assertEqual(1, nqueue:size("test")),

    ?assertEqual({ok, hello}, nqueue:deq("test")),

    ?assertEqual(0, nqueue:size("test")),

    ?assertEqual({error, empty}, nqueue:deq("test")),

    ok.   

enq_deq_many_test_() ->

    {timeout, 30000, [fun() ->

        ?assertEqual(0, nqueue:size("test")),

        ?assertEqual(ok, nqueue:purge("test")),

        Meta = nqueue:get_meta("test"),

        ?assertMatch({0, false, 0, 0, 0, [], false, 0, 0, [], false, {_,_,_}}, Meta),

        ?assertEqual(0, nqueue:size("test")),

        ?assertEqual(ok, enq_deq_many("test", "test message 12345", 10000)),

        ?assertEqual(0, nqueue:size("test")),

        ?assertMatch({0, true, 0, 0, 0, [], true, 0, 0, [], false, {_,_,_}}, nqueue:get_meta("test")),

        ?assertEqual(ok, nqueue:sync("test")),

        ?assertMatch({0, false, 0, 0, 0, [], false, 0, 0, [], false, {_,_,_}}, nqueue:get_meta("test")),

        ok

    end]}.


teardown_test() -> 

    P2 = global:whereis_name({nqueue, "test"}),

    unlink(P2),

    exit(P2, kill),

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


enq_deq_many(QName, Msg, Count) when Count > 0 ->

    ?assertEqual(ok, nqueue:enq(QName, {Msg, Count})),

    ?assertEqual({ok, {Msg, Count}}, nqueue:deq(QName)),

    enq_deq_many(QName, Msg, Count - 1); 

enq_deq_many(_, _, 0) -> ok.


wait_to_die(Pid) ->

    Alive = erlang:is_process_alive(Pid),

    if (not Alive) ->
        ok;
    true ->
        timer:sleep(10),
        wait_to_die(Pid)
    end.





%EOF
