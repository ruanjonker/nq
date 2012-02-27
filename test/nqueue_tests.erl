-module(nqueue_tests).

-include_lib("eunit/include/eunit.hrl").

setup_test() -> 

    error_logger:tty(false),

    os:cmd("rm -fr ./nq_unit_test_data/"),

    ?assertEqual(ok, filelib:ensure_dir("./nq_unit_test_data/")),

    ?assertEqual(ok, application:load(nq)),

    ?assertEqual(ok, application:set_env(nq, base_dir, "./nq_unit_test_data/")),

    ?assertEqual(ok, application:set_env(nq, max_frag_size, 128)),

    ?assertEqual(ok, application:set_env(nq, sync_interval_ms, 5000)),

    ?assertEqual(ok, application:start(nq)),

    ?assertEqual(ok, filelib:ensure_dir("./nq_unit_test_data/")).


start_link_test() ->

    {ok, Pid} = nqueue:start_link("test"),

    ?assert(is_pid(Pid)),

    ?assertEqual(Pid, global:whereis_name({nqueue, "test"})).

enq_test() ->

    ?assertEqual(ok, nqueue:enq("test", "12345678")).

deq_test() ->
    
    ?assertEqual({ok, "12345678"},  nqueue:deq("test")).

stop_test() ->

    ?assertEqual(ok, nqueue:stop("test")).


buffer_test() ->

    ?assertEqual(ok, application:set_env(nq, max_frag_size, 128)),

    os:cmd("rm -fr ./nq_unit_test_data/"),

    ?assertEqual(ok, filelib:ensure_dir("./nq_unit_test_data/")),

    {ok, _} = nqueue:start_link("test"),

    [ ?assertEqual(ok, nqueue:enq("test", L)) || L <- lists:seq(1, 100)],

    [ ?assertEqual({ok, L}, nqueue:deq("test")) || L <- lists:seq(1, 100)],

    [ ?assertEqual(ok, nqueue:enq("test", L)) || L <- lists:seq(1, 1000)],

    [ ?assertEqual({ok, L}, nqueue:deq("test")) || L <- lists:seq(1, 1000)],

    [ ?assertEqual(ok, nqueue:enq("test", L)) || L <- lists:seq(1, 10000)],

    [ ?assertEqual({ok, L}, nqueue:deq("test")) || L <- lists:seq(1, 10000)],


    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 64)],

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 64)],

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 32)],

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 32)],

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 3)],

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 3)],

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 512)],

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 512)],

    ?assertEqual(ok, nqueue:stop("test")).

buffer2_test() ->

    ?assertEqual(ok, application:set_env(nq, max_frag_size, 128)),

    os:cmd("rm -fr ./nq_unit_test_data/"),

    ?assertEqual(ok, filelib:ensure_dir("./nq_unit_test_data/")),

    {ok, _} = nqueue:start_link("test"),

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 16)],

    ?assertEqual(ok, nqueue:stop("test")),

    {ok, _} = nqueue:start_link("test"),

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 16)],

    ?assertEqual(ok, nqueue:stop("test")),


    os:cmd("rm -fr ./nq_unit_test_data/"),

    ?assertEqual(ok, filelib:ensure_dir("./nq_unit_test_data/")),

    {ok, _} = nqueue:start_link("test"),

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 18)],

    ?assertEqual(ok, nqueue:enq("test", <<"**">>)),

    ?assertEqual(ok, nqueue:stop("test")),

    {ok, _} = nqueue:start_link("test"),

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 18)],

    ?assertEqual({ok, <<"**">>},  nqueue:deq("test")),

    ?assertEqual(ok, nqueue:stop("test")),

    os:cmd("rm -fr ./nq_unit_test_data/"),

    ?assertEqual(ok, filelib:ensure_dir("./nq_unit_test_data/")),

    {ok, _} = nqueue:start_link("test"),

    [ ?assertEqual(ok, nqueue:enq("test", <<"12">>)) || _ <- lists:seq(1, 1)],

    ?assertEqual(ok, nqueue:enq("test", <<"**">>)),

    ?assertEqual(ok, nqueue:stop("test")),

    {ok, _} = nqueue:start_link("test"),

    [ ?assertEqual({ok, <<"12">>}, nqueue:deq("test")) || _ <- lists:seq(1, 1)],

    ?assertEqual({ok, <<"**">>},  nqueue:deq("test")),

    ?assertEqual(ok, nqueue:stop("test")).

benchmark1_test() ->

    ?assertEqual(ok, application:set_env(nq, max_frag_size, 128000)),

    os:cmd("rm -fr ./nq_unit_test_data/"),

    ?assertEqual(ok, filelib:ensure_dir("./nq_unit_test_data/")),

    {ok, _} = nqueue:start_link("test"),

    enqueue_many("test", {"username", "password", "27000000000", "499", "clientref", "123456789009876543211234567890123456", "Welcome this is a test message"}, 10000),

    ?assertEqual(ok, nqueue:sync("test")),

    dequeue_many("test", {"username", "password", "27000000000", "499", "clientref", "123456789009876543211234567890123456", "Welcome this is a test message"}, 10000),

    ?assertEqual({error, empty}, nqueue:deq("test")),

    ?assertEqual(ok, nqueue:sync("test")),

    ?assertEqual(ok, nqueue:stop("test")),

    {ok, _} = nqueue:start_link("test"),

    ?assertEqual({error, empty}, nqueue:deq("test")).

handle_test() ->

    ?assertEqual({noreply, state}, nqueue:handle_call(crap, dontcare, state)),
    ?assertEqual({noreply, state}, nqueue:handle_cast(crap, state)),
    ?assertEqual({noreply, state}, nqueue:handle_info(crap, state)),
    ?assertEqual({ok, state}, nqueue:code_change(dontcare, state, dontcare)),
    ?assertEqual(ok, nqueue:terminate(dontcare, dontcare)).



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
