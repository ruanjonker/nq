-module(nq_app_tests).

-include_lib("eunit/include/eunit.hrl").

start_test() -> 

    error_logger:tty(false),

    ?assertEqual(ok, application:start(nq)),

    ?assertEqual({ok, 512000}, application:get_env(nq, max_frag_size)),

    ?assertEqual({ok, 5000}, application:get_env(nq, sync_interval_ms)),

    ?assertEqual({ok, {"./nqdata/nq_consumer_cache/", 16, 1, 4096, 5000}}, application:get_env(nq, consumer_cache_cfg)),

    ?assertEqual(ok, application:stop(nq)),

    ?assertEqual(ok, application:unload(nq)).
    
%EOF
