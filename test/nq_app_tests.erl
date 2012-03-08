-module(nq_app_tests).

-include_lib("eunit/include/eunit.hrl").

start_test() -> 

    error_logger:tty(false),

    ?assertEqual(ok, application:start(nq)),

    ?assertEqual({ok, 64000}, application:get_env(nq, max_frag_size)),

    ?assertEqual({ok, 5000}, application:get_env(nq, sync_interval_ms)),

    ?assertEqual({ok, 1000}, application:get_env(nq, subs_notification_sleep_ms)),

    ?assertEqual(ok, application:stop(nq)),

    ?assertEqual(ok, application:unload(nq)).
    
%EOF
