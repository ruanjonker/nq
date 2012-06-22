-module(nq_app).
-behaviour(application).

-include("nq.hrl").

-export([
        start/2,
        prep_stop/1,
        stop/1
        ]).

start(_Type, Args) ->

    case application:get_env(nq, max_frag_size) of
    {ok, S} when (is_integer(S) and (S > 0)) ->
        ok;
    _ ->
        ok = application:set_env(nq, max_frag_size, 64000)
    end,

    case application:get_env(nq, sync_interval_ms) of
    {ok, SyncIntervalMs} when (is_integer(SyncIntervalMs) and (SyncIntervalMs > 0)) ->
        ok;
    _ ->
        ok = application:set_env(nq, sync_interval_ms, 5000)
    end,

    case application:get_env(nq, subs_notification_sleep_ms) of
    {ok, SubsNotificationSleepMs} when (is_integer(SubsNotificationSleepMs) and (SubsNotificationSleepMs > 0)) ->
        ok;
    _ ->
        ok = application:set_env(nq, subs_notification_sleep_ms, 1000)
    end,

    %{"./nqdata/consumer_cache/", 16, 1, 4096, 5000}
    case application:get_env(nq, consumer_cache_cfg) of
    {ok, {_, _, _, _, _}} -> 
        ok;
    _ ->
        ok = application:set_env(nq, consumer_cache_cfg, {"./nqdata/consumer_cache/", 16, 1, 4096, 5000})
    end,

    'nq_sup':start_link(Args).

prep_stop(State) ->
    State.

stop(_State)->
    ok.

%EOF
