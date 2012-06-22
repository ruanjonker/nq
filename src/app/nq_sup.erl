-module(nq_sup).
-behaviour(supervisor).

-export([
        start_link/1,
        init/1
        ]).

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Args]).

init([_Args]) ->

    RestartSpec = {one_for_one, 1, 10},

    {ok, {BaseDir, CacheSizeMB, BufferSizeMB, PageSizeB, SyncIntervalMs}} = application:get_env(nq, consumer_cache_cfg),

    SyncFun = fun(_BdbName, SynTimeMs) ->
        error_logger:info_msg("Consumer Cache SYNC completed in ~pms~n", [SynTimeMs])
    end,

    ConsCacheOptions = [{txn_enabled, true}, {db_type, btree}, {cache_size, trunc(CacheSizeMB * 1024 * 1024)}, {buffer_size, trunc(BufferSizeMB * 1024 * 1024)}, {page_size, PageSizeB}, {sync, SyncIntervalMs, SyncFun}],

    ConsumerCache = {consumer_cache,
                        {bdb_store, start_link, ["consumer_cache", BaseDir, ConsCacheOptions]},
                        permanent, 60000, worker, [bdb_store]},

    {ok, { RestartSpec, [
                        ConsumerCache
                        ] }}.


%EOF
