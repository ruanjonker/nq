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

    {ok, { RestartSpec, [
                        ] }}.


%EOF
