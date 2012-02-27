-module(nq_build_rel).

-ifdef('TEST').
-compile(export_all).
-endif.


-export([
        start/1
        ]).

start(RelFile)->
    ok = systools:make_script(RelFile),
    ok = systools:make_tar(RelFile, [{dirs, [config, include, src, priv, doc, tests, scripts]}]),
    init:stop().

%EOF
