-module(nq_create_schema).

-ifdef('TEST').
-compile(export_all).
-endif.


-export([
        start/0
        ]).

start()->

    mnesia:stop(),

    mnesia:create_schema([node()]),

    init:stop().

%EOF
