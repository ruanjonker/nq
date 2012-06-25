-module(nq_queue_storage_tests).

-include_lib("eunit/include/eunit.hrl").

behaviour_info_test() ->

    Exports =     [
        {init,2},
        {read_frag,3},
        {write_frag,4},
        {write_frag,8},
        {trash_frag,3},
        {frag_size,3},
        {read_meta,2},
        {write_meta,6}

    ],


    ?assertEqual(Exports, nq_queue_storage:behaviour_info(callbacks)),

    ?assertEqual(undefined, nq_queue_storage:behaviour_info(anything_else)),

    ok.

