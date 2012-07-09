-module(nq_queue_storage).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->

    [
        {init,2},
        {read_frag,3},
        {write_frag,4},
        {write_frag,8},
        {trash_frag,3},
        {frag_size,3},
        {read_meta,2},
        {write_meta,6}

    ];

behaviour_info(_) ->
    undefined.

