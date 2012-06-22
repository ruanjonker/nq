-module(nq_util).

-export([
        db_set/3,
        db_get/2,
        db_del/2
        ]).



db_set(Db, Key, Val) ->
    ok = bdb_store:set(Db, term_to_binary(Key), term_to_binary(Val)).


db_get(Db, Key) ->

    case bdb_store:get(Db, term_to_binary(Key)) of
    {ok, LData} ->
        {ok, binary_to_term(list_to_binary(LData))};

    Error ->
        Error
    end.


db_del(Db, Key) ->
    ok = bdb_store:del(Db, term_to_binary(Key)).

