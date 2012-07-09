-module(nq_util).

-export([
        db_set/3,
        db_get/2,
        db_del/2
        ]).

-type db() :: string().
-type key() :: any().
-type value() :: any().

%% @doc Wrapper for bdb_store:set
%%
-spec db_set(db(), key(), value()) -> ok.
db_set(Db, Key, Val) ->
    ok = bdb_store:set(Db, term_to_binary(Key), term_to_binary(Val)).

%% @doc Wrapper for bdb_store:get
%%
-spec db_get(db(), key()) -> {ok, value()} | {error, not_found}.
db_get(Db, Key) ->

    case bdb_store:get(Db, term_to_binary(Key)) of
    {ok, LData} ->
        {ok, binary_to_term(list_to_binary(LData))};

    {error, not_found} ->
        {error, not_found}
    end.

%% @doc Wrapper for bdb_store:del
%%
-spec db_del(db(), key()) -> ok.
db_del(Db, Key) ->
    ok = bdb_store:del(Db, term_to_binary(Key)).

