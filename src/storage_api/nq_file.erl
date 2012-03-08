-module(nq_file).

-export([
        init/2,
        read_frag/3,
        write_frag/4,
        write_frag/7,
        trash_frag/3,
        frag_size/3,
        read_meta/2,
        write_meta/5
        ]).

init(_QName, BaseDir) ->

    ok = filelib:ensure_dir(BaseDir ++ "/"),

    {ok, BaseDir}.

read_frag(QName, FragIdx, BaseDir) ->

    Filename = BaseDir ++ QName ++ ".frag." ++ integer_to_list(FragIdx),

    case file:read_file(Filename) of
    {ok, Data} ->
        {ok, Data};

    {error, enoent} ->
        {ok, <<>>};

    Error ->
        Error

    end.

write_frag(QName, FragIdx, Data, BaseDir) ->

    Filename = BaseDir ++ QName ++ ".frag." ++ integer_to_list(FragIdx),

    case file:write_file(Filename, Data, [raw, binary]) of
    ok ->
        ok;

    Error ->
        Error
    end.

write_frag(QName, FragIdx, Data, RFragIdx, RFragRecNo, WFragIdx, BaseDir) ->

    case write_frag(QName, FragIdx, Data, BaseDir) of
    ok ->

        write_meta(QName, RFragIdx, RFragRecNo, WFragIdx, BaseDir);

    Error ->
        Error
    end.

frag_size(QName, FragIdx, BaseDir) ->

    Filename = BaseDir ++ QName ++ ".frag." ++ integer_to_list(FragIdx),

    filelib:file_size(Filename).

trash_frag(QName, FragIdx, BaseDir) ->

    Filename = BaseDir ++ QName ++ ".frag." ++ integer_to_list(FragIdx),

    case file:delete(Filename) of
    ok ->
        ok;

    {error,enoent} ->
        ok;

    Error ->
        Error
    end.

read_meta(QName, BaseDir) ->

    Filename = BaseDir ++ QName ++ ".meta",

    case file:read_file(Filename) of
    {ok, <<RFragIdx:64/big-unsigned-integer,RFragRecNo:64/big-unsigned-integer,WFragIdx:64/big-unsigned-integer>>} ->
        {ok, {RFragIdx, RFragRecNo, WFragIdx}};

    {error, enoent} ->
        {ok, {0,0,0}};

    Error ->
        Error
    end.

write_meta(QName, RFragIdx, RFragRecNo, WFragIdx, BaseDir) ->

    Filename = BaseDir ++ QName ++ ".meta",

    file:rename(Filename, Filename ++ ".prev"),

    case file:write_file(Filename ++ ".tmp", <<RFragIdx:64/big-unsigned-integer,RFragRecNo:64/big-unsigned-integer,WFragIdx:64/big-unsigned-integer>>, [raw, binary, write]) of
    ok ->
        file:rename(Filename ++ ".tmp", Filename);

    Error ->

        %Try restoring previous file ...
        file:rename(Filename ++ ".prev", Filename),

        Error

    end.



