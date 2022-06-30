-module(rig).
-include("rig.hrl").

-compile(inline).
-compile({inline_size, 512}).

-ignore_xref([
    {rig_index_utils, tid, 1}
]).

-export([
    all/1,
    read/2,
    read/3,
    read_v/2,
    read_v/3,
    version/1
]).

-export_type([version/0, value_version/0]).

-type version() :: ets:tid().
-type value_version() :: {value(), version()}.

%% public
-spec all(table()) ->
    {ok, [{key(), value()}]} | {error, unknown_table} .

all(Table) ->
    try
        MatchObject = ets:match_object(tid(Table), '_', 500),
        All = lists:append(rig_utils:match_all(MatchObject)),
        {ok, All}
    catch
        _:_ ->
            {error, unknown_table}
    end.

-spec read(table(), key()) ->
    {ok, value()} | {error, unknown_key | unknown_table}.

read(Table, Key) ->
    try ets:lookup(tid(Table), Key) of
        [{Key, Value}] ->
            {ok, Value};
        [] ->
            {error, unknown_key}
    catch
        _:_ ->
            {error, unknown_table}
    end.

-spec read_v(table(), key()) ->
    {ok, value_version()} | {error, {unknown_key, version()} | unknown_table}.

read_v(Table, Key) ->
    try
        Version = tid(Table),
        ets:lookup(Version, Key)
    of
        [{Key, Value}] ->
            {ok, {Value, Version}};
        [] ->
            {error, {unknown_key, Version}}
    catch
        _:_ ->
            {error, unknown_table}
    end.

-spec read(table(), key(), value()) ->
    {ok, value()} | {error, unknown_table}.

read(Table, Key, Default) ->
    case read(Table, Key) of
        {error, unknown_key} ->
            {ok, Default};
        X ->
            X
    end.

-spec read_v(table(), key(), value()) ->
    {ok, value_version()} | {error, unknown_table}.

read_v(Table, Key, Default) ->
    case read_v(Table, Key) of
        {error, {unknown_key, Version}} ->
            {ok, {Default, Version}};
        X ->
            X
    end.

-spec version(table()) ->
    {ok, ets:tid()} | {error, unknown_table}.

version(Table) ->
    try
        {ok, tid(Table)}
    catch
        _:_ ->
            {error, unknown_table}
    end.

%% private
tid(Table) ->
    ets:lookup_element(?ETS_TABLE_INDEX, Table, 2).
