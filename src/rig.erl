-module(rig).
-include("rig.hrl").

-compile(inline).
-compile({inline_size, 512}).

-ignore_xref([
    {rig_index_utils, tid, 1}
]).

-export([
    all/1,
    lock/1, unlock/1, locks/0,
    read/2, read/3,
    read_v/2, read_v/3,
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

% lock current version of the table
-spec lock(table()) -> {ok, ets:tid()} | {error, Error :: _}.

lock(Table) ->
    try
        Tid = tid(Table),
        ets:update_counter(?ETS_TABLE_LOCKS, Tid, 1, {x, 0}),
        {ok, Tid}
    catch
        _:_ ->
            {error, unknown_table}
    end.

% unlock locked table version
-spec unlock(ets:tid()) -> ok | {error, Error :: _}.

unlock(Tid) ->
    try
        case ets:update_counter(?ETS_TABLE_LOCKS, Tid, {2, -1, 0, 0}, {x, 0}) of
            0 ->
                ets:delete(?ETS_TABLE_LOCKS, Tid),
                ets:delete(Tid),
                ok;
            _ ->
                ok
        end
    catch
        _:_ ->
            {error, bad_tid}
    end.

% list all locks with usage counters
-spec locks() -> [{ets:tid(), pos_integer()}].

locks() ->
    ets:tab2list(?ETS_TABLE_LOCKS).

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
