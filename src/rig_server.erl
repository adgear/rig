-module(rig_server).
-include("rig.hrl").

-compile(inline).
-compile({inline_size, 512}).

-export([
    start_link/0
]).

-behaviour(metal).
-export([
    init/3,
    handle_msg/2,
    terminate/2
]).

-record(state, {
    configs      :: [config()],
    reload_delay :: pos_integer(),
    timer_ref    :: undefined | reference(),
    tids         :: #{},
    timestamps   :: #{}
}).

%% public
-spec start_link() ->
    {ok, pid()}.

start_link() ->
    metal:start_link(?SERVER, ?SERVER, undefined).

%% metal callbacks
-spec init(atom(), pid(), term()) ->
    {ok, term()}.

init(_Name, _Parent, undefined) ->
    Configs = ?GET_ENV(configs, ?DEFAULT_CONFIGS),
    ReloadDelay = ?GET_ENV(reload_delay, ?DEFAULT_RELOAD_DELAY),
    Configs2 = configs_validate(Configs),

    {Configs3, Timestamps} = configs_changed(Configs2, #{}),
    Tids = configs_reload(Configs3, #{}),

    {ok, #state {
        configs = Configs2,
        reload_delay = ReloadDelay,
        tids = Tids,
        timer_ref = new_timer(ReloadDelay, ?MSG_RELOAD),
        timestamps = Timestamps
    }}.

-spec handle_msg(term(), term()) ->
    {ok, term()}.

handle_msg(?MSG_RELOAD, #state {
        reload_delay = ReloadDelay,
        timestamps = Timestamp,
        configs = Configs
    } = State) ->

    {Configs2, Timestamp2} = configs_changed(Configs, Timestamp),
    [?SERVER ! {?MSG_RELOAD_CONFIG, Config} || Config <- Configs2],

    {ok, State#state {
        timestamps = Timestamp2,
        timer_ref = new_timer(ReloadDelay, ?MSG_RELOAD)
    }};
handle_msg({?MSG_RELOAD_CONFIG, {Name, _, _, _} = Config}, #state {
        tids = Tids
    } = State) ->

    {Current, New, Tids2} = new_table(Name, Tids),
    async_reload(Config, Current, New),

    {ok, State#state {
        tids = Tids2
    }}.

-spec terminate(term(), term()) ->
    ok.

terminate(_Reason, _State) ->
    ok.

%% private
async_reload(Config, Current, New) ->
    spawn(fun () ->
        reload(Config, Current, New)
    end).

cleanup_table(undefined) ->
    ok;
cleanup_table(Tid) ->
    case ets:lookup(?ETS_TABLE_LOCKS, Tid) of
        [{_, Cnt}] when is_integer(Cnt), Cnt > 0 ->
            ok;
        _ ->
            % this may compete with rig:unlock/1,
            % tid may be not part of ?ETS_TABLE_LOCKS already
            % so catch potential badarg caused by an attempt
            % do delete the table which is already deleted...
            catch ets:delete(Tid)
    end.

configs_changed(Configs, Files) ->
    configs_changed(Configs, Files, []).

configs_changed([], Timestamps, Acc) ->
    {lists:reverse(Acc), Timestamps};
configs_changed([{Name, Filename, _, _} = Index | T], Timestamps, Acc) ->
    case rig_utils:change_time(Filename) of
        undefined ->
            configs_changed(T, Timestamps, Acc);
        Timestamp ->
            case maps:get(Name, Timestamps, 0) of
                X when X >= Timestamp ->
                    configs_changed(T, Timestamps, Acc);
                _ ->
                    Files2 = maps:put(Name, Timestamp, Timestamps),
                    configs_changed(T, Files2, [Index | Acc])
            end
    end.

configs_reload([], Tids2) ->
    Tids2;
configs_reload([{Name, _, _, _} = Config | T], Tids) ->
    {Current, New, Tids2} = new_table(Name, Tids),
    reload(Config, Current, New),
    configs_reload(T, Tids2).

configs_validate(Configs) ->
    configs_validate(Configs, []).

configs_validate([], Acc) ->
    lists:flatten(lists:reverse(Acc));
configs_validate([{BaseDir, Configs} | T], Acc) ->
    Configs2 = [
        {Table, BaseDir ++ Filename, DecoderFun, options_validate(Options)}
        || {Table, Filename, DecoderFun, Options} <- Configs
    ],
    configs_validate(T, [configs_validate(Configs2) | Acc]);
configs_validate([{Table, Filename, Decoder, Options} | T], Acc) ->
    case to_decoder(Decoder) of
        {ok, DecoderFun} ->
            configs_validate(
                T,
                [{Table, Filename, DecoderFun, options_validate(Options)} | Acc]
            );
        {error, invalid_fun} ->
            configs_validate(T, Acc)
    end.

to_decoder(term) ->
    {ok, fun erlang:binary_to_term/1};
to_decoder({Module, Function}) ->
    {ok, fun Module:Function/1};
to_decoder(Function) when is_function(Function, 1) ->
    {ok, Function};
to_decoder(Decoder) ->
    rig_utils:parse_fun(Decoder).

options_validate(Options) ->
    options_validate(Options, []).

options_validate([], Acc) ->
    Acc;
options_validate([{key_element, N} | R], Acc) when is_integer(N), N > 0 ->
    options_validate(R, [{key_element, N} | Acc]);
options_validate([{subscribers, Subscribers} | R], Acc)
  when is_list(Subscribers) ->
    options_validate(R, [{subscribers, Subscribers} | Acc]);
options_validate([{merger, {Module, Function}} | R], Acc) ->
    options_validate(R, [{merger, fun Module:Function/3} | Acc]);
options_validate([{merger, Function} | R], Acc) when is_function(Function, 3) ->
    options_validate(R, [{merger, Function} | Acc]);
options_validate([{Name, Value} | R], Acc) ->
    error_logger:warning_msg(
        "invalid option '~p' with value '~p'",
        [Name, Value]
    ),
    options_validate(R, Acc).

new_table(Name, Tids) ->
    New = ets:new(table, [public, {read_concurrency, true}]),
    {Current, Generations} = case maps:get(Name, Tids, []) of
        [] ->
            {undefined, [New]};
        [T1] ->
            {undefined, [T1, New]};
        [T1, T2] ->
            {T1, [T2, New]}
    end,
    Tids2 = maps:put(Name, Generations, Tids),
    {Current, New, Tids2}.

new_timer(Delay, Msg) ->
    new_timer(Delay, Msg, self()).

new_timer(Delay, Msg, Pid) ->
    erlang:send_after(Delay, Pid, Msg).

reload({Name, Filename, DecoderFun, Opts}, Current, New) ->
    try
        Timestamp = os:timestamp(),
        {ok, File} = file:open(Filename, [binary, read]),
        KeyElement = ?LOOKUP(key_element, Opts, ?DEFAULT_KEY_ELEMENT),
        Merge = ?LOOKUP(merger, Opts, undefined),
        ok = rig_utils:read_file(File, DecoderFun, New, KeyElement, Merge),
        ok = file:close(File),
        ok = rig_index:add(Name, New),
        Subscribers = ?LOOKUP(subscribers, Opts, ?DEFAULT_SUBSCRIBERS),
        [Pid ! {rig_index, update, Name} || Pid <- Subscribers],
        rig_events:publish(Name, {rig_index, update, Name}),
        cleanup_table(Current),
        Diff = timer:now_diff(os:timestamp(), Timestamp) div 1000,
        error_logger:info_msg("~p config reloaded in ~p ms", [Name, Diff])
    catch
        ?EXCEPTION(E, R, Stacktrace) ->
            error_logger:error_msg("error loading ~p: ~p:~p~n~p~n",
                [Name, E, R, ?GET_STACK(Stacktrace)])
    end.
