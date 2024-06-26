%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
-module(rig_persist).

% http://erlang.org/doc/design_principles/gen_server_concepts.html
-behaviour(gen_server).

-include("rig.hrl").

-ifdef(EUNIT).

-include_lib("eunit/include/eunit.hrl").

-endif.

% API
-export([start_link/0]).
% Callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {}).
-type state() :: #state{}.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Callbacks
-spec init([]) -> {ok, state()}.
init([]) ->
    case application:get_env(?APP, persist_topic) of
        {ok, Topic} ->
            case rig_events:subscribe(Topic) of
                {ok, Event} ->
                    self() ! Event;
                ok ->
                    ok
            end;
        _ ->
            ok
    end,
    error_logger:info_msg("rig_persist reporting to work"),
    {ok, #state{}}.

-spec handle_call(_, {pid(), _}, state()) -> {reply, ignored, state()}.
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

-spec handle_cast(_, state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(_, state()) -> {noreply, state()}.
handle_info({rig_index, update, Table}, State) ->
    Now = erlang:system_time(millisecond),
    {ok, Records} = rig:all(Table),
    rig_persist_utils:to_persistent_term(Records),
    Then = erlang:system_time(millisecond),
    error_logger:info_msg("persisted in ~p millsecs", [Then - Now]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(_, state()) -> ok.
terminate(Reason, _State) ->
    error_logger:info_msg(
      "rig_persist terminating with Reason: ~p~n", [Reason]),
    ok.

-spec code_change(term(), term(), term()) -> {ok, term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
