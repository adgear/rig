%% -*- erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 et
-module(rig_persist).

% http://erlang.org/doc/design_principles/gen_server_concepts.html
-behaviour(gen_server).

-include("rig.hrl").

% API
-export([start_link/0]).
% Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

% Callbacks
-spec init([]) -> {ok, term()}.
init(_Args) ->
    {ok, #{}}.

-spec handle_call(term(), pid(), term()) -> {term(), term(), term()}.
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

-spec handle_cast(term(), term()) -> {term(), term()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), term()) -> term().
handle_info({rig_index, update, Table}, State) ->
    Now = erlang:system_time(millisecond),
    {ok, Records} = rig:all(Table),
    SplitLists = rig_persist_utils:divide(Records, 500),
    F1 = fun get_candidates/2,
    WhichLists = mapreduce:mapreduce(F1, [], [], SplitLists),
    rig_persist_utils:to_persistent_term(WhichLists),
    Then = erlang:system_time(millisecond),
    luger:info("rb_persist", "Records found in milliSECONDS:~p~n", [Then - Now]),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), term()) -> term().
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), term(), term()) -> {ok, term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%Internal functions%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_candidates(Parent, Records) ->
    Recs = rig_persist_utils:find_records(Records),
    Parent ! {1, [Rec || {true, Rec} <- Recs]}.
