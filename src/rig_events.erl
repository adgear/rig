-module(rig_events).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    publish/2,
    subscribe/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(TOPIC(Name), {p, l, {?MODULE, Name}}).

-record(state, {cache = #{} :: map()}).
-type state() :: #state{}.

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec publish(Name :: atom(), Event :: term()) -> ok.
publish(Name, Event) ->
    gproc:send(?TOPIC(Name), Event),
    gen_server:cast(?MODULE, {save, Name, Event}).

-spec subscribe(Name :: atom()) -> ok | {ok, MostRecentEvent :: term()}.
subscribe(Name) ->
    gproc:reg(?TOPIC(Name)),
    try
        gen_server:call(?MODULE, {fetch, Name})
    catch
        _:_:_ ->
            ok
    end.

-spec init([]) -> {ok, state()}.
init([]) ->
    {ok, #state{}}.

-spec handle_call({fetch, atom()}, {pid(), _}, state()) ->
    {reply, term(), state()}.
handle_call({fetch, Name}, _From, State) ->
    Reply =
        case maps:find(Name, State#state.cache) of
            error -> ok;
            {ok, Value} -> {ok, Value}
        end,
    {reply, Reply, State}.

-spec handle_cast({save, atom(), term()}, state()) -> {noreply, state()}.
handle_cast({save, Name, Event}, State = #state{cache = Cache}) ->
    WithNewEvent = Cache#{Name => Event},
    {noreply, State#state{cache = WithNewEvent}}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.
