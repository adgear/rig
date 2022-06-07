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
    gen_server:call(?MODULE, {fetch, Name}).

init([]) ->
    {ok, #state{}}.

handle_call({fetch, Name}, _From, State) ->
    Reply =
        case maps:find(Name, State#state.cache) of
            error -> ok;
            {ok, Value} -> {ok, Value}
        end,
    {reply, Reply, State}.

handle_cast({save, Name, Event}, State = #state{cache = Cache}) ->
    WithNewEvent = Cache#{Name => Event},
    {noreply, State#state{cache = WithNewEvent}}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
