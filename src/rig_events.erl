-module(rig_events).

%% API
-export([
    publish/2,
    subscribe/1
]).

-define(TOPIC(Name), {p, l, {?MODULE, Name}}).

-spec publish(Name :: atom(), Data :: term()) -> ok.
publish(Name, Data) ->
    gproc:send(?TOPIC(Name), Data).

-spec subscribe(Name :: atom()) -> ok.
subscribe(Name) ->
    gproc:reg(?TOPIC(Name)).
