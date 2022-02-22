-module(mapreduce).

-export([mapreduce/4]).

-import(lists, [foreach/2]).

-ifdef(EUNIT).

-include_lib("eunit/include/eunit.hrl").

-endif.

%% F1(Pid, X) -> sends the List back to Pid

-spec mapreduce(fun(), fun(), term(), list()) -> list().
mapreduce(F1, F2, Acc0, L) ->
    S = self(),
    Pid = spawn(fun() -> reduce(S, F1, F2, Acc0, L) end),
    receive
        {Pid, Result} ->
            Result
    end.

reduce(Parent, F1, _F2, _Acc0, L) ->
    process_flag(trap_exit, true),
    ReducePid = self(),
    %% Create the Map processes
    %%   One for each element X in L
    foreach(fun(X) -> spawn_link(fun() -> do_job(ReducePid, F1, X) end) end, L),
    N = length(L),
    %% Wait for N Map processes to terminate
    L1 = collect_replies(N, []),
    Parent ! {self(), L1}.

%% collect_replies(N, Dict)
%%     collect and merge {Key, Value} messages from N processes.
%%     When N processes have terminated return a dictionary
%%     of {Key, [Value]} pairs

collect_replies(0, Recs) ->
    Recs;
collect_replies(N, Recs) ->
    receive
        {_Num, L} ->
            collect_replies(N, lists:append(L, Recs));
        {'EXIT', _, _Why} ->
            collect_replies(N - 1, Recs)
    end.

%% Call F(Pid, X)
%%   F must send the built List  to Pid
%%     and then terminate

do_job(ReducePid, F, X) ->
    F(ReducePid, X).

-ifdef(EUNIT).

map_test() ->
    Ls = rig_persist_utils:divide(records(), 2),
    F1 = fun get_candidates/2,
    L = mapreduce(F1, [], [], Ls),
    ?assertEqual(length(L), 6).

extract_record(Record) ->
    Match = lists:keyfind(nonsense, 1, Record),
    Found =
        case Match of
            false ->
                false;
            _ ->
                {_, Recs} = Match,
                Any = [lists:any(fun(RecId) ->
                                    lists:member(RecId, [<<"IAMBESTEST">>, <<"IAMTHEONE">>])
                                 end,
                                 RecIds)
                       || {_, RecIds} <- Recs],
                lists:member(true, Any)
        end,
    {Found, Record}.

find_records(Records) ->
    Fs = [extract_record(Record) || {_, Record} <- Records],
    {L1, _L2} = lists:partition(fun({A, _}) -> A == true end, Fs),
    L1.

get_candidates(Parent, Records) ->
    Recs = find_records(Records),
    Parent ! {1, [Rec || {true, Rec} <- Recs]}.

records() ->
    [{1,
      [{got_your_number, 1},
       {some_date, {{2022, 2, 21}, {18, 0, 0}}},
       {nonsense, [{6, [<<"IAMTHEONE">>]}]},
       {some_bul,
        <<"((((isthisok=true and (\"IAMTHEONE\" in nonsense)))) and (i_partied_in in (\"DE\",\"FR\")) and ((within_months of (\"1\", \"3\")))">>}]},
     {2,
      [{got_your_number, 2},
       {some_date, {{2022, 2, 21}, {18, 0, 0}}},
       {nonsense, [{6, [<<"THE">>]}]},
       {some_bul,
        <<"((((isthisok=true and (\"IAMTNOTHEONE\" in nonsense)))) and (i_partied_in in (\"DE\",\"FR\")) and ((within_months of (\"1\", \"3\")))">>}]},
     {3,
      [{got_your_number, 3},
       {some_date, {{2022, 2, 21}, {18, 0, 0}}},
       {nonsense, [{6, [<<"IAMTHEONE">>]}]},
       {some_bul,
        <<"((((isthisok=true and (\"THEONE\" in nonsense)))) and (i_partied_in in (\"DE\",\"FR\")) and ((within_months of (\"1\", \"3\")))">>}]},
     {4,
      [{got_your_number, 4},
       {some_date, {{2022, 2, 21}, {18, 0, 0}}},
       {nonsense, [{6, [<<"IAMTHEONE">>]}]},
       {some_bul,
        <<"((((isthisok=true and (\"IAMTHEONE\" in nonsense)))) and (i_partied_in in (\"DE\",\"FR\")) and ((within_months of (\"1\", \"3\")))">>}]},
     {5,
      [{got_your_number, 5},
       {some_date, {{2022, 2, 21}, {18, 0, 0}}},
       {nonsense, [{6, [<<"IAMBESTEST">>]}]},
       {some_bul,
        <<"((((isthisok=true and (\"IAMTHEONE\" in nonsense)))) and (i_partied_in in (\"DE\",\"FR\")) and ((within_months of (\"1\", \"3\")))">>}]},
     {6,
      [{got_your_number, 6},
       {some_date, {{2022, 2, 21}, {18, 0, 0}}},
       {nonsense, [{6, [<<"ONE">>]}]},
       {some_bul,
        <<"((((isthisok=true and (\"IAMTHEONE\" in nonsense)))) and (i_partied_in in (\"DE\",\"FR\")) and ((within_months of (\"1\", \"3\")))">>}]},
     {7,
      [{got_your_number, 7},
       {some_date, {{2022, 2, 21}, {18, 0, 0}}},
       {nonsense, [{6, [<<"IAMTHEONE">>]}]},
       {some_bul,
        <<"((((isthisok=true and (\"IAMTHEONE\" in nonsense)))) and (i_partied_in in (\"DE\",\"FR\")) and ((within_months of (\"1\", \"3\")))">>}]},
     {8,
      [{got_your_number, 8},
       {some_date, {{2022, 2, 21}, {18, 0, 0}}},
       {nonsense, [{6, [<<"YO">>, <<"HEY">>]}]},
       {some_bul,
        <<"((((isthisok=true and (\"IAMTHEONE\" in nonsense)))) and (i_partied_in in (\"DE\",\"FR\")) and ((within_months of (\"1\", \"3\")))">>}]},
     {9,
      [{got_your_number, 9},
       {some_date, {{2022, 2, 21}, {18, 0, 0}}},
       {nonsense, [{6, [<<"IAMTHEONE">>]}]},
       {some_bul,
        <<"((((isthisok=true and (\"IAMTHEONE\" in nonsense)))) and (i_partied_in in (\"DE\",\"FR\")) and ((within_months of (\"1\", \"3\")))">>}]}].

-endif.
