-module(test_config).

-export([
    dummy/1,
    duplicate/1,
    merge_duplicates/3
]).

dummy(_Bin) ->
    {domain, 5, <<"foobar.com">>}.

duplicate(Binary) ->
    {App, List} = binary_to_term(Binary),
    {string:casefold(App), List}.

merge_duplicates(_, Old, New) ->
    lists:umerge(Old, New).
