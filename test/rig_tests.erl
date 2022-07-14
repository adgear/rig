-module(rig_tests).
-include("rig.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DECODER, "fun erlang:binary_to_term/1.").

service_tabs() ->
    [
        ?ETS_TABLE_INDEX,
        ?ETS_TABLE_LOCKS,
        gproc,
        gproc_monitor
    ].

rig_test() ->
    error_logger:tty(false),
    register(rig_test, self()),
    Count = length(ets:all()),

    {error, unknown_table} = rig:read(domains, 1),
    {error, unknown_table} = rig:read(domains, 1, undefined),

    application:load(?APP),
    application:set_env(?APP, configs, [
        % table #1 - creatives
        {creatives, "./test/files/creatives.bert", term, []},
        {"./test/files/", [
            % table #2 - domains
            {domains, "domains.bert", term, [{key_element, 2}]},
            % table #3 - dummy
            {dummy, "domains.bert", {test_config, dummy}, [{key_element, 3}]},
            % no table created here
            {invalid_fun, "invalid.bert", "my_fun:decode/1.", []}
        ]},
        % no table created here
        {invalid_file, "", ?DECODER, []},
        % table #4 - users
        {users, "./test/files/users.bert", ?DECODER, [{key_element, 2},
            {subscribers, [rig_test]}]}
    ]),
    TableCount = 4,

    encode_bert_configs(),
    {ok, _} = rig_app:start(),

    receive {rig_index, update, users} ->
        ok
    end,
    Count = length(ets:all() -- service_tabs()) - TableCount,

    {ok, {domain, 1 , <<"adgear.com">>}} = rig:read(domains, 1),
    {ok, {domain, 1 , <<"adgear.com">>}} = rig:read(domains, 1, undefined),
    {ok, {domain, 5 , <<"foobar.com">>}} = rig:read(dummy, <<"foobar.com">>),

    {error, unknown_key} = rig:read(domains, 6),
    {ok, undefined} = rig:read(domains, 6, undefined),

    {ok, {user, 5, super_admin, <<"hello3">>}} = rig:read(users, 5),
    {ok, [
        {1, {user, 1, lpgauth, <<"hello">>}},
        {3, {user, 3, root, <<"hello2">>}},
        {5, {user, 5, super_admin, <<"hello3">>}}
    ]} = rig:all(users),
    {error, unknown_table} = rig:all(invalid),

    {ok, _Tid} = rig:version(creatives),
    {error, unknown_table} = rig:version(invalid),

    % service tables created in supervisor's init, so application stop
    % does not do any cleanup, so all the service tables left alive...
    rig_app:stop(),
    Count = length(ets:all() -- service_tabs()).

lock_test() ->
    error_logger:tty(true),
    application:load(?APP),

    {Table, Config, UpfateFun, WaitForAckFun} = init_table_config(),
    application:set_env(?APP, configs, [Config]),
    encode_bert_configs(),

    Tabs_0 = ets:all(),
    {ok, _} = rig_app:start(),

    % wait for initial data to be loaded
    ok = WaitForAckFun(),

    % initial content loaded
    {ok, L1} = rig:all(Table),

    % lock current version
    {ok, T1} = rig:lock(Table),

    % locked version matches initial data
    L1 = ets:tab2list(T1),

    % perform 10 random updates
    repeat(10, UpfateFun),

    % fetch most recent data
    {ok, L2} = rig:all(Table),

    % locked version still matches initial data
    L1 = ets:tab2list(T1),

    % recent content should not match initial data
    false = L1 == L2,

    % 1 locked table + 2 generations are left
    3 = length((ets:all() -- Tabs_0) -- service_tabs()),
    rig:unlock(T1),

    % 2 generations are left
    2 = length((ets:all() -- Tabs_0) -- service_tabs()),

    ok = rig_app:stop(),

    % all service and data tabs deleted with the app stopped
    0 = length((ets:all() -- Tabs_0)),

    ok.

%% private
repeat(0, _) -> ok;
repeat(N, F) -> F(), repeat(N - 1, F).

init_table_config() ->
    DecoderFun = fun (_) -> {rand:uniform(), rand:uniform()} end,
    Options = [{subscribers, [self()]}],
    Config = {users, "./test/files/users.bert", DecoderFun, Options},
    WaitForAckFun = fun () ->
        receive
            {rig_index, update, users} ->
                ok
            after 1000 ->
                {error, timeout}
        end
    end,
    UpfateFun = fun () ->
        ?SERVER ! {?MSG_RELOAD_CONFIG, Config},
        ok = WaitForAckFun()
    end,
    {users, Config, UpfateFun, WaitForAckFun}.

encode_bert_configs() ->
    [to_bert(Filename) || Filename <- filelib:wildcard("./test/files/*.term")].

to_bert(Filename) ->
    Rootname = filename:rootname(Filename),
    New = Rootname ++ ".bert",
    {ok, File} = file:open(New, [binary, raw, write]),
    {ok, Term} = file:consult(Filename),
    file_write(Term, File),
    file:close(File).

file_write([], _File) ->
    ok;
file_write([Line | T], File) ->
    Line2 = term_to_binary(Line),
    Size = rig_utils:encode_varint(size(Line2)),
    ok = file:write(File, [Size, Line2]),
    file_write(T, File).
