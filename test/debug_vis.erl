#!/usr/bin/env escript
-module(debug_vis).
-mode(compile).

parse_log(Foldername) ->
    NodeName = filename:basename(Foldername),
    Log = case disk_log:open([{name, antidote_lock_server}, {file, filename:join(Foldername, "antidote_lock_server.LOG")}]) of
        {ok, Log} -> Log;
        {repaired, Log, IR, IB} ->
            io:format("repaired log ~p ~p~n", [IR, IB]),
            Log
    end,
    LogEntries = read_log(Log, start),
    ok = disk_log:close(Log),
    [{Time, NodeName, Data} || {Time, Data} <- LogEntries].

read_log(Log, Cont) ->
    case disk_log:chunk(Log, Cont, infinity) of
        {Cont2, Terms} ->
            Terms ++ read_log(Log, Cont2);
        eof ->
            []
    end.

print_systemtime(Ms) when is_integer(Ms) ->
    {{_Year, _Month, _Day}, {Hour, Minute, Second}} = calendar:system_time_to_local_time(Ms, millisecond),
    lists:flatten(io_lib:format("~p:~p:~p.~p", [Hour, Minute, Second, Ms rem 1000]));
print_systemtime(Other) -> Other.

main(Args) ->
    BasePath = "../logs",
    {ok, Filenames} = list_subfolders(BasePath),
    LatestFolder = lists:last(lists:sort(Filenames)),
    io:format("Visualizing folder: ~p~n", [LatestFolder]),
    {ok, Sub1} = list_subfolders(filename:join(LatestFolder, "test.multidc.lock_mgr_SUITE.logs")),
    {ok, Sub2} = list_subfolders(filename:join(Sub1, "log_private")),

    Logs = lists:flatmap(fun(F) -> parse_log(F) end, Sub2),

    SortedLogs = [{print_systemtime(T), N, D} || {T, N, D} <- lists:sort(fun({T1,_,_}, {T2,_,_}) -> T1 =< T2 end, Logs)],

%%    io:format("Logs = ~p~n", [Logs]),

    Html = render_log(SortedLogs),
    {ok, HF} = file:open(filename:join(BasePath, "vis.html"), [write]),
    ok = file:write(HF, Html),
    ok = file:close(HF),


    io:format("Done~n").


-record(acc, {
    state = initial_state,
    crdt = #{}
}).

render_log(LogEntries) ->
    Nodes = lists:usort([N || {_, N, _} <- LogEntries]),
    [
        "<table>",
        "<tr>",
        [["<th>", N, "</th>"] || N <- Nodes],
        "</tr>\n",
        render_log2(LogEntries, [{N, #acc{}} || N <- Nodes]),
        "</table>"
    ].

render_log2([], Accs) ->
    render_acc(Accs);
render_log2([{Time, Node, What} | Rest], Accs) ->
    case What of
        {event, Event, Data} ->
            render_cell(Accs, Time, Node, ["<p>Event ", render(Event), ":</p>", render(Data)])
            ++ render_log2(Rest, Accs);
        {actions, []} ->
            render_log2(Rest, Accs);
        {actions, Actions} ->
            render_cell(Accs, Time, Node, ["<p>Actions</p>", render(Actions)])
            ++ render_log2(Rest, Accs);
        {lock_entries, Entries} ->
            NewAccs = orddict:update(Node, fun(Acc) ->
                Acc#acc{crdt = maps:merge(Acc#acc.crdt, Entries)} end,
                Accs),
            render_acc(NewAccs) ++ render_log2(Rest, NewAccs);
        {state, State} ->
            NewAccs = orddict:update(Node, fun(Acc) ->
                Acc#acc{state = State} end,
                Accs),
            render_acc(NewAccs) ++ render_log2(Rest, NewAccs)
    end.

render_acc(Accs) ->
    [
        "<tr style='background: #eef'>",
        [["<td><pre>", render(Acc#acc.crdt), "</pre></td>"] || {N, Acc} <- Accs],
        "</tr>\n"
        "<tr style='background: #efe'>",
        [["<td><pre>", render(Acc#acc.state), "</pre></td>"] || {N, Acc} <- Accs],
        "</tr>\n"
    ].

render(T) ->
    io_lib:format("~p", [T]).

render_cell(Accs, Time, Node, Content) ->
    [
        "<tr>",
        ["<td></td>" || {N, _} <- Accs, N < Node],
        "<td><strong>", Time, "</strong><pre>",
        Content,
        "</pre></td>",
        ["<td></td>" || {N, _} <- Accs, N > Node],
        "</tr>\n"
    ].


list_subfolders(P) ->
    case file:list_dir(P) of
        {ok, Filenames} ->
            Files = [filename:join(P, F) || F <- Filenames],
            {ok, [F || F <- Files, filelib:is_dir(F)]};
        Err ->
            Err
    end.
