-module(prop_antidote_list_utils).
-include("antidote.hrl").
-include_lib("proper/include/proper.hrl").


prop_test() ->
    ?FORALL(List, list(set()),
        begin
            Sorted = antidote_list_utils:topsort(fun cmp/2, List),
            conjunction([
                {is_sorted, is_sorted(fun cmp/2, Sorted)},
                {same_lements, lists:sort(List) == lists:sort(Sorted)}
            ])
        end).

is_sorted(_Cmp, []) -> true;
is_sorted(Cmp, [X|Xs]) -> lists:all(fun(Y) -> not Cmp(Y, X) end, Xs) andalso is_sorted(Cmp, Xs).

cmp(X, Y) ->
    X /= Y andalso ordsets:is_subset(X, Y).


set() ->
    N = 5,
    Seq = lists:seq(1, N),
    ?LET(L, tuple([boolean() || _ <- Seq]), [I || {I, true} <- lists:zip(Seq, tuple_to_list(L))]).
