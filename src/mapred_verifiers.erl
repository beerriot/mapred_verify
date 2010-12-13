-module(mapred_verifiers).

-compile([export_all]).

%% @type type() = bucket|entries

%% @spec map(type(), [term()], integer()) -> boolean()
%% @doc Checks that the Result is TotalEntries in length.
%%      Returns 'true' if so, false if not.
map(_Type, Result, TotalEntries) ->
    io:format("Got ~p, Expecting: ~p...", [length(Result), TotalEntries]),
    length(Result) == TotalEntries.

%% @spec simple(type(), [term()], integer()) -> boolean()
%% @doc Checks that Result is a single-element list, that element
%%      being an integer less than or equal to TotalEntries.
simple(_Type, Result, TotalEntries) ->
    [R] = Result,
    is_integer(R) andalso R =< TotalEntries.

%% @spec sorted(type(), [term()], integer()) -> boolean()
%% @doc Checks that Result is a list whose length is less than or
%%      equal to TotalEntries, and whose elements are sorted.
sorted(_Type, Result, TotalEntries) ->
    length(Result) =< TotalEntries andalso
        Result == lists:sort(Result).
