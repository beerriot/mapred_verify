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
%%      TODO: actually not checking 'integer', just =<
simple(_Type, Result, TotalEntries) ->
    [R] = Result,
    R =< TotalEntries.

%% @spec sorted(type(), [term()], integer()) -> boolean()
%% @doc Checks that Result is a list whose length is less than or
%%      equal to TotalEntries, and whose elements are sorted.
%%      TODO: only checking first two elements of list, not the whole thing
sorted(_Type, Result, TotalEntries) ->
    RS = length(Result),
    case RS == TotalEntries orelse TotalEntries > RS of
        true ->
            if RS > 0 ->
                    R1 = lists:nth(1, Result),
                    R2 = lists:nth(2, Result),
                    R1 < R2 orelse R1 == R2;
               true ->
                    true
            end;
        false ->
            false
    end.
