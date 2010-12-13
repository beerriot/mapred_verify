-module(mapred_verifiers).

-compile([export_all]).

map(_Type, Result, TotalEntries) ->
    io:format("Got ~p, Expecting: ~p...", [length(Result), TotalEntries]),
    length(Result) == TotalEntries.

simple(_Type, Result, TotalEntries) ->
    [R] = Result,
    R =< TotalEntries.

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
