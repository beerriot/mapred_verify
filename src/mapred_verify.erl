-module(mapred_verify).

-export([main/1]).

-define(BUCKET, <<"mr_validate">>).

-define(ERLANG_JOB, [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, false},
                     {reduce, {modfun, riak_kv_mapreduce, reduce_string_to_integer}, none, false},
                     {reduce, {modfun, riak_kv_mapreduce, reduce_sum}, none, true}]).

-define(JS_JOB, [{map, {jsfun, <<"Riak.mapValuesJson">>}, none, false},
                 {reduce, {jsfun, <<"Riak.reduceSum">>}, none, true}]).

-define(MIXED_JOB1, [{map, {jsfun, <<"Riak.mapValuesJson">>}, none, false},
                     {reduce, {modfun, riak_kv_mapreduce, reduce_sum}, none, true}]).

-define(MIXED_JOB2, [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, false},
                     {reduce, {jsfun, <<"Riak.reduceSort">>}, none, true}]).

-define(MIXED_JOB3, [{map, {jsfun, <<"Riak.mapValuesJson">>}, none, false},
                     {reduce, {modfun, riak_kv_mapreduce, reduce_sort}, none, false},
                     {reduce, {jsfun, <<"Riak.reduceSum">>}, none, true}].

-define(MIXED_JOB4, [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, false},
                     {reduce, {jsanon, list_to_binary([<<"function(v) { values = Riak.filterNotFound(v);">>,
                                                       <<"return values.map(function(value) { if (typeof value === 'string') {">>,
                                                       <<" return JSON.parse(value); } else { return value; } });}">>])},
                      none, false},
                     {reduce, {modfun, riak_kv_mapreduce, reduce_sum}, none, true}]).

-define(MIXED_JOB5, [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, false},
                     {reduce, {jsanon, list_to_binary([<<"function(v) { values = Riak.filterNotFound(v);">>,
                                                       <<"return values.map(function(value) { if (typeof value === 'string') {">>,
                                                       <<" return JSON.parse(value); } else { return value; } });}">>])},
                      none, false},
                     {reduce, {jsfun, <<"Riak.reduceSum">>}, none, true}]).

-define(JS_MAP_JOB, [{map, {jsfun, <<"Riak.mapValuesJson">>}, none, true}]).
-define(ERLANG_MAP_JOB, [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, true}]).

-define(MAP_JOB_VERIFIER, fun(_Type, Result, TotalEntries) -> io:format("Got ~p, Expecting: ~p...", [length(Result), TotalEntries]),
                                                                        length(Result) == TotalEntries end).
-define(SIMPLE_JOB_VERIFIER, fun(_Type, Result, TotalEntries) -> [R] = Result, R =< TotalEntries end).
-define(SORTED_JOB_VERIFIER, fun(_Type, Result, TotalEntries) -> RS = length(Result),
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
                                                          end end).

-define(TESTS, [{"Erlang/Erlang", {?ERLANG_JOB, ?SIMPLE_JOB_VERIFIER}},
                {"JS/JS", {?JS_JOB, ?SIMPLE_JOB_VERIFIER}},
                {"JS/Erlang", {?MIXED_JOB1, ?SIMPLE_JOB_VERIFIER}},
                {"Erlang/JS", {?MIXED_JOB2, ?SORTED_JOB_VERIFIER}},
                {"JS/Erlang/JS", {?MIXED_JOB3, ?SIMPLE_JOB_VERIFIER}},
                {"Erlang/JS/Erlang", {?MIXED_JOB4, ?SIMPLE_JOB_VERIFIER}},
                {"Erlang/JS/JS", {?MIXED_JOB5, ?SIMPLE_JOB_VERIFIER}},
                {"Erlang map", {?ERLANG_MAP_JOB, ?MAP_JOB_VERIFIER}},
                {"JS map", {?JS_MAP_JOB, ?MAP_JOB_VERIFIER}}]).

main([]) ->
    usage();
main(Args) ->
    case setup_environment(Args) of
        {ok, NodeName, KeyCount, KeySize, Populate, RunJobs} ->
            do_verification(NodeName, KeyCount, KeySize, Populate, RunJobs);
        error ->
            usage()
    end.

do_verification(NodeName, KeyCount, KeySize, Populate, RunJobs) ->
    {T1, T2, T3} = erlang:now(),
    random:seed(T1, T2, T3),
    {ok, Client} = riak:client_connect(NodeName),
    case Populate of
        true ->
            io:format("Clearing old data from ~p~n", [?BUCKET]),
            ok = clear_bucket(Client, ?BUCKET, erlang:round(KeyCount * 1.25)),
            io:format("Populating new data to ~p~n", [?BUCKET]),
            ok = populate_bucket(Client, ?BUCKET, KeySize, KeyCount);
        false ->
            ok
    end,
    case RunJobs of
        true ->
            io:format("Verifying map/reduce jobs~n"),
            run_jobs(Client, ?BUCKET, KeyCount);
        false ->
            ok
    end.

clear_bucket(_Client, _BucketName, 0) ->
    ok;
clear_bucket(Client, BucketName, EntryNum) ->
    Key = entry_num_to_key(EntryNum),
    case Client:delete(BucketName, Key, 1) of
        R when R =:= ok orelse R =:= {error, notfound} ->
            clear_bucket(Client, BucketName, EntryNum - 1);
        Error ->
            io:format("Error: ~p~n", [Error])
    end.

populate_bucket(_Client, _BucketName, _KeySize, 0) ->
    ok;
populate_bucket(Client, BucketName, KeySize, EntryNum) ->
    Key = entry_num_to_key(EntryNum),
    Obj = riak_object:new(BucketName, Key, generate_body(KeySize)),
    ok = Client:put(Obj, 0),
    populate_bucket(Client, BucketName, KeySize, EntryNum - 1).

run_jobs(Client, Bucket, KeyCount) ->
    F = fun({Label, {Job, Verifier}}) ->
                verify_job(Client, Bucket, KeyCount, Label, Job, Verifier) end,
    lists:foreach(F, ?TESTS).

verify_job(Client, Bucket, KeyCount, Label, JobDesc, Verifier) ->
    io:format("Running ~p~n", [Label]),
    io:format("   Testing discrete entries..."),
    case verify_entries_job(Client, Bucket, KeyCount, JobDesc, Verifier) of
        {true, ETime} ->
            io:format("OK (~p)~n", [ETime]);
        {false, _} ->
            io:format("FAIL~n")
    end,
    io:format("   Testing full bucket mapred..."),
    case verify_bucket_job(Client, Bucket, KeyCount, JobDesc, Verifier) of
        {true, BTime} ->
            io:format("OK (~p)~n", [BTime]);
        {false, _} ->
            io:format("FAIL~n")
    end.

verify_bucket_job(Client, Bucket, KeyCount, JobDesc, Verifier) ->
    Start = erlang:now(),
    {ok, Result} = Client:mapred_bucket(Bucket, JobDesc, 120000),
    End = erlang:now(),
    {Verifier(bucket, Result, KeyCount), erlang:round(timer:now_diff(End, Start) / 1000)}.

verify_entries_job(Client, Bucket, KeyCount, JobDesc, Verifier) ->
    Inputs = select_inputs(Bucket, KeyCount),
    Start = erlang:now(),
    {ok, Result} = Client:mapred(Inputs, JobDesc, 120000),
    End = erlang:now(),
    {Verifier(entries, Result, length(Inputs)), erlang:round(timer:now_diff(End, Start) / 1000)}.

entry_num_to_key(EntryNum) ->
    list_to_binary(["mrv", integer_to_list(EntryNum)]).

select_inputs(Bucket, KeyCount) ->
    [{Bucket, entry_num_to_key(EntryNum)} || EntryNum <- lists:seq(1, KeyCount),
                                             random:uniform(2) == 2].

usage() ->
    io:format("~p -s <Erlang node name> -c <path to top of Riak source tree> [-k keycount -p -j]~n", [?MODULE]).

setup_environment(Args) ->
    case get_argument("-s", Args) of
        error ->
            error;
        Node ->
            case get_argument("-c", Args) of
                error ->
                    error;
                Path ->
                    case setup_code_paths(Path) of
                        ok ->
                            case setup_networking() of
                                ok ->
                                    {ok, list_to_atom(Node),
                                     list_to_integer(get_argument("-k", Args, "1000")),
                                     parse_key_size(get_argument("-ks", Args, "1b")),
                                     list_to_atom(get_argument("-p", Args, "false")),
                                     list_to_atom(get_argument("-j", Args, "false"))};
                                error ->
                                    error
                            end;
                        error ->
                            error
                    end
            end
    end.

setup_code_paths(Path) ->
    CorePath = filename:join([Path, "deps", "riak_core", "ebin"]),
    KVPath = filename:join([Path, "deps", "riak_kv", "ebin"]),
    LukePath = filename:join([Path, "deps", "luke", "ebin"]),
    setup_paths([{riak_core, CorePath}, {riak_kv, KVPath},
                 {luke, LukePath}]).

setup_paths([]) ->
    ok;
setup_paths([{Label, Path}|T]) ->
    case filelib:is_dir(Path) of
        true ->
            code:add_pathz(Path),
            setup_paths(T);
        false ->
            io:format("ERROR: Path for ~p (~p) not found or doesn't point to a directory.~n",
                      [Label, Path]),
            error
    end.

get_argument(Name, Args) ->
    get_argument(Name, Args, error).

get_argument(_Name, [], Default) ->
    Default;
get_argument(Name, [Name|T], _Default) ->
    case T of
        [] ->
            error;
        _ ->
            hd(T)
    end;
get_argument(Name, [_|T], Default) ->
    get_argument(Name, T, Default).

setup_networking() ->
    {T1, T2, T3} = erlang:now(),
    random:seed(T1, T2, T3),
    Name = "mapred_verify" ++ integer_to_list(random:uniform(100)),
    {ok, H} = inet:gethostname(),
    {ok, {O1, O2, O3, O4}} = inet:getaddr(H, inet),
    NodeName = list_to_atom(lists:flatten(
                              io_lib:format("~s@~p.~p.~p.~p",
                                            [Name, O1, O2, O3, O4]))),
    {ok, _} = net_kernel:start([NodeName, longnames]),
    erlang:set_cookie(NodeName, riak),
    ok.

parse_key_size("1b") ->
    1;
parse_key_size(KeySpec) ->
    Unit = case hd(lists:reverse(KeySpec)) of
               $b ->
                   1;
               $k ->
                   1024;
               _ ->
                   1
           end,
    Size = string:substr(KeySpec, 1, length(KeySpec) - 1),
    list_to_integer(Size) * Unit.

generate_body(1) ->
    <<"1">>;
generate_body(Size) ->
    generate_body(Size, [<<"1">>]).

generate_body(0, Accum) ->
    list_to_binary(Accum);
generate_body(Size, Accum) ->
    generate_body(Size - 1, [<<"0">>|Accum]).
