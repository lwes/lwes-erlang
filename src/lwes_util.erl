-module (lwes_util).

-include_lib ("lwes.hrl").
-include_lib ("lwes_internal.hrl").

-ifdef(HAVE_EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

% API
-export ([normalize_ip/1,
          ip2bin/1,
          check_ip_port/1,
          ceiling/1,
          count_ones/1,
          any_to_binary/1,
          arr_to_binary/1,
          binary_to_any/2,
          binary_to_arr/2]).

%%====================================================================
%% API functions
%%====================================================================
normalize_ip (Ip) when ?is_ip_addr (Ip) ->
  Ip;
normalize_ip (Ip) when is_list (Ip) ->
  case inet_parse:address (Ip) of
    {ok, {N1, N2, N3, N4}} -> {N1, N2, N3, N4};
    _ -> erlang:error(badarg)
  end;
normalize_ip (_) ->
  % essentially turns function_clause error into badarg
  erlang:error (badarg).

ip2bin (Ip) ->
  list_to_binary(inet_parse:ntoa (Ip)).

check_ip_port ({Ip, Port}) when ?is_ip_addr (Ip) andalso ?is_uint16 (Port) ->
  {Ip, Port};
check_ip_port ({Ip, Port}) when is_list (Ip) andalso ?is_uint16 (Port) ->
  case inet_parse:address (Ip) of
    {ok, {N1, N2, N3, N4}} -> {{N1, N2, N3, N4}, Port};
    _ -> erlang:error(badarg)
  end;
check_ip_port (_) ->
  % essentially turns function_clause error into badarg
  erlang:error (badarg).

ceiling(X) when X < 0 ->
    trunc(X);
ceiling(X) ->
    T = trunc(X),
    case X - T == 0 of
        true  -> T;
        false -> T + 1
    end.

count_ones(Bin) -> count_ones(Bin, 0).
count_ones(<<>>, Counter) -> Counter;
count_ones(<<X:1, Rest/bitstring>>, Counter) ->
  count_ones(Rest, Counter + X).

arr_to_binary (L) ->
  [any_to_binary (E) || E <- L ].

any_to_binary (I) when is_integer (I) ->
  list_to_binary (integer_to_list (I));
any_to_binary (F) when is_float (F) ->
  list_to_binary (float_to_list (F));
any_to_binary (L = [[_|_]|_]) when is_list (L) ->
  % support list of lists, being turned into list of binaries 
  [ list_to_binary (E) || E <- L ];
any_to_binary (L) when is_list (L) ->
  list_to_binary (L);
any_to_binary (A) when is_atom (A) ->
  list_to_binary (atom_to_list (A));
any_to_binary (B) when is_binary (B) ->
  B.   

binary_to_arr (List, Type) ->
  [ case E of 
      null -> undefined;
         _ -> binary_to_any(E, Type) end || E <- List ].

binary_to_any (Bin, binary) when is_binary (Bin) ->
  Bin;
binary_to_any (Bin, Type) when is_binary (Bin) ->
  binary_to_any (binary_to_list (Bin), Type);
binary_to_any (List, integer) ->
  {I, _} = string:to_integer (List),
  I;
binary_to_any (List, float) ->
  {F, _} = string:to_float (List),
  F;
binary_to_any (List, ipaddr) -> normalize_ip (List);
binary_to_any (L = [H|_], list) when is_binary (H) ->
  [ binary_to_list (E) || E <- L ];
binary_to_any (List, list) ->
  List;
binary_to_any (List, atom) ->
  list_to_atom (List).


%%====================================================================
%% Test functions
%%====================================================================
-ifdef(EUNIT).

normalize_ip_test () ->
  ?assertEqual ({127,0,0,1}, normalize_ip ("127.0.0.1")),
  ?assertEqual ({127,0,0,1}, normalize_ip ({127,0,0,1})),
  ?assertError (badarg, normalize_ip ("655.0.0.1")),
  ?assertError (badarg, normalize_ip ({655,0,0,1})).

check_ip_port_test () ->
  ?assertEqual ({{127,0,0,1},9191}, check_ip_port ({"127.0.0.1",9191})),
  ?assertEqual ({{127,0,0,1},9191}, check_ip_port ({{127,0,0,1},9191})),
  ?assertError (badarg, check_ip_port ({"655.0.0.1",9191})),
  ?assertError (badarg, check_ip_port ({{655,0,0,1},9191})),
  ?assertError (badarg, check_ip_port ({{127,0,0,1},91919})).

ceil_test () ->
  ?assertEqual(8, ceiling(7.5)),
  ?assertEqual(-10, ceiling(-10.9)).

count_ones_test () ->
  [?assertEqual(7, count_ones(27218)),
  ?assertEqual(0, ceiling(0))].

binary_test_ () ->
  [
    ?_assertEqual (U, binary_to_any (any_to_binary (U), T))
    || { U, T }
    <-
    [
      { 1.35, float },
      { 1, integer },
      { 1234567890123, integer },
      { <<"b">>, binary },
      { "b", list },
      { true, atom },
      { ["1"], list },
      { ["1","2"], list }
    ]
  ].

-endif.
