-module (lwes_util).

-include_lib ("lwes.hrl").
-include_lib ("lwes_internal.hrl").

% API
-export ([normalize_ip/1,
          ip2bin/1,
          ceiling/1,
          count_ones/1,
          any_to_list/1,
          any_to_binary/1,
          arr_to_binary/1,
          arr_to_binary/2,
          binary_to_any/2,
          binary_to_arr/2]).

%%====================================================================
%% API functions
%%====================================================================
normalize_ip (Ip) when ?is_ip_addr (Ip) ->
  Ip;
normalize_ip (Ip) when is_binary (Ip) ->
  normalize_ip (binary_to_list (Ip));
normalize_ip (Ip) when is_list (Ip) ->
  case inet_parse:address (Ip) of
    {ok, {N1, N2, N3, N4}} -> {N1, N2, N3, N4};
    _ -> erlang:error(badarg)
  end;
normalize_ip (_) ->
  % essentially turns function_clause error into badarg
  erlang:error (badarg).

ip2bin (Ip) when is_binary (Ip) ->
  Ip;
ip2bin (Ip) ->
  list_to_binary (inet_parse:ntoa (Ip)).


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

any_to_list (B) when is_binary (B) ->
  binary_to_list (B);
any_to_list (L) when is_list (L) ->
  L;
any_to_list (I) when is_integer (I) ->
  integer_to_list (I);
any_to_list (F) when is_float (F) ->
  float_to_list (F);
any_to_list (A) when is_atom (A) ->
  atom_to_list (A).

arr_to_binary (L, ipaddr) ->
  [ip2bin(E) || E <- L ].
arr_to_binary (L) ->
  [any_to_binary (E) || E <- L ].

any_to_binary (Ip) when ?is_ip_addr (Ip) ->
  ip2bin (Ip);
any_to_binary (T) when is_tuple (T) ->
  T;
any_to_binary (I) when is_integer (I) ->
  list_to_binary (integer_to_list (I));
any_to_binary (F) when is_float (F) ->
  list_to_binary (float_to_list (F));
any_to_binary (L = [H|_]) when is_tuple (H) ->
  L;
any_to_binary (L = [[_|_]|_]) when is_list (L) ->
  % support list of lists, being turned into list of binaries 
  [ any_to_binary (E) || E <- L ];
any_to_binary (L) when is_list (L) ->
  list_to_binary (L);
any_to_binary (A) when is_atom (A) ->
  list_to_binary (atom_to_list (A));
any_to_binary (B) when is_binary (B) ->
  B.

binary_to_arr (List, Type) ->
  [ case E of
      null -> undefined;
         _ -> binary_to_any(E, Type)
    end
    || E
    <- List
  ].

binary_to_any (Bin, binary) when is_binary (Bin) ->
  Bin;
binary_to_any (L = [B|_], binary) when is_binary (B) ->
  L;
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
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

normalize_ip_test_ () ->
  [
    ?_assertEqual ({127,0,0,1}, normalize_ip (<<"127.0.0.1">>)),
    ?_assertEqual ({127,0,0,1}, normalize_ip ("127.0.0.1")),
    ?_assertEqual ({127,0,0,1}, normalize_ip ({127,0,0,1})),
    ?_assertError (badarg, normalize_ip (<<"655.0.0.1">>)),
    ?_assertError (badarg, normalize_ip ("655.0.0.1")),
    ?_assertError (badarg, normalize_ip ({655,0,0,1}))
  ].


ceil_test_ () ->
  [
    ?_assertEqual(8, ceiling(7.5)),
    ?_assertEqual(-10, ceiling(-10.9)),
    ?_assertEqual(0, ceiling(0))
  ].

count_ones_test_ () ->
  [
    ?_assertEqual(0, count_ones (<<2#00000000>>)),
    ?_assertEqual(1, count_ones (<<2#00000001>>)),
    ?_assertEqual(1, count_ones (<<2#00000010>>)),
    ?_assertEqual(2, count_ones (<<2#00000011>>)),
    ?_assertEqual(2, count_ones (<<2#11000000>>)),
    ?_assertEqual(7, count_ones (<<2#01111111>>)),
    ?_assertEqual(14, count_ones (<<2#01111111,2#01111111>>))
  ].

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
