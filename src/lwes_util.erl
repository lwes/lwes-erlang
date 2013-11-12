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
          count_ones/1]).

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

-endif.
