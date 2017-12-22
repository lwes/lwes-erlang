-module (lwes_net_udp).

-include_lib ("lwes_internal.hrl").

% This modules attempts to separate out the low level network details from the
% rest of the system.  It is by no means necessary to use this as a transport
% but the base system assumes UDP or UDP/Multicast, so those forms are
% attempted to be encapsulated here.  A certain set of options is set by
% default.
-export([new/2,
         open/2,
         send/3,
         close/1,
         address/1
        ]).

-define (DEFAULT_TTL, 25).
-define (DEFAULT_RECBUF, 16777216).

-record(lwes_net_udp, { ip                       :: inet:ip_address(),
                        port                     :: inet:port_number(),
                        is_multicast = false     :: boolean(),
                        ttl = ?DEFAULT_TTL       :: non_neg_integer(),
                        recbuf = ?DEFAULT_RECBUF :: non_neg_integer(),
                        options = []
                      }).
new (listener, Config) ->
  InitialStruct = check_config (Config),
  case InitialStruct of
    #lwes_net_udp { ip = Ip, is_multicast = true, ttl = TTL,
                    recbuf = Recbuf, options = ExtraOptions} ->
      InitialStruct#lwes_net_udp {
        options = merge_options ([ { ip, Ip },
                                   { multicast_ttl, TTL },
                                   { multicast_loop, false },
                                   { add_membership, {Ip, {0,0,0,0}}},
                                   { reuseaddr, true },
                                   { recbuf, Recbuf },
                                   { mode, binary }
                                 ],
                                 ExtraOptions)
      };
    #lwes_net_udp { is_multicast = false,
                    recbuf = Recbuf, options = ExtraOptions} ->
      InitialStruct#lwes_net_udp {
        options = merge_options ([ { reuseaddr, true },
                                   { recbuf, Recbuf },
                                   { mode, binary }
                                 ],
                                 ExtraOptions)
      }
  end;
new (emitter, Config) ->
  InitialStruct = check_config (Config),
  InitialOptions = InitialStruct#lwes_net_udp.options,
  TTL = InitialStruct#lwes_net_udp.ttl,

  % emitters don't care about most options, but including ttl
  % means we can use the same emitter for multicast as well as
  % unicast traffic
  InitialStruct#lwes_net_udp {
    options = merge_options ([ { multicast_ttl, TTL },
                               { mode, binary }
                             ],
                             InitialOptions)
  }.

open (listener, #lwes_net_udp { port = Port, options = Options}) ->
  gen_udp:open (Port, Options);
open (emitter, #lwes_net_udp { options = Options}) ->
  gen_udp:open (0, Options).

% allow some of the other forms of config, so the config structure can
% just be reused when sending in most cases
send (Socket, {Ip, Port, _}, Packet) ->
  gen_udp:send (Socket, Ip, Port, Packet);
send (Socket, {Ip, Port, _, _}, Packet) ->
  gen_udp:send (Socket, Ip, Port, Packet);
send (Socket, {Ip, Port}, Packet) ->
  gen_udp:send (Socket, Ip, Port, Packet);
send (Socket, #lwes_net_udp { ip = Ip, port = Port}, Packet) ->
  gen_udp:send (Socket, Ip, Port, Packet).

close (Socket) ->
  gen_udp:close (Socket).

address (#lwes_net_udp { ip = Ip, port = Port }) ->
  {Ip, Port}.

%%====================================================================
%% Internal functions
%%====================================================================

% this will merge in any passed in Overrides options on top of any defaults
% listed.  it's currently assumed there are no raw defaults
merge_options (Defaults, Overrides) ->
  % first split into options and raw options
  {OverrideOpts, OverrideRaw} = partition (Overrides),
  % then merge, preferring overrides and add back the raw at the end
  lists:ukeymerge(1, lists:sort(OverrideOpts), lists:sort(Defaults))
  ++ OverrideRaw.

% gen_udp has a long list of mostly consistent options, the few outliers are
%   binary | list - these can also be specified as {mode, binary | list}
%   {raw,_,_,_} - these do not have a 2-tuple form
% this function will normalize on {mode, binary | list} and separate out
% {raw,_,_,_} options, resulting in two lists
partition (Options) ->
  lists:foldl( fun (list, {A,O}) -> {[{mode,list} | A],O};
                   (binary, {A,O}) -> {[{mode,binary} | A],O};
                   (R = {raw,_,_,_},{A,O}) -> {A,[R|O]};
                   (Other, {A,O}) -> {[Other|A],O}
               end,
               {[],[]},
               Options).

parse_options ([], AccumlatedOptions) ->
  AccumlatedOptions;
% some options are simply passed through
parse_options ([{recbuf, Recbuf} | Rest], N) ->
  parse_options (Rest, N#lwes_net_udp{ recbuf = Recbuf });
% others have slightly different naming, so are changed
parse_options ([{ttl, TTL} | Rest], N ) ->
  parse_options (Rest, N#lwes_net_udp { ttl = TTL });
% check_config options which get converted to raw options
parse_options ([reuseport | Rest],
               N = #lwes_net_udp { options = OptionsIn}) ->
  parse_options (Rest, N#lwes_net_udp { options = reuseport() ++ OptionsIn});
% finally options which we assume are correct for now
parse_options ([O | Rest],
               N = #lwes_net_udp { options = OptionsIn}) ->
  parse_options (Rest, N#lwes_net_udp { options = [ O | OptionsIn]}).

is_multicast ({N1, _, _, _}) when N1 >= 224, N1 =< 239 ->
  true;
is_multicast (_) ->
  false.

reuseport() ->
  case os:type() of
    {unix, linux} ->
      [ {raw, 1, 15, <<1:32/native>>} ];
    {unix, OS} when OS =:= darwin;
                    OS =:= freebsd;
                    OS =:= openbsd;
                    OS =:= netbsd ->
      [ {raw, 16#ffff, 16#0200, <<1:32/native>>} ];
    _ -> []
  end.

check_ip (Ip) when ?is_ip_addr (Ip) ->
  Ip;
check_ip (IpList) when is_list (IpList) ->
  case inet_parse:address (IpList) of
    {ok, Ip} -> Ip;
    _ ->
      erlang:error(badarg)
  end;
check_ip (_) ->
  % essentially turns function_clause error into badarg
  erlang:error (badarg).

check_port(Port) when ?is_uint16 (Port) ->
  Port;
check_port(_) ->
  % essentially turns function_clause error into badarg
  erlang:error (badarg).

check_config ({Ip, Port}) ->
  CheckedIp = check_ip (Ip),
  #lwes_net_udp { ip = CheckedIp,
                  port = check_port(Port),
                  is_multicast = is_multicast(CheckedIp) };
check_config ({Ip, Port, Options}) when is_list (Options) ->
  parse_options (Options, check_config ({Ip,Port}));
check_config ({Ip, Port, TTL}) when ?is_ttl (TTL) ->
  InitialStruct = check_config ({Ip, Port}),
  InitialStruct#lwes_net_udp { ttl = TTL };
check_config ({Ip, Port, TTL, Recbuf})
  when ?is_ttl (TTL) andalso is_integer(Recbuf) ->
  InitialStruct = check_config ({Ip, Port}),
  InitialStruct#lwes_net_udp { ttl = TTL, recbuf = Recbuf };
check_config (_) ->
  % essentially turns function_clause error into badarg
  erlang:error (badarg).


%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

test_one (Config) ->
  EmitterConfig = new (emitter, Config),
  ListenerConfig = new (listener, Config),
  {ok, Emitter} = lwes_net_udp:open(emitter, EmitterConfig),
  {ok, Listener}= lwes_net_udp:open(listener, ListenerConfig),
  Input = <<"hello">>,
  lwes_net_udp:send(Emitter, EmitterConfig, Input),
  Output = receive {udp,_,_,_,P} -> P end,
  lwes_net_udp:close(Listener),
  lwes_net_udp:close(Emitter),
  ?assertEqual (Input, Output).

% test emission and receipt with various options
lwes_net_udp_test_ () ->
  { inorder,
    [
      fun () -> test_one (C) end
      || C
      <- [
           {"127.0.0.1",12321},
           {"127.0.0.1",12321,3},
           {"127.0.0.1",12321,12,65535},
           {"224.1.1.111",12321,[{multicast_loop, true}]}
         ]
    ]
  }.

% test address functionality
lwes_net_udp_address_test_ () ->
  [
    ?_assertEqual (Expected, address(Given))
    || {Expected, Given}
    <- [
         {{{127,0,0,1},9191}, new(emitter, {"127.0.0.1",9191})}
       ]
  ].

% test various error cases
lwes_net_udp_error_test_ () ->
  [
    ?_assertError (badarg, new(emitter,{foo,bar})),
    ?_assertError (badarg, new(emitter,{"256.0.0.0",99})),
    ?_assertError (badarg, new(emitter,{"127.0.0.1",99999}))
  ].

% test options merging
lwes_net_udp_options_test_ () ->
  [
    ?_assertEqual(Expected, merge_options (Defaults, Overrides))
    || {Expected, Defaults, Overrides}
    <- [
         { [{mode,list}], [{mode,binary}], [list] },
         { [{mode,binary}], [{mode,binary}], [binary] },
         { [{mode,list},{raw,a,b,c}], [{mode,binary}], [list,{raw,a,b,c}] },
         { [{add_membership,{{127,0,0,1},{0,0,0,0}}},
            {ip,{127,0,0,1}},
            {mode,binary},
            {multicast_loop,true},
            {multicast_ttl,12},
            {recbuf,65535},
            {reuseaddr,true}],
           [{ip, {127,0,0,1}},
            {multicast_ttl, 12},
            {multicast_loop, false},
            {add_membership, {{127,0,0,1}, {0,0,0,0}}},
            {reuseaddr, true},
            {recbuf, 65535},
            {mode, binary}
           ],
           [{multicast_loop, true}]
         }

       ]
  ].

check_config_test_ () ->
  [
    ?_assertEqual (#lwes_net_udp {ip = {127,0,0,1}, port = 9191,
                                  is_multicast = false,
                                  ttl = 3, recbuf = 65535},
                   check_config ({{127,0,0,1},9191,3,65535})),
    ?_assertEqual (#lwes_net_udp {ip = {127,0,0,1}, port = 9191,
                                  is_multicast = false,
                                  ttl = 3, recbuf = 65535},
                   check_config ({"127.0.0.1",9191,3,65535})),
    ?_assertEqual (#lwes_net_udp {ip = {127,0,0,1}, port = 9191,
                                  is_multicast = false,
                                  ttl = 3, recbuf = 65535},
                   check_config ({"127.0.0.1",9191,[{ttl,3},{recbuf,65535}]})),
    ?_assertEqual (#lwes_net_udp {ip = {127,0,0,1}, port = 9191,
                                  is_multicast = false,
                                  ttl = 3, recbuf = 65535,
                                  options = [{raw, 1, 15, <<1:32/native>>}] },
                   check_config ({"127.0.0.1",9191,
                                   [{ttl,3}, {recbuf,65535}, reuseport]})),
    ?_assertEqual (#lwes_net_udp {ip = {127,0,0,1}, port = 9191,
                                  is_multicast = false,
                                  ttl = 3, recbuf = ?DEFAULT_RECBUF},
                   check_config ({"127.0.0.1",9191,3})),
    ?_assertEqual (#lwes_net_udp {ip = {127,0,0,1}, port = 9191,
                                  is_multicast = false,
                                  ttl = 3, recbuf = ?DEFAULT_RECBUF},
                                  check_config ({{127,0,0,1},9191,3})),
    ?_assertEqual (#lwes_net_udp {ip = {127,0,0,1}, port = 9191,
                                  is_multicast = false,
                                  ttl = ?DEFAULT_TTL, recbuf = ?DEFAULT_RECBUF},
                   check_config ({"127.0.0.1",9191})),
    ?_assertEqual (#lwes_net_udp {ip = {127,0,0,1}, port = 9191,
                                  is_multicast = false,
                                  ttl = ?DEFAULT_TTL, recbuf = ?DEFAULT_RECBUF},
                                  check_config ({{127,0,0,1},9191})),
    ?_assertError (badarg, check_config ({"655.0.0.1",9191,3})),
    ?_assertError (badarg, check_config ({"655.0.0.1",9191,65})),
    ?_assertError (badarg, check_config ({"655.0.0.1",9191})),
    ?_assertError (badarg, check_config ({{655,0,0,1},9191})),
    ?_assertError (badarg, check_config ({{127,0,0,1},91919}))
  ].

-endif.
