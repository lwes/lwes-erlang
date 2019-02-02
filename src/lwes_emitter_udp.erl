-module (lwes_emitter_udp).

-behaviour (lwes_emitter).

-export ([ new/1,
           id/1,
           prep/1,
           emit/2,
           close/1
         ]).

new (Config) ->
  lwes_net_udp:new (emitter, Config).

id (Config) ->
  lwes_net_udp:address(Config).

prep (Event) ->
  lwes_event:to_iolist(Event).

emit (Config, Event) ->
  Address = lwes_net_udp:address(Config),
  lwes_emitter_udp_pool:send (lwes_emitters, Address, Event).

close (_Config) ->
  ok.
