-module (lwes_emitter_stdout).

-behaviour (lwes_emitter).

-export ([ new/1,
           id/1,
           prep/1,
           emit/2,
           close/1
         ]).

-record (lwes_emitter_stdout, {config}).

new (Config) ->
  #lwes_emitter_stdout { config = Config }.

id(#lwes_emitter_stdout {config = [{label,B}]}) ->
  B.

prep (Event) ->
  lwes_event:to_binary(Event).

emit (L, Event) when is_list(L) ->
  [ emit(E, Event) || E <- L ];
emit (#lwes_emitter_stdout {}, E) ->
  io:format("Emit event ~p for ~p~n",[E,?MODULE]),
  ok.

close (#lwes_emitter_stdout {}) ->
  ok.
