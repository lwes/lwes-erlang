-module (lwes_emitter_stdout).

-export ([ new/1,
           emit/2,
           close/1
         ]).

new (Config) ->
  io:format("Creating new emitter with ~p~n",[Config]),
  {ok, {lwes_emitter_stdout, {0}}}.

emit ({lwes_emitter_stdout, {CountIn}}, E) ->
  io:format("Emit event ~p for ~p~n",[E,lwes_emitter_stdout]),
  {lwes_emitter_stdout, {CountIn + 1}}.

close ({lwes_emitter_stdout, {CountFinal}}) ->
  io:format("Closing ~p after ~p events~n",[lwes_emitter_stdout,CountFinal]),
  ok.
