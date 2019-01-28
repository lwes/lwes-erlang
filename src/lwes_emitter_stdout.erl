-module (lwes_emitter_stdout).

-export ([ new/1,
           emit/2,
           close/1
         ]).

new (Config) ->
  io:format("Creating new emitter with ~p~n",[Config]),
  lwes_stats:initialize(?MODULE),
  {ok, {?MODULE, {0}}}.

emit ({?MODULE, {CountIn}}, E) ->
  io:format("Emit event ~p for ~p~n",[E,?MODULE]),
  lwes_stats:increment_sent (?MODULE),
  {?MODULE, {CountIn + 1}}.

close ({?MODULE, {CountFinal}}) ->
  io:format("Closing ~p after ~p events~n",[?MODULE,CountFinal]),
  lwes_stats:delete(?MODULE),
  ok.
