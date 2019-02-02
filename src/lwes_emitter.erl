-module(lwes_emitter).

-callback new(Config :: term()) -> term().
-callback id(Config :: term()) ->
  {Ip :: list() | tuple(), Port :: integer() } | atom().
-callback prep(Event :: term()) -> term().
-callback emit(Config :: term(), Event :: term()) -> ok | {error, atom()}.
-callback close(Config :: term()) -> ok.
