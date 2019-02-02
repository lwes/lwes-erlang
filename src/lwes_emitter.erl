-module(lwes_emitter).

% create any resources and return a fixed structure which will be sent in
% subsequent calls
-callback new(Config :: term()) -> State :: term().

% return an id to be used for stats gathering.  In most cases this should
% be an atom so that it will show up under the label column for lwes:stats()
% calls
-callback id(State :: term()) ->
    {Ip :: tuple(), Port :: integer() }
  | {Label :: atom(), {Ip :: tuple(), Port :: integer() }}
  | atom().

% prepare the event, in most cases this will use one of the to_* methods
% in lwes_event (like to_binary, to_iolist, to_json, etc), but also allows
% one to implement any serialization format as a plugin
-callback prep(Event :: term()) -> term().

% emit the event, this will get the State from new/1 as well as the event
% from prep/1
-callback emit(State :: term(), Event :: term()) -> ok | {error, atom()}.

% close the emitter, called on shutdown or if configuration is updated
% this could be called, then immediately new/1 called again.
-callback close(State :: term()) -> ok.
