%%%
%%% Light Weight Event System (LWES)
%%%
%%% Creating Events
%%%   Event0 = lwes_event:new ("MyEvent"),
%%%   Event1 = lwes_event:set_uint16 (Event0, "MyUint16", 25),
%%%
%%% Emitting
%%%
%%%   Channel = lwes:open (emitter, Ip, Port)
%%%   ok = lwes:emit (Channel, Event1).
%%%
%%% Listening via callback
%%%
%%%   Channel = lwes:open (listener, Ip, Port)
%%%   lwes:listen (Channel, Type, Fun, Accum).
%%%
%%%   Fun is called for each event
%%%
%%% Closing channel
%%%
%%%   lwes:close (Channel)

-module (lwes).

-include_lib ("lwes.hrl").

-ifdef(HAVE_EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export ([ open/3,
           emit/2,
           listen/4,
           close/1 ]).

%%====================================================================
%% API functions
%%====================================================================
open (Type, Ip, Port) ->
  try check_args (Type, Ip, Port) of
    { T, I, P } -> lwes_channel:open (T, I, P)
  catch
    _:_ -> {error, cant_open}
  end.

emit (Channel, Event) ->
  lwes_channel:send_to (Channel, lwes_event:to_binary (Event)).

%
% Callback function - function is called with an event in given format
%                     and the current state, it should return the next
%                     state
%
% Type is one of
%
%   raw    - callback is given raw udp structure, use lwes_event:from_udp to
%            turn into event
%   list   - callback is given an #lwes_event record where the name is a
%            binary, and the attributes is a proplist where keys are binaries,
%            and values are either integers (for lwes int types), binaries
%            (for lwes strings), true|false atoms (for lwes booleans),
%            or 4-tuples (for lwes ip addresses)
%   tagged - callback is given an #lwes_event record where the name is a
%            binary, and the attributes are 3-tuples with the first element
%            the type of data, the second the key as a binary and the
%            third the values as in the list format
%
% Initial State is whatever you want
listen (Channel, CallbackFunction, EventType, CallbackInitialState)
  when is_function (CallbackFunction, 2),
       EventType =:= raw ; EventType =:= tagged ; EventType =:= list ->
  lwes_channel:register_callback (Channel, CallbackFunction,
                                  EventType, CallbackInitialState).

close (Channel) ->
  lwes_channel:close (Channel).

%%====================================================================
%% Internal functions
%%====================================================================

check_args (Type, Ip, Port) ->
  { normalize_type (Type), lwes_util:normalize_ip (Ip), normalize_port (Port) }.

normalize_port (Port) when is_integer (Port), Port >= 0, Port =< 65535 ->
  Port;
normalize_port (Port) when is_list (Port) ->
  list_to_integer (Port);
normalize_port (_) ->
  erlang:error (badarg).

normalize_type (Type) when Type =:= emitter; Type =:= listener ->
  Type;
normalize_type (_) ->
  erlang:error (badarg).

%%====================================================================
%% Test functions
%%====================================================================
-ifdef(EUNIT).

-endif.
