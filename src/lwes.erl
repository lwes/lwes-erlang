%%%
%%% Light Weight Event System (LWES)
%%%
%%% Creating Events
%%%   Event0 = lwes_event:new ("MyEvent"),
%%%   Event1 = lwes_event:set_uint16 (Event0, "MyUint16", 25),
%%%
%%% Emitting to a single channel
%%%
%%%   {ok, Channel0} = lwes:open (emitter, {Ip, Port})
%%%   Channel1 = lwes:emit (Channel0, Event1).
%%%
%%% Emit to several channels
%%%
%%%   % emit to 1 of a set in a round robin fashion
%%%   {ok, Channels0} = lwes:open (emitters, {1, [{Ip1,Port1},...{IpN,PortN}]})
%%%   Channels1 = lwes:emit (Channels0, Event1)
%%%   Channels2 = lwes:emit (Channels1, Event2)
%%%   ...
%%%   lwes:close (ChannelsN)
%%%
%%%   % emit to 2 of a set in an m of n fashion (ie, emit to first 2 in list,
%%%   % then 2nd and 3rd, then 3rd and 4th, etc., wraps at end of list)
%%%   {ok, Channels0} = lwes:open (emitters, {2, [{Ip1,Port1},...{IpN,PortN}]})
%%%
%%% Listening via callback
%%%
%%%   {ok, Channel} = lwes:open (listener, {Ip, Port})
%%%   lwes:listen (Channel, Fun, Type, Accum).
%%%
%%%   Fun is called for each event
%%%
%%% Closing channel
%%%
%%%   lwes:close (Channel)

-module (lwes).

-include_lib ("lwes.hrl").
-include ("lwes_internal.hrl").

-ifdef(HAVE_EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export ([ open/2,
           emit/2,
           listen/4,
           close/1 ]).

%%====================================================================
%% API functions
%%====================================================================
%
% open an lwes emitter, listener or set of emitters
%
% config for emitter/listener is {Ip, Port}
% config for emitters is {M,[{Ip0,Port0},...,{IpN,PortN}]}
%
open (emitters, Config) ->
  lwes_multi_emitter:open (Config);
open (Type, Config) when Type =:= emitter; Type =:= listener ->
  try lwes_util:check_ip_port (Config) of
    C -> lwes_channel:open (Type, C)
  catch
    _:_ -> { error, bad_ip_port }
  end;
open (_, _) ->
  { error, bad_type }.

% emit an event to one or more channels
emit (Channel, Event) when is_record (Channel, lwes_channel) ->
  ok = lwes_channel:send_to (Channel, lwes_event:to_binary (Event)),
  % channel doesn't actually change for a single emitter
  Channel;
emit (Channels, Event) when is_record (Channels, lwes_multi_emitter) ->
  lwes_multi_emitter:emit (Channels, lwes_event:to_binary (Event)).

%
% listen for events
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
%   dict   - callback is given an #lwes_event record where the name is a
%            binary, and the attributes are a dictionary with a binary
%            key and value according to the type
%   json   - this returns a proplist instead of an #lwes_event record.  The
%            valuse are mostly the same as list, but ip addresses are strings
%            (as binary).  This should means you can pass the returned value
%            to mochijson2:encode (or other json encoders), and have the event
%            as a json document
%
% Initial State is whatever you want
listen (Channel, CallbackFunction, EventType, CallbackInitialState)
  when is_function (CallbackFunction, 2),
       EventType =:= raw ; EventType =:= tagged ;
       EventType =:= list ; EventType =:= dict ;
       EventType =:= json ->
  lwes_channel:register_callback (Channel, CallbackFunction,
                                  EventType, CallbackInitialState).

% close the channel or channels
close (Channel) when is_record (Channel, lwes_channel) ->
  lwes_channel:close (Channel);
close (Channels) when is_record (Channels, lwes_multi_emitter) ->
  lwes_multi_emitter:close (Channels).

%%====================================================================
%% Internal functions
%%====================================================================

%%====================================================================
%% Test functions
%%====================================================================
-ifdef(EUNIT).

-endif.
