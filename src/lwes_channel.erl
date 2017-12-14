-module (lwes_channel).

-behaviour (gen_server).

-include_lib ("lwes.hrl").
-include ("lwes_internal.hrl").

%% API
-export ([ start_link/1,
           new/2,
           open/1,
           register_callback/4,
           send_to/2,
           close/1
         ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-record (state, {socket, channel, type, callback}).
-record (callback, {function, format, state}).

%%====================================================================
%% API functions
%%====================================================================
start_link (Channel) ->
  gen_server:start_link (?MODULE, [Channel], []).

new (Type, Config) ->
  #lwes_channel {
     type = Type,
     config = lwes_net_udp:new (Type, Config),
     ref = make_ref()
  }.

open (Channel) ->
  { ok, _Pid } = lwes_channel_manager:open_channel (Channel),
  { ok, Channel}.

register_callback (Channel, CallbackFunction, EventType, CallbackState) ->
  find_and_call ( Channel,
                  { register, CallbackFunction, EventType, CallbackState }).

send_to (Channel, Msg) ->
  find_and_call (Channel, { send, Msg }).

close (Channel) ->
  find_and_cast (Channel, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init ([ Channel = #lwes_channel { type = Type, config = Config } ]) ->
  { ok, Socket } = lwes_net_udp:open (Type, Config),
  lwes_stats:initialize (lwes_net_udp:address(Config)),
  lwes_channel_manager:register_channel (Channel, self()),
  { ok, #state { socket = Socket,
                 channel = Channel,
                 type = Type
               }
  }.

handle_call ({ register, Function, Format, Accum },
             _From,
             State = #state {
               channel = #lwes_channel {type = listener }
             }) ->
  { reply,
    ok,
    State#state { callback = #callback { function = Function,
                                         format   = Format,
                                         state    = Accum } } };

handle_call ({ send, Packet },
             _From,
             State = #state {
               socket = Socket,
               channel = #lwes_channel { config = Config }
             }) ->
  Reply =
    case lwes_net_udp:send (Socket, Config, Packet) of
      ok ->
        lwes_stats:increment_sent(lwes_net_udp:address(Config)),
        ok;
      {error, Error} ->
        lwes_stats:increment_errors(lwes_net_udp:address(Config)),
        {error, Error}
    end,
  { reply, Reply, State };
handle_call (_Request, _From, State) ->
  { reply, ok, State }.

handle_cast (stop, State) ->
  {stop, normal, State};
handle_cast (_Request, State) ->
  { noreply, State }.

% skip if we don't have a handler
handle_info ( {udp, _, _, _, _},
              State = #state {
                type = listener,
                channel = #lwes_channel { config = Config },
                callback = undefined
              } ) ->
  lwes_stats:increment_received (lwes_net_udp:address(Config)),
  { noreply, State };

handle_info ( Packet = {udp, _, _, _, _},
              State = #state {
                type = listener,
                channel = #lwes_channel { config = Config },
                callback = #callback { function = Function,
                                       format   = Format,
                                       state    = CbState }
              } ) ->
  lwes_stats:increment_received (lwes_net_udp:address(Config)),
  Event = lwes_event:from_udp_packet (Packet, Format),
  NewCbState = Function (Event, CbState),
  { noreply,
    State#state { callback = #callback { function = Function,
                                         format   = Format,
                                         state    = NewCbState }
                }
  };

handle_info (_Request, State) ->
  {noreply, State}.

terminate (_Reason, #state {socket = Socket, channel = Channel}) ->
  lwes_net_udp:close (Socket),
  lwes_channel_manager:unregister_channel (Channel).

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
find_and_call (Channel, Msg) ->
  case lwes_channel_manager:find_channel (Channel) of
    {error, not_open} ->
      {error, not_open};
    Pid ->
      gen_server:call ( Pid, Msg )
  end.

find_and_cast (Channel, Msg) ->
  case lwes_channel_manager:find_channel (Channel) of
    {error, not_open} ->
      {error, not_open};
    Pid ->
      gen_server:cast ( Pid, Msg )
  end.

%%====================================================================
%% Test functions
%%====================================================================
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

-endif.
