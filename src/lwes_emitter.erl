-module (lwes_emitter).

-behaviour (gen_server).

%% API
-export ([start_link/1,
          send/3
         ]).

-export ([init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2,
          code_change/3]).

-record (state, {id, queue = queue:new() }).

% there are 2 type of records stored in the ets table, we'll use the
% first element as the keypos, so either the port for a socket, or
% the id for config
-record (connection, {socket,
                      create_time_epoch_seconds,
                      access_time_epoch_seconds,
                      max_age_seconds}).
-define (CONNECTION_CREATE_TIME_INDEX, #connection.create_time_epoch_seconds).
-define (CONNECTION_LAST_ACCESS_INDEX, #connection.access_time_epoch_seconds).
-define (CONNECTION_MAX_AGE_INDEX,     #connection.max_age_seconds).

-record (config, {id,
                  max_age_seconds = 60,
                  max_connections = 65535, % set unreasonably high?
                  active = 0,
                  busy = 0
                 }).
-define (CONFIG_MAX_INDEX, #config.max_connections).
-define (CONFIG_ACTIVE_INDEX, #config.active).
-define (CONFIG_BUSY_INDEX, #config.busy).
-define (CONFIG_MAX_AGE_INDEX, #config.max_age_seconds).

-define (TABLE, lwes_emitters).

%%====================================================================
%% API
%%====================================================================
start_link (Config = #config {id = Id}) ->
  gen_server:start_link({local, Id}, ?MODULE, [Config], []);
start_link (ConfigList) when is_list (ConfigList) ->
  case parse_config (ConfigList, #config {}) of
    {error, E} -> {stop, {error, E}};
    {ok, Config}-> start_link (Config)
  end.

send (Id, AddressOrAddresses, Packet) ->
  case checkout (Id) of
    {error, Error} ->
      {error, {checkout, Error}}; % Error is {error, busy} or {error, down}
    Socket ->
      % I want to optimize by allowing for the sending of a packet to a
      % list of addressses, so convert to a list in all cases
      Addresses =
        case AddressOrAddresses of
          L when is_list(L) -> L;
          _ -> [AddressOrAddresses]
        end,
      % then fold over the list keeping track of the last error if it
      % exists
      case
        lists:foldl (fun (Address, Accum) ->
                       case lwes_net_udp:send (Socket, Address, Packet) of
                         {error, Error} ->
                           lwes_stats:increment_errors (Address),
                           {error, Error};
                         _ ->
                           lwes_stats:increment_sent (Address),
                           Accum
                       end
                     end,
                     ok,
                     Addresses) of
        {error, Error} ->
          checkin (Id, Socket, error),
          {error, {call, Error}};
        Answer ->
          checkin (Id, Socket),
          Answer
      end
  end.

out (Id) -> gen_server:call (Id, {out}).
in (Id, Socket) -> gen_server:cast (Id, {in, Socket}).

checkout (Id) ->
  case out (Id) of
    empty -> open (Id);
    {value, Socket} -> Socket
  end.

checkin (Id, Socket, error) ->
  close (Id, Socket).

checkin (Id, Socket) ->
  % at checkin, we'll do all our housekeeping

  % get the time now
  Now = seconds_since_epoch(),

  % fetch out the start time and set the last access
  [Start, MaxAge] =
    ets:update_counter (Id, Socket,
                        [ {?CONNECTION_CREATE_TIME_INDEX, 0},
                          {?CONNECTION_MAX_AGE_INDEX, 0} ] ),

  % check to see if we've been alive for more than the alloted alive time
  case Now - Start > MaxAge of
    true ->
      % alive too long, so close
      close (Id, Socket);
    false ->
      ets:update_element (Id, Socket,
                          {?CONNECTION_LAST_ACCESS_INDEX, Now}),
      in (Id, Socket)
  end.

open (Id) ->
  % optimistically update the active connections, so we don't go above max
  [Max, Active] =
    ets:update_counter (Id, Id,
                        [ {?CONFIG_MAX_INDEX,0},
                          {?CONFIG_ACTIVE_INDEX,1}
                        ]),

  % then check if the active connections are more than the max
  case Active > Max of
    true ->
      % we decrement active connections because we optimistically incremented
      % above, also increment busy index.
      ets:update_counter (Id, Id, [{?CONFIG_BUSY_INDEX,1},
                                   {?CONFIG_ACTIVE_INDEX,-1}]),
      {error, busy};
    false ->
      case ets:lookup (Id, Id) of
        [#config { max_age_seconds = MaxAge } ] ->
          % address doesn't matter for emitter's it's added at send time
          Dummy = lwes_net_udp:new (emitter, {"127.0.0.1",9191}),
          case lwes_net_udp:open (emitter, Dummy) of
            {ok, Socket} ->
              % Let this gen_server be the controlling process, not the
              % process opening the connection, this allows the process
              % opening the connection to die without killing the connection
              ok = gen_tcp:controlling_process (Socket, whereis (Id)),

              Now = seconds_since_epoch(),
              ets:insert_new (Id,
                              #connection { socket = Socket,
                                            create_time_epoch_seconds = Now,
                                            access_time_epoch_seconds = Now,
                                            max_age_seconds = MaxAge
                                          }),
              Socket;
            {error, E} ->
              % we decrement active connections because we optimistically
              % incremented above, also increment busy index.
              ets:update_counter (Id, Id, [{?CONFIG_BUSY_INDEX,1},
                                           {?CONFIG_ACTIVE_INDEX,-1}]),

              error_logger:error_msg ("Unexpected connect error 1 : ~p", [E]),
              {error, down}
          end;
        E2 ->
          % we decrement active connections because we optimistically
          % incremented above, also increment busy index.
          ets:update_counter (Id, Id, [{?CONFIG_BUSY_INDEX,1},
                                       {?CONFIG_ACTIVE_INDEX,-1}]),

          error_logger:error_msg ("Unexpected connect error 2 : ~p", [E2]),
          {error, down}
      end
  end.

close (Id, Socket) ->
  ets:update_counter (Id, Id, [{?CONFIG_ACTIVE_INDEX,-1}]),
  ets:delete (Id, Socket),
  lwes_net_udp:close (Socket).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Config = #config { id = Id }]) ->
  ets:new (Id,
           [ named_table,
             public,
             set,
             {keypos, 2},
             {write_concurrency, true}
           ]),
  ets:insert_new (Id, Config),
  {ok, #state { id = Id }}.

handle_call ({size}, _From, State = #state { queue = QueueIn }) ->
    {reply, queue:len (QueueIn), State};
handle_call ({out}, _From, State = #state { queue = QueueIn }) ->
    {Value, QueueOut} = queue:out (QueueIn),
    {reply, Value, State#state { queue = QueueOut }};
handle_call (Request, From, State) ->
    io:format ("~p:handle_call ~p from ~p~n",[?MODULE, Request, From]),
    {reply, ok, State}.

handle_cast ({in, Socket}, State = #state { queue = QueueIn }) ->
    {noreply, State#state { queue = queue:in (Socket, QueueIn) }};
handle_cast (Request, State) ->
    io:format ("~p:handle_cast ~p~n",[?MODULE, Request]),
    {noreply, State}.

handle_info (Info, State) ->
    io:format ("~p:handle_info ~p~n",[?MODULE,Info]),
    {noreply, State}.

terminate (_Reason, _State) ->
    ok.

code_change (_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Helper functions
%%--------------------------------------------------------------------
parse_config ([], Config) ->
  validate_config (Config);
parse_config ([{id, Id}|Rest], Config = #config{}) ->
  parse_config (Rest, Config#config {id = Id});
parse_config ([{max_age_seconds, MaxAge}|Rest], Config = #config{}) ->
  parse_config (Rest, Config#config {max_age_seconds = MaxAge});
parse_config ([{max_connections, MaxConnections}|Rest], Config = #config{}) ->
  parse_config (Rest, Config#config {max_connections = MaxConnections });
parse_config ([_|Rest], Config = #config{}) ->
  parse_config (Rest, Config).

validate_config ( Config = #config { id = Id }) ->
  % double check non-defaulted options have values
  case Id =/= undefined of
    true -> {ok, Config};
    false -> {error, bad_config}
  end.

seconds_since_epoch () ->
  {Mega, Secs, _ } = os:timestamp(),
  Mega * 1000000 + Secs.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").


-endif.
