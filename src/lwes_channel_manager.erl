-module (lwes_channel_manager).

-behaviour (gen_server).

-include_lib ("lwes.hrl").
-include_lib ("lwes_internal.hrl").

%% API
-export ([ start_link/0,
           open_channel/1,
           register_channel/2,
           unregister_channel/1,
           find_channel/1,
           close_channel/1
         ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-define (TABLE, lwes_channels).
-record (state, {}).

%%====================================================================
%% API
%%====================================================================
start_link () ->
  gen_server:start_link ( { local, ?MODULE }, ?MODULE, [], []).

open_channel (Channel) ->
  lwes_channel_sup:open_channel (Channel).

register_channel (Channel, Pid) ->
  gen_server:call (?MODULE, {reg, Channel, Pid}).

unregister_channel (Channel) ->
  gen_server:call (?MODULE, {unreg, Channel}).

find_channel (Channel) ->
  case ets:lookup (?TABLE, Channel) of
    [] -> {error, not_open} ;
    [{_Channel, Pid}] -> Pid
  end.

close_channel (Channel) ->
  gen_server:call (find_channel (Channel), stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init ([]) ->
  ets:new (?TABLE, [ named_table, { read_concurrency, true } ]),
  { ok, #state {} }.

handle_call ({reg, Key, Val}, _From, State) ->
  { reply, ets:insert (?TABLE, {Key, Val}), State };
handle_call ({unreg, Key}, _From, State) ->
  {reply, ets:delete (?TABLE, Key), State };
handle_call (Request, From, State) ->
  error_logger:warning_msg
    ("lwes_channel_manager unrecognized call ~p from ~p~n",[Request, From]),
  { reply, ok, State }.

handle_cast (Request, State) ->
  error_logger:warning_msg
    ("lwes_channel_manager unrecognized cast ~p~n",[Request]),
  { noreply, State }.

handle_info (Request, State) ->
  error_logger:warning_msg
    ("lwes_channel_manager unrecognized info ~p~n",[Request]),
  {noreply, State}.

terminate (_Reason, _State) ->
  ets:delete (?TABLE),
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%%====================================================================
%% Test functions
%%====================================================================
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

-endif.
