%
% This module implements an lwes journaller.
%
% configuration is as follows
%
% [ { root, "." },                      % journal root
%   { name, "all_events.log" },         % journal name
%   { interval, <rotation_interval> },  % interval for jouirnal file rotation
% ]

-module (lwes_journaller).

-behaviour (gen_server).

-include_lib ("lwes_internal.hrl").

%% API
-export ([ start_link/1,
           process_event/2,
           rotate/1,
           format_header/5 ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-record (state, {
                  journal_root,
                  journal_file_name,
                  journal_file_ext,
                  journal_current,
                  journal_last_rotate,
                  timer
                }).

%%====================================================================
%% API
%%====================================================================
start_link (Config) ->
  gen_server:start_link ( ?MODULE, [Config], []).

process_event (Event, Pid) ->
  gen_server:cast (Pid, {process, Event}),
  Pid.

rotate (Pid) ->
  gen_server:cast (Pid, {rotate}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init ([Config]) ->
  % get appication variables
  Root = proplists:get_value (root, Config, "."),
  Name = proplists:get_value (name, Config, "all_events.log"),
  Interval = proplists:get_value (interval, Config, 60),
  Ext = "gz",

  % I want terminate to be called
  process_flag (trap_exit, true),

  % open journal file
  { ok, File } = open (Root, Name, Ext),

  % setup rotation of journal
  { ok, TRef } =
    timer:apply_interval (Interval * 1000, ?MODULE, rotate, [self()]),

  { ok, #state {
          journal_root        = Root,
          journal_file_name   = Name,
          journal_file_ext    = Ext,
          journal_current     = File,
          journal_last_rotate = seconds_since_epoch (),
          timer               = TRef
        }
  }.

handle_call (Request, From, State) ->
  error_logger:warning_msg ("Unrecognized call ~p from ~p~n",[Request, From]),
  { reply, ok, State }.

format_header (EventSize, MillisTimestamp, Ip = {Ip1,Ip2,Ip3,Ip4}, Port, SiteId)
  when ?is_uint16(EventSize), ?is_uint64(MillisTimestamp), ?is_ip_addr (Ip),
       ?is_uint16(Port), ?is_uint16(SiteId) ->
  <<EventSize:16/integer-unsigned-big,        % 2 bytes
    MillisTimestamp:64/integer-unsigned-big,  % 8 bytes
    Ip4:8/integer-unsigned-big,               % 1 byte
    Ip3:8/integer-unsigned-big,               % 1 byte
    Ip2:8/integer-unsigned-big,               % 1 byte
    Ip1:8/integer-unsigned-big,               % 1 byte
    Port:16/integer-unsigned-big,             % 2 bytes
    SiteId:16/integer-unsigned-big,           % 2 bytes
    0:32/integer-signed-big                   % 4 bytes
  >>.                                         % 22 bytes total

handle_cast ( {process, {udp, _, Ip, Port, B}},
              State = #state { journal_current = Journal }) ->
  S = byte_size (B),
  M = milliseconds_since_epoch (),
  SiteId = 1,
  ok = file:write ( Journal,
                    [ format_header(S, M, Ip, Port, SiteId),
                      B
                    ]),
  { noreply, State };
handle_cast ( {rotate}, State = #state {
                                  journal_root = Root,
                                  journal_file_name = Name,
                                  journal_file_ext  = Ext,
                                  journal_current = File,
                                  journal_last_rotate = LastRotate
                                }) ->
  file:close (File),
  rename (Root, Name, Ext, LastRotate),
  {ok, NewFile} = open (Root, Name, Ext),
  { noreply, State#state { journal_current = NewFile,
                           journal_last_rotate = seconds_since_epoch () }};
handle_cast (Request, State) ->
  error_logger:warning_msg ("Unrecognized cast ~p~n",[Request]),
  { noreply, State }.

handle_info (Request, State) ->
  error_logger:warning_msg ("Unrecognized info ~p~n",[Request]),
  {noreply, State}.

terminate (_Reason, #state {
                      journal_root = Root,
                      journal_file_name = Name,
                      journal_file_ext  = Ext,
                      journal_current = File,
                      journal_last_rotate = LastRotate
                    }) ->
  file:close (File),
  rename (Root, Name, Ext, LastRotate),
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

%%====================================================================
%% Internal
%%====================================================================
open (Root, Name, Ext) ->
  JournalFile = filename:join ([Root, string:join ([Name, Ext],".")]),
  file:open (JournalFile, [ write, raw, compressed ]).

rename (Root, Name, Ext, LastRotate) ->
  {{Year,Month,Day},{Hour,Minute,Second}} =
    calendar:now_to_universal_time(os:timestamp()),
  NewFile =
    filename:join
      ([Root,
        io_lib:format
          ("~s.~4.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B.~b.~b.~s",
            [ Name, Year, Month, Day, Hour, Minute, Second, LastRotate,
              seconds_since_epoch(), Ext])]),
  CurrentFile = filename:join ([Root, string:join ([Name, Ext],".")]),
  error_logger:info_msg("rename ~p -> ~p",[CurrentFile, NewFile]),
  ok = file:rename (CurrentFile, NewFile).

milliseconds_since_epoch () ->
  {Meg, Sec, Mic} = os:timestamp(),
  trunc (Meg * 1000000000 + Sec * 1000 + Mic / 1000).

seconds_since_epoch () ->
  {M, S, _ } = os:timestamp(),
  M*1000000+S.

%%====================================================================
%% Test functions
%%====================================================================
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

-endif.
