-module (lwes_journal_listener).

-behaviour (gen_server).

%% API
-export ([ start_link/1,
           rescan/1
         ]).

%% gen_server callbacks
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3
         ]).

-record (state, { dirs,
                  timer,
                  interval,
                  delay,
                  last_scan,
                  callback
                } ).

%%====================================================================
%% API
%%====================================================================
start_link (Config) ->
  gen_server:start_link (?MODULE, [Config], []).

rescan (Pid) ->
  gen_server:cast (Pid, {rescan}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init ([Config]) ->
  % get appication variables
  Interval = proplists:get_value (interval, Config, 60),
  Dirs = proplists:get_value (dirs, Config, ["."]),
  CallbackFunction = proplists:get_value (callback, Config, undefined),
  Delay = Interval * 1000,

  % I want terminate to be called
  process_flag (trap_exit, true),

  % setup checking for journals
  { ok, TRef } = timer:apply_interval (Delay, ?MODULE, rescan, [self()]),

%  Scan = rescan ([],Dirs),
  { ok,
    #state {
      dirs = Dirs,
      timer = TRef,
      interval = Interval,
      delay = Delay,
      last_scan = [],
      callback = CallbackFunction
    }
  }.

handle_call (Request, From, State) ->
  error_logger:warning_msg ("Unrecognized call ~p from ~p~n",[Request, From]),
  { reply, ok, State }.

handle_cast ({rescan}, State = #state { dirs = Dirs,
                                        last_scan = LastScan,
                                        callback = CB
                                      }) ->
  NewScan = rescan (LastScan, Dirs, CB),
  { noreply, State#state { last_scan = NewScan }};
handle_cast (Request, State) ->
  error_logger:warning_msg ("Unrecognized cast ~p~n",[Request]),
  { noreply, State }.

handle_info (Request, State) ->
  error_logger:warning_msg ("Unrecognized info ~p~n",[Request]),
  {noreply, State}.

terminate (_Reason, #state {}) ->
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

rescan (LastScan, Dirs, CB) ->
  NewScan = lwes_file_watcher:scan (Dirs),
  case lwes_file_watcher:changes (LastScan, NewScan) of
    [] ->
      io:format ("No Changes!~n",[]),
      ok;
    Changes ->
      [
        case E of
          {file, added, F} ->
            case re:run (F, "[^\\d]\.(\\d+)\.(\\d+)\.(\\d+).gz",
                         [{capture, all_but_first, list}]) of
              {match, [_, _, _]} ->
                 Start = os:timestamp (),
                 process_journal (F, CB),
                 io:format ("took ~p millis to process ~p~n",
                            [millis_diff (os:timestamp(), Start), F]),
                 file:delete (F);
              nomatch ->
                 ok
            end;
          _ ->
            ok
        end
        || E
        <- Changes
      ],
      ok
  end,
  NewScan.

-define(KILO, 1000).
-define(MEGA, 1000000).
-define(GIGA, 1000000000).
-define(TERA, 1000000000000).

millis_diff ({M,S,U}, {M,S1,U1}) ->
  ((S-S1) * ?KILO) + ((U-U1) div ?KILO);
millis_diff ({M,S,U}, {M1,S1,U1}) ->
  ((M-M1)*?MEGA + (S-S1))*?KILO + ((U-U1) div ?KILO).

process_journal (File, CB) ->
  {ok, Dev} = file:open (File, [read, raw, compressed, binary]),
    case read_next (Dev, CB) of
    eof -> ok;
    E -> E
  end.

read_next (Dev, CB) ->
  case file:read (Dev, 22) of
    {ok, <<S:16/integer-unsigned-big,  % 2 length
           M:64/integer-unsigned-big,  % 8 receipttime
           V4:8/integer-unsigned-big,   % 1 ip
           V3:8/integer-unsigned-big,   % 1 ip
           V2:8/integer-unsigned-big,   % 1 ip
           V1:8/integer-unsigned-big,   % 1 ip
           P:16/integer-signed-big,    % 2 port
           _:16/integer-signed-big,    % 2 id
           0:32/integer-signed-big     % 4
         >> } ->
     case file:read (Dev, S) of
       {ok, B} ->
         CB ({udp, M, {V1, V2, V3, V4}, P, B}, ok),
         read_next (Dev, CB);
       E -> E
     end;
    E -> E
  end.
