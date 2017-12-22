-module (lwes_stats).

% the stats module keeps some stats about what lwes is doing, and provides
% a few ways to roll those stats up.
%
% the key for most methods is one of two forms
%
% - {Label, {Ip, Port}}
% - {Ip, Port}
%
% individual stats are incremented via function calls and the current
% stats can be fetched as a proplist via fetch/1, or as a list of lists
% via rollup/1

%% API
-export([ start_link/0,
          initialize/1,            % (Id)
          increment_sent/1,        % (Id)
          increment_received/1,    % (Id)
          increment_errors/1,      % (Id)
          % these 2 are only used if validating
          increment_considered/1,  % (Id)
          increment_validated/1,   % (Id)
          delete/1,                % (Id)
          fetch/1,                 % (Id)
          rollup/1,                % 'id' | 'port' | 'none'
          print/1
        ]).

%% gen_server callbacks
-export ([init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2,
          code_change/3]).

% gen_server state
-record (state, {}).
-define (TABLE, lwes_stats).

% stats record for db
-record (stats, {id,       % Id
                 sent = 0,
                 received = 0,
                 errors = 0,
                 considered = 0,
                 validated = 0
                }).

-define (STATS_SENT_INDEX, #stats.sent).
-define (STATS_RECEIVED_INDEX, #stats.received).
-define (STATS_ERRORS_INDEX, #stats.errors).
-define (STATS_CONSIDERED_INDEX, #stats.considered).
-define (STATS_VALIDATED_INDEX, #stats.validated).

start_link () ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

initialize (Id = {_, {_,_}}) ->
  init0 (Id);
initialize (Id = {_, _}) ->
  init0 (Id);
initialize (_) ->
  erlang:error(badarg).

init0 (Id) ->
  ets:insert_new (?TABLE, #stats {id = Id}).

increment_sent (Id) ->
  [V] = try_update_counter (Id, ?STATS_SENT_INDEX, 1),
  V.

increment_received (Id) ->
  [V] = try_update_counter (Id, ?STATS_RECEIVED_INDEX, 1),
  V.

increment_errors (Id) ->
  [V] = try_update_counter (Id, ?STATS_ERRORS_INDEX, 1),
  V.

increment_considered (Id) ->
  [V] = try_update_counter (Id, ?STATS_CONSIDERED_INDEX, 1),
  V.

increment_validated (Id) ->
  [V] = try_update_counter (Id, ?STATS_VALIDATED_INDEX, 1),
  V.

update_counter (Id, Index, Amount) ->
  ets:update_counter (?TABLE, Id, [{Index,Amount}]).

try_update_counter (Id, Index, Amount) ->
  try update_counter (Id, Index, Amount) of
    V -> V
  catch
    error:badarg ->
      initialize (Id),
      update_counter (Id, Index, Amount)
  end.


delete (Id) ->
  ets:delete (?TABLE, Id).

fetch (Id) ->
  case ets:lookup (?TABLE, Id) of
    [] -> undefined;
    [#stats { sent = Sent,
              received = Received,
              errors = Errors,
              considered = Considered,
              validated = Validated }] ->
      [ {sent, Sent},
        {received, Received},
        {errors, Errors},
        {considered, Considered},
        {validated, Validated}
      ]
  end.

pivot_by_key ({Data, Keys}) ->
  lists:foldl( fun (Key, A) ->
                 [ [ Key, dict:fetch({Key,sent}, Data),
                          dict:fetch({Key,received}, Data),
                          dict:fetch({Key,errors}, Data),
                          dict:fetch({Key,considered}, Data),
                          dict:fetch({Key,validated}, Data) ] | A ]
               end,
               [],
               dict:fetch_keys(Keys)).

add_by_key (Key, S, R, E, C, V, {DataIn, KeysIn}) ->
  D1 = dict:update_counter({Key,sent},S,DataIn),
  D2 = dict:update_counter({Key,received},R,D1),
  D3 = dict:update_counter({Key,errors},E,D2),
  D4 = dict:update_counter({Key,considered},C,D3),
  D5 = dict:update_counter({Key,validated},V,D4),
  KeysOut = dict:update_counter(Key,1,KeysIn),
  {D5, KeysOut}.

% outputs the table in a two dimensional form which should allow for easy
% construction of output.
% The form is
%
% [ [ id, sent, received, errors, considered, validated],
%   ...
% ]
rollup(label) ->
  pivot_by_key(
    lists:foldl (
      fun (#stats {id = {Label,{_,_}},
                   sent = S, received = R, errors = E,
                   considered = C, validated = V}, Accum) ->
            add_by_key ({Label,{'*','*'}}, S, R, E, C, V, Accum);
          (#stats { sent = S, received = R, errors = E,
                    considered = C, validated = V}, Accum) ->
            add_by_key ({'_',{'*','*'}}, S, R, E, C, V, Accum)
      end,
      { dict:new(), dict:new() },
      ets:tab2list(?TABLE)
    )
  );
rollup(port) ->
  pivot_by_key(
    lists:foldl (
      fun (#stats {id = {_,{_,Port}},
                   sent = S, received = R, errors = E,
                   considered = C, validated = V}, Accum) ->
            add_by_key ({'*',{'*',Port}}, S, R, E, C, V, Accum);
          (#stats {id = {_, Port},
                   sent = S, received = R, errors = E,
                   considered = C, validated = V}, Accum) ->
            add_by_key ({'*',{'*',Port}}, S, R, E, C, V, Accum)
      end,
      { dict:new(), dict:new() },
      ets:tab2list(?TABLE)
    )
  );
rollup(none) ->
  [
    [ case I of {_,{_,_}} -> I ; _ -> {'*',I} end, S, R, E, C, V ]
    || #stats { id = I, sent = S, received = R,
                errors = E, considered = C, validated = V}
    <- ets:tab2list(?TABLE)
  ].

format_ip_port({'*','*'}) -> io_lib:format("*:~-5s",["*"]);
format_ip_port({'*', Port}) -> io_lib:format("*:~-5b",[Port]);
format_ip_port({Ip, Port}) -> io_lib:format ("~s:~-5b",[lwes_util:ip2bin (Ip), Port]).

print (Type) ->
  io:format ("~-21s ~-21s ~10s ~10s ~10s ~10s ~10s~n",
             ["label","channel","sent","received",
              "errors","considered","validated"]),
  io:format ("~-21s ~-21s ~10s ~10s ~10s ~10s ~10s~n",
             ["---------------------",
              "---------------------",
              "----------","----------",
              "----------","----------","----------"]),
  [
    io:format ("~-21w ~-21s ~10b ~10b ~10b ~10b ~10b~n",
               [Label, format_ip_port (IpPort),
                Sent, Recv, Err, Cons, Valid]
              )
    || [ {Label, IpPort = {_,_}}, Sent, Recv, Err, Cons, Valid ]
    <- rollup(Type)
  ],
  ok.

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([]) ->
  ets:new (?TABLE,
           [ named_table,
             public,
             set,
             {keypos, 2},
             {write_concurrency, true}
           ]),
  {ok, #state { }}.

handle_call (_Request, _From, State) ->
  {reply, ok, State}.

handle_cast (_Request, State) ->
  {noreply, State}.

handle_info (_Info, State) ->
  {noreply, State}.

terminate (_Reason, _State) ->
  ets:delete (?TABLE),
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

setup () ->
  case start_link() of
    {ok, Pid} -> Pid;
    {error, {already_started, _}} -> already_started
  end.

cleanup (already_started) -> ok;
cleanup (Pid) -> gen_server:stop (Pid).

fetch_list (Sent, Received, Errors, Considered, Validated) ->
  [ {sent, Sent},
    {received, Received},
    {errors, Errors},
    {considered, Considered},
    {validated, Validated}
  ].

check_entry ([[_,Sent,Recv,Errors,Considered,Validated]],
             [[_,Sent,Recv,Errors,Considered,Validated]]) ->
  true.

tests_with_id (Id) ->
  [
    ?_assertEqual(true, initialize(Id)),
    % initialization only works once
    ?_assertEqual(false, initialize(Id)),
    % rollups should show all zeros
    ?_assert(check_entry([[dummy,0,0,0,0,0]], rollup(port))),
    ?_assert(check_entry([[dummy,0,0,0,0,0]], rollup(label))),
    ?_assert(check_entry([[dummy,0,0,0,0,0]], rollup(none))),
    % increment all stats and check counts
    ?_assertEqual(fetch_list(0,0,0,0,0), fetch(Id)),
    ?_assertEqual(1, increment_sent(Id)),
    ?_assertEqual(fetch_list(1,0,0,0,0), fetch(Id)),
    ?_assertEqual(1, increment_received(Id)),
    ?_assertEqual(fetch_list(1,1,0,0,0), fetch(Id)),
    ?_assertEqual(2, increment_received(Id)),
    ?_assertEqual(fetch_list(1,2,0,0,0), fetch(Id)),
    ?_assertEqual(1, increment_errors(Id)),
    ?_assertEqual(fetch_list(1,2,1,0,0), fetch(Id)),
    ?_assertEqual(1, increment_considered(Id)),
    ?_assertEqual(fetch_list(1,2,1,1,0), fetch(Id)),
    ?_assertEqual(1, increment_validated(Id)),
    ?_assertEqual(fetch_list(1,2,1,1,1), fetch(Id)),
    % see that the rollups reflect the same values
    ?_assert(check_entry([[dummy,1,2,1,1,1]], rollup(port))),
    ?_assert(check_entry([[dummy,1,2,1,1,1]], rollup(label))),
    ?_assert(check_entry([[dummy,1,2,1,1,1]], rollup(none))),
    % delete the test ids
    ?_assertEqual(true, delete(Id)),
    ?_assertEqual(true, delete(Id))
  ].


lwes_stats_test_ () ->
  IdWithLabel = {foo,{{127,0,0,1},9191}},
  IdNoLabel = {{127,0,0,1},9191},
  BadId = undefined,
  NonExistentId = {foo, bar,{{127,0,0,1},9292}},
  { setup,
    fun setup/0,
    fun cleanup/1,
    [
      % tests with different types of ids
      tests_with_id (IdWithLabel),
      tests_with_id (IdNoLabel),
      % test a few cases with bad ids
      ?_assertEqual(undefined, fetch(BadId)),
      ?_assertEqual(undefined, fetch(NonExistentId)),
      ?_assertException(error, badarg, increment_sent(NonExistentId)),
      % these are for additional coverage on the gen_server calls which
      % are not currently used for anything
      ?_assertEqual({reply, ok, #state{}}, handle_call(foo, bar, #state{})),
      ?_assertEqual({noreply, #state{}}, handle_cast(foo, #state{})),
      ?_assertEqual({noreply, #state{}}, handle_info(foo, #state{})),
      ?_assertEqual({ok, #state{}}, code_change(foo, #state{}, bar)),
      % this just tests the case where we might be running inside of a
      % larger process, so mostly just here for coverage
      ?_assertEqual(ok, cleanup(setup()))
    ]
  }.

lwes_stats_rollups_test_ () ->
  Id1 = {{127,0,0,1},9191},
  Id2 = {{127,0,0,1},9192},
  Id3 = {foo, Id1},
  Id4 = {foo, Id2},
  LabelRollupId1 = {foo,{'*','*'}},
  LabelRollupId2 = {'_',{'*','*'}},
  PortRollupId1 = {'*',{'*',9191}},
  PortRollupId2 = {'*',{'*',9192}},
  { setup,
    fun setup/0,
    fun cleanup/1,
    { inorder,
      [
        [ ?_assertEqual(true, initialize(I)) || I <- [ Id1, Id2, Id3, Id4 ] ],
        ?_assertEqual(1, increment_sent(Id1)),
        ?_assertEqual(1, increment_sent(Id4)),
        ?_assertEqual(1, increment_received(Id2)),
        ?_assertEqual(1, increment_received(Id3)),
        ?_assertEqual(1, increment_errors(Id3)),
        ?_assertEqual(1, increment_errors(Id4)),
        % check no rollups
        ?_assertEqual(lists:sort([[{'*',Id1},1,0,0,0,0],
                                  [{'*',Id2},0,1,0,0,0],
                                  [Id3,0,1,1,0,0],
                                  [Id4,1,0,1,0,0]]),
                      lists:sort(rollup(none))),
        % check rollup by label
        ?_assertEqual(lists:sort([[LabelRollupId1,1,1,2,0,0],
                                  [LabelRollupId2,1,1,0,0,0]]),
                      lists:sort(rollup(label))),
        % check rollup by port
        ?_assertEqual(lists:sort([[PortRollupId1,1,1,1,0,0],
                                  [PortRollupId2,1,1,1,0,0]]),
                      lists:sort(rollup(port)))
      ]
    }
  }.

-endif.
