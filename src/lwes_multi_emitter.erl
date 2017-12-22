%
% This module implements an M of N emitter.  Given N different channels,
% when emit is called, the event is emitted over M of them.  You can use
% this in situations where you want to mimic multicast by emitting the
% same event to multiple places, in addition to load balancing the emission
% across a set of machines
%
-module (lwes_multi_emitter).

-include ("lwes_internal.hrl").

%% API
-export ([ new/1,
           select/1,
           emit/2,
           close/1
         ]).

%%====================================================================
%% API
%%====================================================================
new ({NumToSelect, Type, ListOfSubConfigs})
  when is_integer(NumToSelect),
       (is_atom(Type) andalso (Type =:= group orelse Type =:= random)),
       is_list(ListOfSubConfigs) ->

  Max = length(ListOfSubConfigs),
  case NumToSelect of
     _ when NumToSelect >= 1; NumToSelect > Max ->
      { ok,
        #lwes_multi_emitter {
          type = Type,
          max  = Max,
          num = NumToSelect,
          configs = [ begin {ok, Cout} = new (Config), Cout end
                      || Config <- ListOfSubConfigs ]
        }
      };
    _ ->
      { error, bad_m_value }
  end;
new (Config) ->
  {ok, lwes_net_udp:new (emitter, Config)}.

select (#lwes_multi_emitter {type = _, max = N,
                             num = N, configs = Configs}) ->
  select (Configs);
select (#lwes_multi_emitter {type = _, max = M,
                             num = N, configs = Configs}) ->
  Start = rand:uniform(M),
  Config = list_to_tuple (Configs),
  Indices = wrapped_range (Start, N, M),
  case Indices of
    [I] -> select (element(I,Config));
    _ -> [ select(element(I, Config)) || I <- Indices ]
  end;
select (A = {_,_}) ->
  A;
select (L) when is_list(L) ->
  [ select(E) || E <- L ];
select (A) ->
  lwes_net_udp:address(A).

wrapped_range (Start, Number, Max) when Start > Max ->
  wrapped_range (case Start rem Max of 0 -> Max; V -> V end, Number, Max);
wrapped_range (Start, Number, Max) ->
  wrapped_range (Start, Number, Max, []).

% determine a range of integers which wrap
wrapped_range (_, 0, _, Accumulated) ->
  lists:reverse (Accumulated);
wrapped_range (Max, Number, Max, Accumulated) ->
  wrapped_range (1, Number - 1, Max, [Max | Accumulated]);
wrapped_range (Current, Number, Max, Accumulated) ->
  wrapped_range (Current + 1, Number - 1, Max, [Current | Accumulated]).

emit (C, P) ->
  Selected = select(C),
  Packet = lwes_event:to_iolist(P),
  lwes_emitter:send (lwes_emitters, Selected, Packet).

close (#lwes_multi_emitter { configs = Configs }) ->
  [ close(C) || C <- Configs ];
close (O) ->
  lwes_stats:delete (lwes_net_udp:address(O)).


%%====================================================================
%% Internal functions
%%====================================================================

%%====================================================================
%% Test functions
%%====================================================================
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

config (basic) ->
  { "127.0.0.1", 9191 };
config (random) ->
  { 2, random,
    [ {"127.0.0.1",30000}, {"127.0.0.1",30001}, {"127.0.0.1",30002} ] };
config (group) ->
  { 3, group,
    [ {1, random, [ { "127.0.0.1",5390 }, { "127.0.0.1",5391 } ] },
      {1, random, [ { "127.0.0.1",5392 }, { "127.0.0.1",5393 } ] },
      {1, random, [ { "127.0.0.1",5394 }, { "127.0.0.1",5395 } ] }
    ]
  }.

possible_answers (basic) ->
  [ {{127,0,0,1},9191} ];
possible_answers (random) ->
  [
    [{{127,0,0,1},30000}, {{127,0,0,1},30001}],
    [{{127,0,0,1},30001}, {{127,0,0,1},30002}],
    [{{127,0,0,1},30002}, {{127,0,0,1},30000}]
  ];
possible_answers (group) ->
  [
    [{{127,0,0,1},5390}, {{127,0,0,1},5392}, {{127,0,0,1},5394}],
    [{{127,0,0,1},5390}, {{127,0,0,1},5392}, {{127,0,0,1},5395}],
    [{{127,0,0,1},5390}, {{127,0,0,1},5393}, {{127,0,0,1},5394}],
    [{{127,0,0,1},5390}, {{127,0,0,1},5393}, {{127,0,0,1},5395}],
    [{{127,0,0,1},5391}, {{127,0,0,1},5392}, {{127,0,0,1},5394}],
    [{{127,0,0,1},5391}, {{127,0,0,1},5392}, {{127,0,0,1},5395}],
    [{{127,0,0,1},5391}, {{127,0,0,1},5393}, {{127,0,0,1},5394}],
    [{{127,0,0,1},5391}, {{127,0,0,1},5393}, {{127,0,0,1},5395}]
  ].

check_selection_test_ () ->
  [
    [
      [
        begin
          {ok, C} = new(config(T)),
          ?_assert (lists:member (select(C), possible_answers(T)))
        end
        || _ <- lists:seq(1,100)
      ]
      || T <- [basic, random, group]
    ]
  ].


-endif.

