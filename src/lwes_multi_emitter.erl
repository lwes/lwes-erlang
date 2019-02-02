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
new (Config) ->
  new0 (normalize_emitters_config(Config)).

new0 ({NumToSelect, Type, ListOfSubConfigs})
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
          configs = [ begin
                        Cout =
                          case new0(Config) of
                            {ok, C} -> C;
                            C -> C
                          end,
                          Cout
                      end
                      || Config <- ListOfSubConfigs ]
        }
      };
    _ ->
      { error, bad_m_value }
  end;
new0 ({Module,Config}) when is_atom(Module), is_list(Config) ->
  Emitter = Module:new (Config),
  lwes_stats:initialize(Module:id(Emitter)),
  {Module, Emitter};
new0 (Config) ->
  Emitter = lwes_emitter_udp:new(Config),
  lwes_stats:initialize(lwes_emitter_udp:id(Emitter)),
  {lwes_emitter_udp, Emitter}.

% Normalize all emitter configuration to the form used by lwes_multi_emitter.
%
% This clause is for config of pluggable emission for a single emitter
%   {module_implementing_emitter_behaviour, Config}
normalize_emitters_config ({M, Config}) when is_atom(M) ->
  {1, group, [{M, Config}]};
% This clause is for config which has a number and a list of SubConfigs
%   {NumberToEmitTo, ListOfSubConfigs}
normalize_emitters_config ({N, L}) when is_integer(N), is_list(L) ->
  {N, group, L};
% This clause is for the simple case of a single Ip/Port so
%   {"127.0.0.1",9191}
normalize_emitters_config (C = {Ip, Port}) when is_list(Ip), ?is_uint16(Port) ->
  {1, group, [C]};
% This clause is for the simple case of a single Ip/Port so
%   {"127.0.0.1",9191, [other,config]}
normalize_emitters_config (C = {Ip, Port,Config})
  when is_list(Ip), ?is_uint16(Port), is_list(Config) ->
  {1, group, [C]};
% This is for the case where the config is actually already in the form
% the lwes_mulit_emitter recognizes
normalize_emitters_config ({NumToSelect, Type, ListOfSubConfigs})
  when is_integer(NumToSelect),
       (is_atom(Type) andalso (Type =:= group orelse Type =:= random)),
       is_list(ListOfSubConfigs) ->
  case ListOfSubConfigs of
    L = [{_,_}] ->
      {NumToSelect, Type, L};
    L = [{_,Port,Config}] when ?is_uint16(Port), is_list(Config) ->
      {NumToSelect, Type, L};
    L when is_list(L) ->
      {NumToSelect, Type,[ sub_normalize(S) || S <- ListOfSubConfigs ]}
  end.

sub_normalize (C = {Ip, Port}) when is_list(Ip), ?is_uint16(Port) ->
  C;
sub_normalize (C = {Ip, Port, Config})
  when is_list(Ip), ?is_uint16(Port), is_list(Config) ->
  C;
sub_normalize (C = {M, Config}) when is_atom(M), is_list(Config) ->
  C;
sub_normalize (C) ->
  C.

% returns a sorted list of places to emit to in the form
% [ {EmitterModule, [State0, ...]}, ... ]
select (C) ->
  collate(lists:sort(lists:flatten(select0(C)))).

select0 (#lwes_multi_emitter {type = _, max = N,
                             num = N, configs = Configs}) ->
  select0 (Configs);
select0 (#lwes_multi_emitter {type = _, max = M,
                             num = N, configs = Configs}) ->
  Start = rand:uniform(M),
  Config = list_to_tuple (Configs),
  Indices = wrapped_range (Start, N, M),
  case Indices of
    [I] -> select0(element(I, Config));
    _ -> [ select0(element(I, Config)) || I <- Indices ]
  end;
select0 (A = {_,P}) when is_integer(P) ->
  A;
select0 (L) when is_list(L) ->
  [ select0(E) || E <- L ];
select0 (A) ->
  A.

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

collate (L) ->
  collate0(L,[]).

collate0 ([], Accum) ->
  Accum;
collate0 ([{M,State}|RestIn],[]) ->
  collate0 (RestIn, [{M,[State]}]);
collate0 ([{M,State} | RestIn], [{M,StateList} | RestOut]) ->
  collate0 (RestIn, [{M,[State | StateList]} | RestOut]);
collate0 ([{M,State} | RestIn], AccumIn = [{N,_} | _]) when M =/= N ->
  collate0 (RestIn, [{M,[State]} | AccumIn]).

emit (AllEmitters, Event) ->
  SelectedEmitters = select(AllEmitters),
  % select/1 will return a list of 2-tuples of the module to use and the
  % emitter configs for that module
  lists:foreach (
    fun ({Module, Emitters}) ->
      % one callback to prep the event for the Emitters
      Packet = Module:prep (Event),

      % then loop over emitters
      lists:foreach (fun (Emitter) ->
                       Id = Module:id (Emitter),
                       case Module:emit (Emitter, Packet) of
                         ok -> lwes_stats:increment_sent (Id);
                         {error, _} -> lwes_stats:increment_errors (Id)
                       end
                     end,
                     Emitters)
    end,
    SelectedEmitters),
  ok.

close (#lwes_multi_emitter { configs = Configs }) ->
  [ close(C) || C <- Configs ];
close ({Module,Config}) ->
  lwes_stats:delete (Module:id(Config)),
  Module:close(Config).

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
  [
    [{{127,0,0,1},9191}]
  ];
possible_answers (random) ->
  [
    [{{127,0,0,1},30000}, {{127,0,0,1},30001}],
    [{{127,0,0,1},30001}, {{127,0,0,1},30002}],
    [{{127,0,0,1},30000}, {{127,0,0,1},30002}]
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

test_one(T) ->
  Config = config(T),
  {ok, C} = new(Config),
  Results =
    lists:foldl (
      fun (_, A) ->
        % jumping through a few hoops here as I changed the way select
        % worked to not return the results of getting the identifier as
        % it worked before.  So in this case we unbox the selection
        [{lwes_emitter_udp, Selected}] = select(C),
        % then sort the list of Ip/Address pairs
        Ids = lists:sort([ lwes_emitter_udp:id (S) || S <- Selected ]),
        Answers = possible_answers(T),
        % finally we check to see if we have an answer
        lists:member (Ids, Answers) and A
      end,
      true,
      lists:seq(1,100)),
  close(C),
  Results.

check_selection_test_ () ->
  { setup,
    fun() ->
      case lwes_stats:start_link() of
        {error,{already_started,_}} -> exists;
        {ok, Pid} -> Pid
      end
    end,
    fun (exists) -> ok;
        (Pid) -> unlink(Pid), gen_server:stop(Pid)
    end,
    [
      ?_assert(test_one(T))
      || T <- [basic, random, group]
    ]
  }.

normalize_test_ () ->
  [
    ?_assertEqual (Expected, normalize_emitters_config(Given))
    || {Given, Expected}
    <- [
         { {"127.0.0.1",9191},
           {1,group,[{"127.0.0.1",9191}]} },
         { {"127.0.0.1",9191,[{ttl,25}]},
           {1,group,[{"127.0.0.1",9191,[{ttl,25}]}]} },
         { {2, [{"127.0.0.1",9191},{"127.0.0.1",9292}]},
           {2,group,[{"127.0.0.1",9191},{"127.0.0.1",9292}]} },
         { {1, [{"127.0.0.1",9191},{"127.0.0.1",9292}]},
           {1,group,[{"127.0.0.1",9191},{"127.0.0.1",9292}]} },
         { {2, random, [{"127.0.0.1",30000},{"127.0.0.1",30001},{"127.0.0.1",30002}]},
           {2, random, [{"127.0.0.1",30000}, {"127.0.0.1",30001}, {"127.0.0.1",30002}]} },
         { {3, group, [{1,random,[{"127.0.0.1",5301},{"127.0.0.1",5302}]},{1,random,[{"127.0.0.1",5311},{"127.0.0.1",5312}]},{1,random,[{"127.0.0.1",5321},{"127.0.0.1",5322}]}]},
           {3, group, [{1,random,[{"127.0.0.1",5301},{"127.0.0.1",5302}]},{1,random,[{"127.0.0.1",5311},{"127.0.0.1",5312}]},{1,random,[{"127.0.0.1",5321},{"127.0.0.1",5322}]}]} },
         { {2, group, [{2, random, [{"127.0.0.1",30000},{"127.0.0.1",30001},{"127.0.0.1",30002}]},{lwes_emitter_stdout,[{label,stdout}]}]},
           {2, group, [{2, random, [{"127.0.0.1",30000},{"127.0.0.1",30001},{"127.0.0.1",30002}]},{lwes_emitter_stdout,[{label,stdout}]}]}
         }

       ]
  ].

-endif.

