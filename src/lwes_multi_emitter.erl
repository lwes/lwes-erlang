%
% This module implements an M of N emitter.  Given N different channels,
% when emit is called, the event is emitted over M of them.  You can use
% this in situations where you want to mimic multicast by emitting the
% same event to multiple places, in addition to load balancing the emission
% across a set of machines
%
-module (lwes_multi_emitter).

-include ("lwes_internal.hrl").

-ifdef(HAVE_EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export ([ open/1,
           emit/2,
           close/1
         ]).

%%====================================================================
%% API
%%====================================================================
open ({M, N}) ->
  open ({M, queue, N});
open ({M, group, N}) ->
   case M of
    _ when is_integer (M), M >= 1, M =:= length (N) ->
      {ok,
        #lwes_multi_emitter { type = group, m  = M,
          n = [ begin {ok, Cout} = open (C), Cout end || C <- N] }};
    _ ->
      { error, bad_m_value }
  end;
open ({M, Type, N}) ->
  case M of
    _ when is_integer (M), M >= 1 ->
      case open_emitters (N) of
        {error, Error} ->
          {error, Error};
        E ->
          Combos = allm (M, E),
          case Type of
            queue ->
              MyN = queue:from_list (Combos),
              { ok, #lwes_multi_emitter { type = queue, m  = M, n = MyN } };
            random ->
              MyN = list_to_tuple (Combos),
              {ok, #lwes_multi_emitter { type = random, m = M, n = MyN }}
          end
      end;
    _ ->
      { error, bad_m_value }
  end.

emit (Emitters = #lwes_multi_emitter { type = group, n = NIn }, Bin) ->
  Emitters#lwes_multi_emitter { n = [ emit (N, Bin) || N <- NIn ] };
emit (Emitters = #lwes_multi_emitter { type = random,  n = NIn }, Bin) ->
  % get list to emit to as a random entry from list
  Index = crypto:rand_uniform (1, tuple_size (NIn) + 1),
  ToEmitTo = element (Index, NIn),

  % emit event to each
  lists:foreach (fun (E) ->
                   lwes_channel:send_to (E, Bin)
                 end, ToEmitTo),
  Emitters;

emit (Emitters = #lwes_multi_emitter { type = queue,  n = NIn }, Bin) ->
  % get list to emit to from queue
  {{value, ToEmitTo},NTmp} = queue:out (NIn),

  % emit event to each
  lists:foreach (fun (E) ->
                   lwes_channel:send_to (E, Bin)
                 end, ToEmitTo),

  % put back into queue
  NOut = queue:in (ToEmitTo, NTmp),

  Emitters#lwes_multi_emitter { n = NOut }.

close (#lwes_multi_emitter { type = group, n = N }) ->
  [ close (E) || E <- N ],
  ok;
close (#lwes_multi_emitter { type = random, n = N }) ->
  close_emitters (lists:usort(lists:flatten(tuple_to_list (N)))),
  ok;
close (#lwes_multi_emitter { type = queue, n = N }) ->
  close_emitters (lists:usort(lists:flatten(queue:to_list(N)))),
  ok.

%%====================================================================
%% Internal functions
%%====================================================================
open_emitters (EmittersConfig) ->
  open_emitters (EmittersConfig, []).

open_emitters ([], E) ->
  E;
open_emitters ([Config | R], E) ->
  case lwes:open (emitter, Config) of
    {error, Error} ->
      close_emitters (E),
      {error, Error};
    {ok, Emitter} ->
      open_emitters (R, [Emitter | E])
  end.

close_emitters (Emitters) ->
  lists:map (fun (E) -> lwes:close (E) end, Emitters).

% get next m values from queue, then put back m-1 values
nextm (M, Q) ->
  % get first M elements from Q
  {FQ, RQ} = queue:split (M, Q),

  % save as the list we will return
  O = queue:to_list (FQ),

  % join the queue back together
  TQ = queue:join (FQ, RQ),

  % take the first value from the queue
  {{value, H},OQ} = queue:out(TQ),

  % put in back at the end
  NQ = queue:in (H, OQ),
  {O, NQ}.

% find all m sets of values in a list of values
allm (M, N) ->
  Q = queue:from_list (N),
  {_,Out} =
    lists:foldl (fun (_, {QI,A}) ->
                   {AO, QO} = nextm (M, QI),
                   {QO, [AO|A]}
                 end,
                 {Q, []},
                 N),
  lists:reverse (Out).

%%====================================================================
%% Test functions
%%====================================================================
-ifdef(EUNIT).

-endif.
