-module (lwes_app).

-behaviour (application).

-ifdef(HAVE_EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start/0]).

%% application callbacks
-export ([start/2, stop/1]).

%%====================================================================
%% API functions
%%====================================================================
start () ->
  [application:start(App) || App <- [sasl, lwes_erlang]].

%%====================================================================
%% application callbacks
%%====================================================================
start (_Type, _Args) ->
  case lwes_sup:start_link() of
    {ok, Pid} ->
      {ok, Pid};
    Error ->
      Error
  end.

stop (_State) ->
  ok.

%%====================================================================
%% Test functions
%%====================================================================
-ifdef(EUNIT).

-endif.
