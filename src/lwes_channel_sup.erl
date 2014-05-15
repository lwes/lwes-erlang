-module (lwes_channel_sup).

-behaviour (supervisor).

%% API
-export ([ start_link/0,
           open_channel/1 ]).

%% supervisor callbacks
-export ([ init/1 ]).

%%====================================================================
%% API functions
%%====================================================================
%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

open_channel (Channel) ->
  supervisor:start_child (?MODULE, [Channel]).

%%====================================================================
%% supervisor callbacks
%%====================================================================
%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
  { ok,
    {
      {simple_one_for_one, 10, 10},
      [ { lwes_channel,
          {lwes_channel, start_link, []},
          transient,
          2000,
          worker,
          [lwes_channel]
        }
      ]
    }
  }.

%%====================================================================
%% Test functions
%%====================================================================
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

-endif.
