-module (lwes_sup).

-behaviour (supervisor).

%% API
-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

%-=====================================================================-
%-                                API                                  -
%-=====================================================================-
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%-=====================================================================-
%-                        supervisor callbacks                         -
%-=====================================================================-
init([]) ->
  { ok,
    {
      { one_for_one, 10, 10 },
      [
        { lwes_channel_manager,                    % child spec id
          { lwes_channel_manager, start_link, [] },% child function to call
          permanent,                               % always restart
          2000,                                    % time to wait for child stop
          worker,                                  % type of child
          [ lwes_channel_manager ]                 % modules used by child
        },
        {
          lwes_channel_sup,                        % child spec id
          { lwes_channel_sup, start_link, []},     % child function to call
          permanent,                               % always restart
          2000,                                    % time to wait for child stop
          supervisor,                              % type of child
          [lwes_channel_sup]                       % modules used by child
        }
      ]
    }
  }.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-


%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

lwes_sup_test_ () ->
  { setup,
    fun () ->
      {ok, Pid} = start_link(),
      Pid
    end,
    fun (Pid) ->
      exit (Pid, normal)
    end,
    {
      inorder,
      [
        ?_assertEqual (true, true)
      ]
    }
  }.

-endif.
