-module(lwes_esf_validator).

-behaviour(gen_server).

%% API
-export([start_link/0,
         add_esf/2,
         validate/2,
         stats/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include_lib ("lwes.hrl").

-define (SPEC_TAB, lwes_esf_specs).

-define(LEXER, lwes_esf_lexer).
-define(PARSER, lwes_esf_parser).

-define(META_EVENT, "MetaEventInfo").
-define(STATS_KEY, stats).

%%====================================================================
%% API
%%====================================================================
start_link () ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_esf (ESFName, ESFFile) ->
  Events = parse (file, ESFFile),
  add_esf_events(ESFName, Events).

add_esf_events(ESFName, Events) ->
  % if 'MetaEventInfo' is defined we need to merge the attribute
  %specification from the 'Meta Event' to the specification of
  % all other 'real' events
  Events1 = case lists:keyfind (?META_EVENT, 2, Events) of
     false -> Events;
     MetaEvent ->
       RealEvents = lists:delete (MetaEvent, Events),
       {event, _, MetaAttrs} = MetaEvent,
       [{event, Name, Attrs ++ MetaAttrs } || {event, Name, Attrs} <- RealEvents]
  end,
  lists:foreach (fun (E) -> add_event (ESFName, E) end, Events1).

validate (ESFName, #lwes_event {name = EventName, attrs = Attrs} = _Event) ->
  ets:update_counter (?SPEC_TAB, ?STATS_KEY, {2,1}),

  EventName1 = lwes_util:any_to_list (EventName),
  Key = { ESFName, EventName1 },

  case ets:lookup (?SPEC_TAB, Key) of
    [{_, EventSpec}] ->
        validate_event (EventName1, EventSpec, Attrs);
    _ -> error_logger:warning_msg ("event ~p is not defined in ESF!",
                                   [EventName1]),
         false
  end.

stats () ->
  [Stats] = ets:lookup (?SPEC_TAB, ?STATS_KEY),
  Stats.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init ([]) ->
  % make sure terminate is called
  process_flag (trap_exit, true),

  ets:new (?SPEC_TAB, [set, public, named_table, {keypos, 1}]),
  ets:insert (?SPEC_TAB,{?STATS_KEY, 0, 0}),
  { ok, {} }.

handle_call ({state}, _From, State) ->
  {reply, State, State }.

handle_cast (_Msg, State) ->
  { noreply, State }.

handle_info (_Info, State) ->
  { noreply, State }.

terminate (_Reason, _State) ->
  ets:delete (?SPEC_TAB),
  ok.

code_change (_OldVsn, State, _Extra) ->
  { ok, State }.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

add_event (ESFName, Event) ->
  { event, EventName, Attributes } = Event,
  Attributes1 = [ A || {attribute, A} <- Attributes ],
  { RequiredAttrs, OptionalAttrs } =
    lists:partition(
       fun ({_, _, _, Qualifier}) -> Qualifier == required end,
       Attributes1),
  ets:insert (?SPEC_TAB,
             {{ ESFName, EventName }, { RequiredAttrs, OptionalAttrs }}).

validate_event (EventName, { RequiredSpec, OptionalSpec }, EventAttrs) ->
  case validate_unique (EventName, EventAttrs, []) of 
    ok ->
      case validate_required (EventName, RequiredSpec, EventAttrs) of
        {ok, OptionalAttrs} ->
               case validate_optional (EventName, OptionalSpec, OptionalAttrs) of
                 ok -> ets:update_counter (?SPEC_TAB, ?STATS_KEY, {3,1}),
                         ok;
                 ErrorOpt -> ErrorOpt
               end;
        ErrorReq -> ErrorReq
      end;
    ErrorUnique -> ErrorUnique
  end.


validate_unique (_, [], _) -> ok;
validate_unique (EventName, [{_, AttrName, _} | T], AttrsFound) -> 
  case lists:keyfind (AttrName, 1, AttrsFound) of
    false -> validate_unique(EventName, T, [{AttrName} | AttrsFound]);
    _ -> 
       error_logger:warning_msg("'duplicate' field ~s in event ~p",
             [AttrName, EventName]),
       {field_duplicate, AttrName, EventName}
  end.

validate_required (_, [], EventAttrs) -> {ok, EventAttrs};

validate_required (EventName, [{_Type_S,
                   AttributeName_S, _, _} = H| T], EventAttrs) ->
  case lists:keyfind (AttributeName_S, 2, EventAttrs) of
     false ->
       error_logger:warning_msg("'required' field ~s is missing from event ~p",
             [AttributeName_S, EventName]),
       {field_missing, AttributeName_S, EventName};
     Attr ->
       case validate_attribute (EventName, H, Attr) of
         ok ->
           EventAttrs1 = lists:delete (Attr, EventAttrs),
           validate_required (EventName, T, EventAttrs1);
         Error -> Error
        end
  end.

validate_optional (_, _OptionalSpec, []) -> ok;

validate_optional (EventName, OptionalSpec, [{_Type, AttrName, _} = H | T]) ->
    case lists:keyfind (AttrName, 2, OptionalSpec) of
       false -> 
         error_logger:warning_msg("'unexpected' field ~s was found in event ~p",
               [AttrName, EventName]),
         {field_undefined, AttrName, EventName};
       Spec ->
         case validate_attribute (EventName, Spec, H) of
           ok -> validate_optional (EventName, OptionalSpec, T);
           Error -> Error
          end
    end.

validate_attribute (EventName, {Type_S, AttrName_S, _, _}, {Type, AttrName, _}) ->
    case AttrName_S == AttrName andalso Type_S == Type of
        false ->
           error_logger:warning_msg(
              "field ~p has wrong type ~p in event ~p! Expected type : ~p",
              [binary_to_list(AttrName_S), Type, EventName, Type_S]),
           {field_type_mismatch, AttrName, EventName, Type, Type_S};
        _ -> ok
    end.

parse (file, FileName) ->
    { ok, InFile } = file:open(FileName, [read]),
    Acc = loop (InFile, []),
    file:close (InFile),
    {ok, ParseTree} = ?PARSER:parse (Acc),
    ParseTree.

loop (InFile, Acc) ->
    case io:request(InFile, { get_until, prompt, ?LEXER, token, [1] } ) of
        { ok, Toks, _EndLine } ->
            loop (InFile,Acc ++ [Toks]);
        { error, token } ->
            error_logger:error_msg ("failed to read ESF file"),
            Acc;
        { eof, _ } ->
            Acc
    end.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


parse_string (String) ->
    {ok, Tokens, _EndLine} = ?LEXER:string (String),
    {ok, ParseTree} = ?PARSER:parse ( Tokens ),
    ParseTree.

validate_test_() ->
  EventDefs = parse_string(
     "TestEvent {
        required int32 i = 1;
        required boolean j;
        int32 k = 1;
        string l;
      }"
  ),
  Validate = fun (Attrs) ->
    try
      ets:delete (?SPEC_TAB)
    catch _:_ -> 0 end, 
    ets:new (?SPEC_TAB, [set, public, named_table, {keypos, 1}]),
    ets:insert (?SPEC_TAB,{?STATS_KEY, 0, 0}),
    add_esf_events(test_esf, EventDefs),
    Valid = validate(test_esf, #lwes_event {name = <<"TestEvent">>, attrs = Attrs}),
    ets:delete (?SPEC_TAB),
    Valid
  end,
  [
    ?_assertEqual(ok, % all fields
      Validate([{int32,<<"i">>,314159},
       {boolean,<<"j">>,false},
       {int32,<<"k">>,654321},
       {string,<<"l">>,<<"foo">>}])
    ),
    % NOTE: Default values aren't patched in anywhere currently.
    %   So, let this fail validation for now.
    %?assertEqual(true, % missing required field with default
    %  Validate([ {boolean,<<"j">>,false},
    %   {int32,<<"k">>,654321},
    %   {string,<<"l">>,<<"foo">>}])
    %),
    ?_assertEqual({field_missing, <<"j">>, "TestEvent"}, 
      % missing required field (no default)
      Validate([{int32,<<"i">>,314159},
       {int32,<<"k">>,654321},
       {string,<<"l">>,<<"foo">>}])
    ),
    ?_assertEqual({field_duplicate, <<"j">>, "TestEvent"}, 
      % duplicate field
      Validate([{int32,<<"i">>,314159},
       {boolean,<<"j">>,false},
       {boolean,<<"j">>,false},
       {int32,<<"k">>,654321},
       {string,<<"l">>,<<"foo">>}])
    ),
    ?_assertEqual({field_type_mismatch, <<"i">>, "TestEvent", boolean, int32},
      % type error, required field
      Validate([{boolean,<<"i">>,true},
       {boolean,<<"j">>,false},
       {int32,<<"k">>,654321},
       {string,<<"l">>,<<"foo">>}])
    ),
    ?_assertEqual({field_type_mismatch, <<"k">>, "TestEvent", string, int32},
      % type error, optional field
      Validate([{int32,<<"i">>,314159},
       {boolean,<<"j">>,false},
       {string,<<"k">>,<<"blah">>},
       {string,<<"l">>,<<"foo">>}])
    ),
    ?_assertEqual({field_undefined, <<"extra">>, "TestEvent"},
      % extra undefined field
      Validate([{int32,<<"i">>,314159},
       {boolean,<<"j">>,false},
       {int32,<<"k">>,654321},
       {int32,<<"extra">>,12345},
       {string,<<"l">>,<<"foo">>}])
    )
  ].

parse_test_() ->
  TestCases = [
                {
                 [{event,"A",
                    [{attribute, {int32, <<"i">>, undefined, undefined}}]}],
                 "A
                  {
                    int32 i;
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {int32, <<"i">>, undefined, undefined}},
                     {attribute, {uint32, <<"j">>, undefined, undefined}}]}],
                 "A
                  {
                    int32 i;
                    uint32 j;
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {int32, <<"i">>, undefined, undefined}},
                     {attribute, {uint32, <<"j">>, undefined, undefined}}]}],
                 "A
                  {
                    int32 i; # comments should be ignored
                    uint32 j; # comments should be ignored
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {int32, <<"i">>, undefined, undefined}},
                     {attribute, {uint32, <<"j">>, undefined, undefined}}]}],
                 "
                  # comments should be ignored
                  # like this one
                  A
                  {
                    int32 i;
                    uint32 j;
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {int32, <<"i">>, "1", undefined}},
                     {attribute, {uint32, <<"j">>, undefined, undefined}}]}],
                 "
                  # comments should be ignored
                  # like this one
                  A
                  {
                    int32 i = 1; # we can set defaults
                    uint32 j;
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {int32, <<"i">>, "-1", undefined}},
                     {attribute, {uint32, <<"j">>, undefined, undefined}}]}],
                 "
                  A
                  {
                    int32 i = -1; # we can set negative defaults
                    uint32 j;
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {float, <<"i">>, "-0.1234", undefined}},
                     {attribute, {float, <<"j">>, "12.8999", undefined}}]}],
                 "
                  A
                  {
                    float i = -0.1234; # we can set negative defaults for float
                    float j = 12.8999;
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {double, <<"i">>, "-0.1234", undefined}},
                     {attribute, {double, <<"j">>, "12.8999", undefined}}]}],
                 "
                  A
                  {
                    double i = -0.1234; # we can set negative defaults for double
                    double j = 12.8999;
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {boolean, <<"i">>, "true", undefined}},
                     {attribute, {boolean, <<"j">>, "false", undefined}}]}],
                 "
                  A
                  {
                    boolean i = true; # we can set defaults for boolean
                    boolean j = false;
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {string, <<"i">>, "\"hello\"", undefined}},
                     {attribute, {string, <<"j">>, "\"hello\nworld\"", undefined}}]}],
                 "
                  A
                  {
                    string i = \"hello\"; # we can set defaults for string
                    string j = \"hello\nworld\";
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {uint32, <<"i">>, "1", optional}},
                     {attribute, {int32, <<"j">>, "-1", nullable}},
                     {attribute, {uint32, <<"k">>, "2", required}}]}],
                 "
                  A
                  {
                    optional uint32 i = 1; # make this optional
                    nullable int32 j = -1; # nullable
                    required uint32 k = 2; # required
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {byte, <<"i">>, "1",undefined}}]}],
                 "
                  A
                  {
                    byte i = 1; # define 'byte' attribute
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {nullable_int32_array, <<"i">>, undefined, nullable}}]}],
                 "
                  A
                  {
                    nullable int32 i[16]; # define array attribute
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {int32_array, <<"i">>, undefined, undefined}}]}],
                 "
                  A
                  {
                    int32 i[16]; # define array attribute
                  }
                 "
                },
                {
                 [{event,"A",
                    [{attribute, {int32_array, <<"i">>, undefined, required}}]}],
                 "
                  A
                  {
                    required int32 i[16]; # define array attribute
                  }
                 "
                }
              ],
  [ ?_assertEqual (E, parse_string(TC)) || {E, TC} <- TestCases ].

-endif.
