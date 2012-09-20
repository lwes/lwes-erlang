-module (lwes_event).

-include_lib ("lwes.hrl").
-include_lib ("lwes_internal.hrl").

-ifdef(HAVE_EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([new/1,
         set_uint16/3,
         set_int16/3,
         set_uint32/3,
         set_int32/3,
         set_uint64/3,
         set_int64/3,
         set_string/3,
         set_ip_addr/3,
         set_boolean/3,
         to_binary/1,
         from_udp_packet/2,
         from_binary/2,
         peek_name_from_udp/1]).

%%====================================================================
%% API
%%====================================================================
new (Name) when is_atom (Name) ->
  new (atom_to_list (Name));
new (Name) ->
  % assume the user will be setting values with calls below, so set attrs
  % to an empty list
  #lwes_event { name = Name, attrs = [] }.

set_int16 (E = #lwes_event { attrs = A }, K, V) when ?is_int16 (V) ->
  E#lwes_event { attrs = [ { ?LWES_INT_16, K, V } | A ] };
set_int16 (_,_,_) ->
  erlang:error(badarg).
set_uint16 (E = #lwes_event { attrs = A }, K, V) when  ?is_uint16 (V) ->
  E#lwes_event { attrs = [ { ?LWES_U_INT_16, K, V } | A ] };
set_uint16 (_,_,_) ->
  erlang:error(badarg).
set_int32 (E = #lwes_event { attrs = A}, K, V) when ?is_int32 (V) ->
  E#lwes_event { attrs = [ { ?LWES_INT_32, K, V } | A ] };
set_int32 (_,_,_) ->
  erlang:error(badarg).
set_uint32 (E = #lwes_event { attrs = A}, K, V) when ?is_uint32 (V)  ->
  E#lwes_event { attrs = [ { ?LWES_U_INT_32, K, V } | A ] };
set_uint32 (_,_,_) ->
  erlang:error(badarg).
set_int64 (E = #lwes_event { attrs = A}, K, V) when ?is_int64 (V) ->
  E#lwes_event { attrs = [ { ?LWES_INT_64, K, V } | A ] };
set_int64 (_,_,_) ->
  erlang:error(badarg).
set_uint64 (E = #lwes_event { attrs = A}, K, V) when ?is_uint64 (V) ->
  E#lwes_event { attrs = [ { ?LWES_U_INT_64, K, V } | A ] };
set_uint64 (_,_,_) ->
  erlang:error(badarg).
set_boolean (E = #lwes_event { attrs = A}, K, V) when is_boolean (V) ->
  E#lwes_event { attrs = [ { ?LWES_BOOLEAN, K, V } | A ] };
set_boolean (_,_,_) ->
  erlang:error(badarg).
set_string (E = #lwes_event { attrs = A}, K, V) when ?is_string (V) ->
  E#lwes_event { attrs = [ { ?LWES_STRING, K, V } | A ] };
set_string (_,_,_) ->
  erlang:error(badarg).
set_ip_addr (E = #lwes_event { attrs = A}, K, V) ->
  Ip = lwes_util:normalize_ip (V),
  E#lwes_event { attrs = [ { ?LWES_IP_ADDR, K, Ip } | A ] };
set_ip_addr (_,_,_) ->
  erlang:error(badarg).

to_binary (Event = #lwes_event { name = EventName, attrs = Attrs }) ->
  case Attrs of
    Dict when is_tuple (Attrs) andalso element (1, Attrs) =:= dict ->
      to_binary (Event#lwes_event { attrs = dict:to_list (Dict) });
    A  ->
      NumAttrs = length (A),
      iolist_to_binary (
        [ write_name (EventName),
          <<NumAttrs:16/integer-unsigned-big>>,
          write_attrs (A, [])
        ]
      )
  end.

peek_name_from_udp ({ udp, _, _, _, Packet }) ->
  { EventName, _ } = read_name (Packet),
  EventName.

from_udp_packet ({ udp, _Socket, SenderIP, SenderPort, Packet }, Format) ->
  Extra =
    case Format of
      tagged ->
        [ { ?LWES_IP_ADDR,  <<"SenderIP">>,   SenderIP },
          { ?LWES_U_INT_16, <<"SenderPort">>, SenderPort },
          { ?LWES_INT_64,   <<"ReceiptTime">>, millisecond_since_epoch () } ];
      dict ->
        dict:from_list (
          [ { <<"SenderIP">>,   SenderIP },
            { <<"SenderPort">>, SenderPort },
            { <<"ReceiptTime">>, millisecond_since_epoch () } ]);
      json ->
        [ { <<"SenderIP">>,   ip2bin (SenderIP) },
          { <<"SenderPort">>, SenderPort },
          { <<"ReceiptTime">>, millisecond_since_epoch () } ];
      json_proplist ->
        [ { <<"SenderIP">>,   ip2bin (SenderIP) },
          { <<"SenderPort">>, SenderPort },
          { <<"ReceiptTime">>, millisecond_since_epoch () } ];
      json_eep18 ->
        [ { <<"SenderIP">>,   ip2bin (SenderIP) },
          { <<"SenderPort">>, SenderPort },
          { <<"ReceiptTime">>, millisecond_since_epoch () } ];
      _ ->
        [ { <<"SenderIP">>,   SenderIP },
          { <<"SenderPort">>, SenderPort },
          { <<"ReceiptTime">>, millisecond_since_epoch () } ]
    end,
  from_binary (Packet, Format, Extra).

from_binary (<<>>, _) ->
  undefined;
from_binary (Binary, dict) ->
  from_binary (Binary, dict, dict:new());
from_binary (Binary, Format) ->
  from_binary (Binary, Format, []).

%%====================================================================
%% Internal functions
%%====================================================================

from_binary (Binary, Format, Accum0) ->
  { EventName, Attrs } = read_name (Binary),
  AttrList = read_attrs (Attrs, Format, Accum0),
  case Format of
    json ->
      [{<<"EventName">>, EventName} | AttrList ];
    json_proplist ->
      [{<<"EventName">>, EventName} | AttrList ];
    json_eep18 ->
      [{<<"EventName">>, EventName} | AttrList ];
    _ ->
      #lwes_event { name = EventName, attrs = AttrList }
  end.

type_to_atom (?LWES_TYPE_U_INT_16) -> ?LWES_U_INT_16;
type_to_atom (?LWES_TYPE_INT_16)   -> ?LWES_INT_16;
type_to_atom (?LWES_TYPE_U_INT_32) -> ?LWES_U_INT_32;
type_to_atom (?LWES_TYPE_INT_32)   -> ?LWES_INT_32;
type_to_atom (?LWES_TYPE_U_INT_64) -> ?LWES_U_INT_64;
type_to_atom (?LWES_TYPE_INT_64)   -> ?LWES_INT_64;
type_to_atom (?LWES_TYPE_STRING)   -> ?LWES_STRING;
type_to_atom (?LWES_TYPE_BOOLEAN)  -> ?LWES_BOOLEAN;
type_to_atom (?LWES_TYPE_IP_ADDR)  -> ?LWES_IP_ADDR.

millisecond_since_epoch () ->
  {Meg, Sec, Mic} = os:timestamp(),
  trunc (Meg * 1000000000 + Sec * 1000 + Mic / 1000).

write_sized (Min, Max, Thing) when is_atom (Thing) ->
  write_sized (Min, Max, atom_to_list (Thing));
write_sized (Min, Max, Thing) ->
  case iolist_size (Thing) of
    L when L >= Min, L =< Max ->
      [ <<L:8/integer-unsigned-big>>, Thing];
    _ ->
      throw (size_too_big)
  end.

write_attrs ([], Accum) ->
  Accum;
write_attrs ([{T,K,V} | Rest], Accum) ->
  write_attrs (Rest, [ write_key (K), write (T, V) | Accum ]);
write_attrs ([{K,V} | Rest], Accum) when ?is_int16 (V) ->
  write_attrs (Rest, [ write_key (K), write (?LWES_INT_16, V) | Accum ]);
write_attrs ([{K,V} | Rest], Accum) when ?is_uint16 (V) ->
  write_attrs (Rest, [ write_key (K), write (?LWES_U_INT_16, V) | Accum ]);
write_attrs ([{K,V} | Rest], Accum) when ?is_int32 (V) ->
  write_attrs (Rest, [ write_key (K), write (?LWES_INT_32, V) | Accum ]);
write_attrs ([{K,V} | Rest], Accum) when ?is_uint32 (V) ->
  write_attrs (Rest, [ write_key (K), write (?LWES_U_INT_32, V) | Accum ]);
write_attrs ([{K,V} | Rest], Accum) when ?is_int64 (V) ->
  write_attrs (Rest, [ write_key (K), write (?LWES_INT_64, V) | Accum ]);
write_attrs ([{K,V} | Rest], Accum) when ?is_uint64 (V) ->
  write_attrs (Rest, [ write_key (K), write (?LWES_U_INT_64, V) | Accum ]);
write_attrs ([{K,V} | Rest], Accum) when is_boolean (V) ->
  write_attrs (Rest, [ write_key (K), write (?LWES_BOOLEAN, V) | Accum ]);
write_attrs ([{K,V} | Rest], Accum) when ?is_string (V) ->
  write_attrs (Rest, [ write_key (K), write (?LWES_STRING, V) | Accum ]);
write_attrs ([{K,V = {_,_,_,_}} | Rest], Accum) when ?is_ip_addr (V) ->
  write_attrs (Rest, [ write_key (K), write (?LWES_IP_ADDR, V) | Accum ]).

write_key (Key) ->
  write_sized (1, 255, Key).

write_name (Name) ->
  write_sized (1, 127, Name).

write (uint16, V) ->
  <<?LWES_TYPE_U_INT_16:8/integer-unsigned-big, V:16/integer-unsigned-big>>;
write (int16, V) ->
  <<?LWES_TYPE_INT_16:8/integer-unsigned-big, V:16/integer-signed-big>>;
write (uint32, V) ->
  <<?LWES_TYPE_U_INT_32:8/integer-unsigned-big, V:32/integer-unsigned-big>>;
write (int32, V) ->
  <<?LWES_TYPE_INT_32:8/integer-unsigned-big, V:32/integer-signed-big>>;
write (uint64, V) ->
  <<?LWES_TYPE_U_INT_64:8/integer-unsigned-big, V:64/integer-unsigned-big>>;
write (int64, V) ->
  <<?LWES_TYPE_INT_64:8/integer-unsigned-big, V:64/integer-signed-big>>;
write (ip_addr, {V1, V2, V3, V4}) ->
  <<?LWES_TYPE_IP_ADDR:8/integer-unsigned-big,
    V4:8/integer-unsigned-big,
    V3:8/integer-unsigned-big,
    V2:8/integer-unsigned-big,
    V1:8/integer-unsigned-big>>;
write (boolean, true) ->
  <<?LWES_TYPE_BOOLEAN:8/integer-unsigned-big, 1>>;
write (boolean, false) ->
  <<?LWES_TYPE_BOOLEAN:8/integer-unsigned-big, 0>>;
write (string, V) when is_atom (V) ->
  write (string, atom_to_list (V));
write (string, V) when is_list (V); is_binary (V) ->
  case iolist_size (V) of
    SL when SL >= 0, SL =< 65535 ->
      [ <<?LWES_TYPE_STRING:8/integer-unsigned-big,
          SL:16/integer-unsigned-big>>, V ];
    _ ->
      throw (string_too_big)
  end.

read_name (Binary) ->
  <<Length:8/integer-unsigned-big,
    EventName:Length/binary,
    _NumAttrs:16/integer-unsigned-big,
    Rest/binary>> = Binary,
  { EventName, Rest }.

read_attrs (<<>>, _Format, Accum) ->
  Accum;
read_attrs (Bin, Format, Accum) ->
  <<L:8/integer-unsigned-big, K:L/binary,
    T:8/integer-unsigned-big, Vals/binary>> = Bin,
  { V, Rest } = read_value (T, Vals, Format),
  read_attrs (Rest, Format,
               case Format of
                 dict ->
                   dict:store (K, V, Accum);
                 tagged ->
                   [ {type_to_atom (T), K, V} | Accum ];
                 json ->
                   [ {K, try lwes_mochijson2:decode (V, [{format, struct}]) of
                           S -> S
                         catch
                           _:_ -> V
                         end }
                     | Accum ];
                 json_proplist ->
                   [ {K, try lwes_mochijson2:decode (V, [{format, proplist}]) of
                           S -> S
                         catch
                           _:_ -> V
                         end }
                     | Accum ];
                 json_eep18 ->
                   [ {K, try lwes_mochijson2:decode (V, [{format, eep18}]) of
                           S -> S
                         catch
                           _:_ -> V
                         end }
                     | Accum ];
                 _ ->
                   [ {K, V} | Accum ]
               end).

read_value (?LWES_TYPE_U_INT_16, Bin, _Format) ->
  <<V:16/integer-unsigned-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_INT_16, Bin, _Format) ->
  <<V:16/integer-signed-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_U_INT_32, Bin, _Format) ->
  <<V:32/integer-unsigned-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_INT_32, Bin, _Format) ->
  <<V:32/integer-signed-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_U_INT_64, Bin, _Format) ->
  <<V:64/integer-unsigned-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_INT_64, Bin, _Format) ->
  <<V:64/integer-signed-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_IP_ADDR, Bin, json) ->
  <<V1:8/integer-unsigned-big,
    V2:8/integer-unsigned-big,
    V3:8/integer-unsigned-big,
    V4:8/integer-unsigned-big, Rest/binary>> = Bin,
  { ip2bin ({V4,V3,V2,V1}), Rest };
read_value (?LWES_TYPE_IP_ADDR, Bin, json_proplist) ->
  <<V1:8/integer-unsigned-big,
    V2:8/integer-unsigned-big,
    V3:8/integer-unsigned-big,
    V4:8/integer-unsigned-big, Rest/binary>> = Bin,
  { ip2bin ({V4,V3,V2,V1}), Rest };
read_value (?LWES_TYPE_IP_ADDR, Bin, json_eep18) ->
  <<V1:8/integer-unsigned-big,
    V2:8/integer-unsigned-big,
    V3:8/integer-unsigned-big,
    V4:8/integer-unsigned-big, Rest/binary>> = Bin,
  { ip2bin ({V4,V3,V2,V1}), Rest };
read_value (?LWES_TYPE_IP_ADDR, Bin, _Format) ->
  <<V1:8/integer-unsigned-big,
    V2:8/integer-unsigned-big,
    V3:8/integer-unsigned-big,
    V4:8/integer-unsigned-big, Rest/binary>> = Bin,
  { {V4, V3, V2, V1}, Rest };
read_value (?LWES_TYPE_BOOLEAN, Bin, _Format) ->
  <<V:8/integer-unsigned-big, Rest/binary>> = Bin,
  { case V of 0 -> false; _ -> true end, Rest };
read_value (?LWES_TYPE_STRING, Bin, _Format) ->
  <<SL:16/integer-unsigned-big, V:SL/binary, Rest/binary>> = Bin,
  { V, Rest };
read_value (_, _, _) ->
  throw (unknown_type).

ip2bin (Ip) ->
  list_to_binary(inet_parse:ntoa (Ip)).

%%====================================================================
%% Test functions
%%====================================================================
-ifdef(EUNIT).

new_test_ () ->
  [
    ?_assertEqual (#lwes_event { name = "foo", attrs = []}, new (foo)),
    ?_assertEqual (#lwes_event{name = "foo",attrs = [{int16,cat,-5}]},
                   lwes_event:set_int16 (lwes_event:new(foo),cat,-5)),
    ?_assertError (badarg,
                   lwes_event:set_int16 (lwes_event:new(foo),cat,32768)),
    ?_assertEqual (#lwes_event{name = "foo",attrs = [{uint16,cat,5}]},
                   lwes_event:set_uint16 (lwes_event:new(foo),cat,5)),
    ?_assertError (badarg,
                   lwes_event:set_uint16 (lwes_event:new(foo),cat,-5))
  ].

-endif.
