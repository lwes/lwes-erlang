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
         from_binary/2]).

%%====================================================================
%% API
%%====================================================================
new (Name) ->
  #lwes_event { name = Name }.

set_int16 (E = #lwes_event { attrs = A }, K, V) when ?is_int16 (V) ->
    E#lwes_event { attrs = [ { ?LWES_INT_16, K, V } | A ] }.
set_uint16 (E = #lwes_event { attrs = A }, K, V) when  ?is_uint16 (V) ->
    E#lwes_event { attrs = [ { ?LWES_U_INT_16, K, V } | A ] }.
set_int32 (E = #lwes_event { attrs = A}, K, V) when ?is_int32 (V) ->
    E#lwes_event { attrs = [ { ?LWES_INT_32, K, V } | A ] }.
set_uint32 (E = #lwes_event { attrs = A}, K, V) when ?is_uint32 (V)  ->
    E#lwes_event { attrs = [ { ?LWES_U_INT_32, K, V } | A ] }.
set_int64 (E = #lwes_event { attrs = A}, K, V) when ?is_int64 (V) ->
    E#lwes_event { attrs = [ { ?LWES_INT_64, K, V } | A ] }.
set_uint64 (E = #lwes_event { attrs = A}, K, V) when ?is_uint64 (V) ->
    E#lwes_event { attrs = [ { ?LWES_U_INT_64, K, V } | A ] }.
set_boolean (E = #lwes_event { attrs = A}, K, V) when is_boolean (V) ->
    E#lwes_event { attrs = [ { ?LWES_BOOLEAN, K, V } | A ] }.
set_string (E = #lwes_event { attrs = A}, K, V) when ?is_string (V) ->
    E#lwes_event { attrs = [ { ?LWES_STRING, K, V } | A ] }.
set_ip_addr (E = #lwes_event { attrs = A}, K, V) ->
  Ip = lwes_util:normalize_ip (V),
  E#lwes_event { attrs = [ { ?LWES_IP_ADDR, K, Ip } | A ] }.

to_binary (#lwes_event { name = EventName, attrs = AttrList }) ->
  NumAttrs = length (AttrList),
  iolist_to_binary (
    [ write_key (EventName),
      <<NumAttrs:16/integer-unsigned-big>>,
      write_attrs (AttrList, [])
    ]
  ).

from_udp_packet ({ udp, _Socket, SenderIP, SenderPort, Packet }, Format) ->
  Extra =
    case Format of
      tagged ->
        [ { ?LWES_IP_ADDR,  <<"SenderIP">>,   SenderIP },
          { ?LWES_U_INT_16, <<"SenderPort">>, SenderPort },
          { ?LWES_INT_64,   <<"ReceiptTime">>, millisecond_since_epoch () } ];
      _ ->
        [ { <<"SenderIP">>,   SenderIP },
          { <<"SenderPort">>, SenderPort },
          { <<"ReceiptTime">>, millisecond_since_epoch () } ]
    end,
  from_binary (Packet, Format, Extra).

from_binary (<<>>,_) ->
  undefined;
from_binary (Binary,Format) ->
  from_binary (Binary, Format, []).

%%====================================================================
%% Internal functions
%%====================================================================

from_binary (Binary, Format, Accum0) ->
  <<Length:8/integer-unsigned-big,
    EventName:Length/binary,
    _NumAttrs:16/integer-unsigned-big,
    Attrs/binary>> = Binary,
  AttrList = read_attrs (Attrs, Format, Accum0),
  #lwes_event { name = EventName, attrs = AttrList }.

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

write_key (Key) when is_atom (Key) ->
  write_key (atom_to_list (Key));
write_key (Key) ->
  Len = iolist_size (Key),
  [ <<Len:8/integer-unsigned-big>>, Key].

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
  SL = iolist_size (V),
  [ <<?LWES_TYPE_STRING:8/integer-unsigned-big,
      SL:16/integer-unsigned-big>>, V ].

read_attrs (<<>>, _Format, Accum) ->
  Accum;
read_attrs (Bin, Format, Accum) ->
  <<L:8/integer-unsigned-big, K:L/binary,
    T:8/integer-unsigned-big, Vals/binary>> = Bin,
  { V, Rest } = read_value (T, Vals),
  read_attrs (Rest, Format,
               [ case Format of
                   tagged -> {type_to_atom (T), K, V};
                   _ -> {K, V}
                 end
                 | Accum ]).

read_value (?LWES_TYPE_U_INT_16, Bin) ->
  <<V:16/integer-unsigned-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_INT_16, Bin) ->
  <<V:16/integer-signed-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_U_INT_32, Bin) ->
  <<V:32/integer-unsigned-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_INT_32, Bin) ->
  <<V:32/integer-signed-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_U_INT_64, Bin) ->
  <<V:64/integer-unsigned-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_INT_64, Bin) ->
  <<V:64/integer-signed-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_IP_ADDR, Bin) ->
  <<V1:8/integer-unsigned-big,
    V2:8/integer-unsigned-big,
    V3:8/integer-unsigned-big,
    V4:8/integer-unsigned-big, Rest/binary>> = Bin,
  { {V4, V3, V2, V1}, Rest };
read_value (?LWES_TYPE_BOOLEAN, Bin) ->
  <<V:8/integer-unsigned-big, Rest/binary>> = Bin,
  { case V of 0 -> false; _ -> true end, Rest };
read_value (?LWES_TYPE_STRING, Bin) ->
  <<SL:16/integer-unsigned-big, V:SL/binary, Rest/binary>> = Bin,
  { V, Rest };
read_value (_, _) ->
  throw (unknown_type).

%%====================================================================
%% Test functions
%%====================================================================
-ifdef(EUNIT).

-endif.
