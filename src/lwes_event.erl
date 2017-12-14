-module (lwes_event).

-include_lib ("lwes.hrl").
-include_lib ("lwes_internal.hrl").

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
         set_byte/3,
         set_float/3,
         set_double/3,
         set_long_string/3,
         set_uint16_array/3,
         set_int16_array/3,
         set_uint32_array/3,
         set_int32_array/3,
         set_uint64_array/3,
         set_int64_array/3,
         set_string_array/3,
         set_ip_addr_array/3,
         set_boolean_array/3,
         set_byte_array/3,
         set_float_array/3,
         set_double_array/3,
         set_nuint16_array/3,
         set_nint16_array/3,
         set_nuint32_array/3,
         set_nint32_array/3,
         set_nuint64_array/3,
         set_nint64_array/3,
         set_nstring_array/3,
         set_nboolean_array/3,
         set_nbyte_array/3,
         set_nfloat_array/3,
         set_ndouble_array/3,
         to_binary/1,
         to_iolist/1,
         from_udp_packet/2,
         from_binary/1,
         from_binary/2,
         peek_name_from_udp/1,
         header_fields_to_iolist/3,
         has_header_fields/1,
         from_json/1,
         to_json/1,
         to_json/2,
         remove_attr/2
        ]).

-define (write_nullable_array (LwesType,Guard,BinarySize,BinaryType, V ),
   Len = length (V),
   {Bitset, Data} = lists:foldl (
       fun
         (undefined, {BitAccum, DataAccum}) -> {<<0:1, BitAccum/bitstring>>, DataAccum};
         (X, {BitAccum, DataAccum}) when Guard (X) ->
           {<<1:1, BitAccum/bitstring>>, <<DataAccum/binary, X:BinarySize/BinaryType>>};
         (_, _) -> erlang:error (badarg)
       end, {<<>>, <<>>}, V),
    LwesBitsetBin = lwes_bitset_rep (Len, Bitset),
    <<LwesType:8/integer-unsigned-big,
      Len:16/integer-unsigned-big, Len:16/integer-unsigned-big,
      LwesBitsetBin/binary, Data/binary>>
    ).

-define (read_nullable_array (Bin, LwesType, ElementSize),
    <<AL:16/integer-unsigned-big, _:16, Rest/binary>> = Bin,
    {NotNullCount, BitsetLength, Bitset} = decode_bitset(AL, Rest),
    Count = NotNullCount * ElementSize,
    <<_:BitsetLength, Values:Count/bits, Rest2/binary>> = Rest,
    { read_n_array (LwesType, AL, 1, Bitset ,Values, Format, []), Rest2 }).

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
set_byte(E = #lwes_event { attrs = A}, K, V) when ?is_byte (V) ->
  E#lwes_event { attrs = [ { ?LWES_BYTE, K, V } | A ] };
set_byte(_,_,_) ->
  erlang:error(badarg).
set_float(E = #lwes_event { attrs = A}, K, V) when is_float (V) ->
  E#lwes_event { attrs = [ { ?LWES_FLOAT, K, V } | A ] };
set_float(_,_,_) ->
  erlang:error(badarg).
set_double(E = #lwes_event { attrs = A}, K, V) when is_float (V) ->
  E#lwes_event { attrs = [ { ?LWES_DOUBLE, K, V } | A ] };
set_double(_,_,_) ->
  erlang:error(badarg).
set_long_string(E = #lwes_event { attrs = A}, K, V) when is_binary (V) ->
  E#lwes_event { attrs = [ { ?LWES_LONG_STRING, K, V } | A ] };
set_long_string(_,_,_) ->
  erlang:error(badarg).
set_uint16_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_U_INT_16_ARRAY, K, V } | A ] };
set_uint16_array(_,_,_) ->
  erlang:error(badarg).
set_int16_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_INT_16_ARRAY, K, V } | A ] };
set_int16_array(_,_,_) ->
  erlang:error(badarg).
set_uint32_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_U_INT_32_ARRAY, K, V } | A ] };
set_uint32_array(_,_,_) ->
  erlang:error(badarg).
set_int32_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_INT_32_ARRAY, K, V } | A ] };
set_int32_array(_,_,_) ->
  erlang:error(badarg).
set_uint64_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_U_INT_64_ARRAY, K, V } | A ] };
set_uint64_array(_,_,_) ->
  erlang:error(badarg).
set_int64_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_INT_64_ARRAY, K, V } | A ] };
set_int64_array(_,_,_) ->
  erlang:error(badarg).
set_string_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_STRING_ARRAY, K, V } | A ] };
set_string_array(_,_,_) ->
  erlang:error(badarg).
set_ip_addr_array (E = #lwes_event { attrs = A}, K, V)  when is_list (V) ->
  Ips = [lwes_util:normalize_ip (I) || I <- V],
  E#lwes_event { attrs = [ { ?LWES_IP_ADDR_ARRAY, K, Ips } | A ] };
set_ip_addr_array(_,_,_) ->
  erlang:error(badarg).
set_boolean_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_BOOLEAN_ARRAY, K, V } | A ] };
set_boolean_array(_,_,_) ->
  erlang:error(badarg).
set_byte_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_BYTE_ARRAY, K, V } | A ] };
set_byte_array(_,_,_) ->
  erlang:error(badarg).
set_float_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_FLOAT_ARRAY, K, V } | A ] };
set_float_array(_,_,_) ->
  erlang:error(badarg).
set_double_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_DOUBLE_ARRAY, K, V } | A ] };
set_double_array(_,_,_) ->
  erlang:error(badarg).
set_nuint16_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_U_INT_16_ARRAY, K, V } | A ] };
set_nuint16_array(_,_,_) ->
  erlang:error(badarg).
set_nint16_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_INT_16_ARRAY, K, V } | A ] };
set_nint16_array(_,_,_) ->
  erlang:error(badarg).
set_nuint32_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_U_INT_32_ARRAY, K, V } | A ] };
set_nuint32_array(_,_,_) ->
  erlang:error(badarg).
set_nint32_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_INT_32_ARRAY, K, V } | A ] };
set_nint32_array(_,_,_) ->
  erlang:error(badarg).
set_nuint64_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_U_INT_64_ARRAY, K, V } | A ] };
set_nuint64_array(_,_,_) ->
  erlang:error(badarg).
set_nint64_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_INT_64_ARRAY, K, V } | A ] };
set_nint64_array(_,_,_) ->
  erlang:error(badarg).
set_nstring_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_STRING_ARRAY, K, V } | A ] };
set_nstring_array(_,_,_) ->
  erlang:error(badarg).
set_nboolean_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_BOOLEAN_ARRAY, K, V } | A ] };
set_nboolean_array(_,_,_) ->
  erlang:error(badarg).
set_nbyte_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_BYTE_ARRAY, K, V } | A ] };
set_nbyte_array(_,_,_) ->
  erlang:error(badarg).
set_nfloat_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_FLOAT_ARRAY, K, V } | A ] };
set_nfloat_array(_,_,_) ->
  erlang:error(badarg).
set_ndouble_array(E = #lwes_event { attrs = A}, K, V) when is_list (V) ->
  E#lwes_event { attrs = [ { ?LWES_N_DOUBLE_ARRAY, K, V } | A ] };
set_ndouble_array(_,_,_) ->
  erlang:error(badarg).

to_binary (Event = #lwes_event { }) ->
  iolist_to_binary (to_iolist (Event));
% allow for re-emission of events, if it doesn't match the record, it
% could be a binary or an iolist, so just forward it through
to_binary (Event) ->
  Event.

to_iolist (Event = #lwes_event { name = EventName, attrs = Attrs }) ->
  case Attrs of
    Dict when is_tuple (Attrs) andalso element (1, Attrs) =:= dict ->
      to_iolist (Event#lwes_event { attrs = dict:to_list (Dict) });
    A ->
      NumAttrs = length (A),
      [ write_name (EventName),
        <<NumAttrs:16/integer-unsigned-big>>,
        write_attrs (A, [])
      ]
  end;
to_iolist (Event) ->
  % assume if we get anything else it's either a binary or an iolist
  Event.

peek_name_from_udp ({ udp, _, _, _, Packet }) ->
  case read_name (Packet) of
    { ok, EventName, _ } -> EventName;
    E -> E
  end.

header_fields_to_iolist (ReceiptTime, SenderIP, SenderPort) ->
  % these need to be in reverse order as we don't bother reversing the io_list
  write_attrs (
    [ {?LWES_U_INT_16, <<"SenderPort">>, SenderPort},
      {?LWES_IP_ADDR, <<"SenderIP">>, SenderIP},
      {?LWES_INT_64, <<"ReceiptTime">>, ReceiptTime} ], []).

has_header_fields (B) when is_binary(B) ->
  Size = erlang:byte_size (B),
  % I need to check for the following at the end of the event binary
  % int64   ReceiptTime =    1 (short string length)
  %                       + 11 (length of string)
  %                       +  1 (length of type byte)
  %                       +  8 (length of int64)
  %                       = 21
  % ip_addr SenderIp    = 1 + 8 + 1 + 4 = 14
  % uint16  SenderPort  = 1 + 10 + 1 + 2 = 14
  %
  % So total bytes at the end are 21+14+14 = 49
  NumberToSkip = Size - 49,

  case B of
    <<_:NumberToSkip/bytes,                      % skip beginning
      11:8/integer-unsigned-big,                 % length of "ReceiptTime" 11
      "ReceiptTime",                             %
      ?LWES_TYPE_INT_64:8/integer-unsigned-big,  % type byte
      _:64/integer-signed-big,                   % value
      8:8/integer-unsigned-big,                  % length of "SenderIP" 8
      "SenderIP",                                %
      ?LWES_TYPE_IP_ADDR:8/integer-unsigned-big, % type byte
      _:32/integer-unsigned-big,                 % value
      10:8/integer-unsigned-big,                 % length of "SenderPort" 10
      "SenderPort",                              %
      ?LWES_TYPE_U_INT_16:8/integer-unsigned-big,% type byte
      _:16/integer-unsigned-big>> ->             % value
      true;
    _ ->
      false
  end.

from_udp_packet (Packet, raw) ->
  Packet;
from_udp_packet ({ udp, Socket, SenderIP, SenderPort, Packet }, Format) ->
  % allow ReceiptTime to come in via the second element of the tuple in
  % some cases, this was put in place to work with the journal listener
  ReceiptTime =
    case Socket of
      S when is_port(S) ->
        millisecond_since_epoch();
      R when is_integer(R) ->
        R
    end,
  % only add header fields if they have not been added by upstream
  Extra =
    case has_header_fields (Packet) of
      true -> [];
      false ->
        case Format of
          tagged ->
            [ { ?LWES_IP_ADDR,  <<"SenderIP">>,   SenderIP },
              { ?LWES_U_INT_16, <<"SenderPort">>, SenderPort },
              { ?LWES_INT_64,   <<"ReceiptTime">>, ReceiptTime } ];
          dict ->
            dict:from_list (
              [ { <<"SenderIP">>,   SenderIP },
                { <<"SenderPort">>, SenderPort },
                { <<"ReceiptTime">>, ReceiptTime } ]);
          F when F =:= json;
                 F =:= json_untyped;
                 F =:= json_proplist;
                 F =:= json_proplist_untyped;
                 F =:= json_eep18;
                 F =:= json_eep18_untyped  ->
            [ { <<"SenderIP">>,   lwes_util:ip2bin (SenderIP) },
              { <<"SenderPort">>, SenderPort },
              { <<"ReceiptTime">>, ReceiptTime } ];
          F when F =:= json_typed;
                 F =:= json_proplist_typed;
                 F =:= json_eep18_typed ->
            [ { ?LWES_IP_ADDR, <<"SenderIP">>, lwes_util:ip2bin (SenderIP) },
              { ?LWES_U_INT_16, <<"SenderPort">>, SenderPort },
              { ?LWES_INT_64, <<"ReceiptTime">>, ReceiptTime } ];
          _ ->
            [ { <<"SenderIP">>,   SenderIP },
              { <<"SenderPort">>, SenderPort },
              { <<"ReceiptTime">>, ReceiptTime } ]
        end
    end,
  from_binary (Packet, Format, Extra).

from_binary (B) when is_binary (B) ->
  from_binary (B, list).

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
  { ok, EventName, Attrs } = read_name (Binary),
  case Format of
    json ->
      AttrList = read_attrs (Attrs, json, Accum0),
      {struct, [{<<"EventName">>, EventName} | AttrList ]};
    json_untyped ->
      AttrList = read_attrs (Attrs, json, Accum0),
      {struct, [{<<"EventName">>, EventName} | AttrList]};
    json_typed ->
      AttrList = read_attrs (Attrs, json_typed, Accum0),
      {struct,
       [ {<<"EventName">>, EventName},
         {<<"typed">>,
          {struct, export_attributes(json_typed, AttrList)}
         }
       ]
      };
    json_proplist ->
      AttrList = read_attrs (Attrs, json_proplist, Accum0),
      [{<<"EventName">>, EventName} | AttrList ];
    json_proplist_untyped ->
      AttrList = read_attrs (Attrs, json_proplist, Accum0),
      [{<<"EventName">>, EventName} | AttrList ];
    json_proplist_typed ->
      AttrList = read_attrs (Attrs, json_proplist_typed, Accum0),
      [{<<"EventName">>, EventName},
       {<<"typed">>, export_attributes(json_proplist_typed, AttrList)}
      ];
    json_eep18 ->
      AttrList = read_attrs (Attrs, json_eep18, Accum0),
      {[{<<"EventName">>, EventName} | AttrList ]};
    json_eep18_untyped ->
      AttrList = read_attrs (Attrs, json_eep18, Accum0),
      {[{<<"EventName">>, EventName} | AttrList ]};
    json_eep18_typed ->
      AttrList = read_attrs (Attrs, json_eep18_typed, Accum0),
      {[{<<"EventName">>, EventName},
        {<<"typed">>, {export_attributes(json_eep18_typed, AttrList)}}
       ]};
    _ ->
      AttrList = read_attrs (Attrs, Format, Accum0),
      #lwes_event { name = EventName, attrs = AttrList }
  end.

split_bounds(Index, Bitset) ->
  Size = size(Bitset) * 8,
  L = Size - Index,
  R = Index - 1,
  {L, R}.

lwes_bitset_rep (Len, Bitset) ->
  Padding = (erlang:byte_size(Bitset) * 8) - Len,
  BitsetBin = <<0:Padding, Bitset/bitstring>>,
    reverse_bytes_in_bin(BitsetBin).

reverse_bytes_in_bin (Bitset) ->
  binary:list_to_bin(
      lists:reverse(
          binary:bin_to_list(Bitset))).

decode_bitset(AL, Bin) ->
  BitsetLength = lwes_util:ceiling( AL/8 ) * 8,
  <<Bitset:BitsetLength/bitstring, _/bitstring>> = Bin,
  {lwes_util:count_ones(Bitset), BitsetLength, reverse_bytes_in_bin(Bitset)}.

type_to_atom (?LWES_TYPE_U_INT_16) -> ?LWES_U_INT_16;
type_to_atom (?LWES_TYPE_INT_16)   -> ?LWES_INT_16;
type_to_atom (?LWES_TYPE_U_INT_32) -> ?LWES_U_INT_32;
type_to_atom (?LWES_TYPE_INT_32)   -> ?LWES_INT_32;
type_to_atom (?LWES_TYPE_U_INT_64) -> ?LWES_U_INT_64;
type_to_atom (?LWES_TYPE_INT_64)   -> ?LWES_INT_64;
type_to_atom (?LWES_TYPE_STRING)   -> ?LWES_STRING;
type_to_atom (?LWES_TYPE_BOOLEAN)  -> ?LWES_BOOLEAN;
type_to_atom (?LWES_TYPE_IP_ADDR)  -> ?LWES_IP_ADDR;
type_to_atom (?LWES_TYPE_BYTE)     -> ?LWES_BYTE;
type_to_atom (?LWES_TYPE_FLOAT)    -> ?LWES_FLOAT;
type_to_atom (?LWES_TYPE_DOUBLE)   -> ?LWES_DOUBLE;
type_to_atom (?LWES_TYPE_LONG_STRING) -> ?LWES_LONG_STRING;
type_to_atom (?LWES_TYPE_U_INT_16_ARRAY) -> ?LWES_U_INT_16_ARRAY;
type_to_atom (?LWES_TYPE_N_U_INT_16_ARRAY) -> ?LWES_N_U_INT_16_ARRAY;
type_to_atom (?LWES_TYPE_INT_16_ARRAY)   -> ?LWES_INT_16_ARRAY;
type_to_atom (?LWES_TYPE_N_INT_16_ARRAY)   -> ?LWES_N_INT_16_ARRAY;
type_to_atom (?LWES_TYPE_U_INT_32_ARRAY) -> ?LWES_U_INT_32_ARRAY;
type_to_atom (?LWES_TYPE_N_U_INT_32_ARRAY) -> ?LWES_N_U_INT_32_ARRAY;
type_to_atom (?LWES_TYPE_INT_32_ARRAY)   -> ?LWES_INT_32_ARRAY;
type_to_atom (?LWES_TYPE_N_INT_32_ARRAY)   -> ?LWES_N_INT_32_ARRAY;
type_to_atom (?LWES_TYPE_INT_64_ARRAY)   -> ?LWES_INT_64_ARRAY;
type_to_atom (?LWES_TYPE_N_INT_64_ARRAY)   -> ?LWES_N_INT_64_ARRAY;
type_to_atom (?LWES_TYPE_U_INT_64_ARRAY) -> ?LWES_U_INT_64_ARRAY;
type_to_atom (?LWES_TYPE_N_U_INT_64_ARRAY) -> ?LWES_N_U_INT_64_ARRAY;
type_to_atom (?LWES_TYPE_STRING_ARRAY)   -> ?LWES_STRING_ARRAY;
type_to_atom (?LWES_TYPE_N_STRING_ARRAY)   -> ?LWES_N_STRING_ARRAY;
type_to_atom (?LWES_TYPE_IP_ADDR_ARRAY)  -> ?LWES_IP_ADDR_ARRAY;
type_to_atom (?LWES_TYPE_BOOLEAN_ARRAY)  -> ?LWES_BOOLEAN_ARRAY;
type_to_atom (?LWES_TYPE_N_BOOLEAN_ARRAY)  -> ?LWES_N_BOOLEAN_ARRAY;
type_to_atom (?LWES_TYPE_BYTE_ARRAY)     -> ?LWES_BYTE_ARRAY;
type_to_atom (?LWES_TYPE_N_BYTE_ARRAY)     -> ?LWES_N_BYTE_ARRAY;
type_to_atom (?LWES_TYPE_FLOAT_ARRAY)    -> ?LWES_FLOAT_ARRAY;
type_to_atom (?LWES_TYPE_N_FLOAT_ARRAY)    -> ?LWES_N_FLOAT_ARRAY;
type_to_atom (?LWES_TYPE_DOUBLE_ARRAY)   -> ?LWES_DOUBLE_ARRAY;
type_to_atom (?LWES_TYPE_N_DOUBLE_ARRAY)   -> ?LWES_N_DOUBLE_ARRAY.

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
write_attrs ([{K,V} | Rest], Accum) when is_atom (V); is_binary (V) ->
  case V of
    A when is_atom (A) ->
       write_attrs (Rest, [ write_key (K), write (?LWES_STRING, V) | Accum ]);
    _ ->
       case iolist_size (V) of
         SL when SL >= 0, SL =< 65535 ->
           write_attrs (Rest, [ write_key (K), write (?LWES_STRING, SL, V) | Accum ]);
         SL when SL=< 4294967295 ->
           write_attrs (Rest, [ write_key (K), write (?LWES_LONG_STRING, SL, V) | Accum ]);
         _ -> throw (string_too_big)
       end
  end;
write_attrs ([{K,V} | Rest], Accum) when is_list (V) ->
  write_attrs (Rest, [ write_key (K), write (type_array (V), V) | Accum ]);
write_attrs ([{K,V = {_,_,_,_}} | Rest], Accum) when ?is_ip_addr (V) ->
  write_attrs (Rest, [ write_key (K), write (?LWES_IP_ADDR, V) | Accum ]).

type_array (L)        -> type_array(L, undefined).
type_array ([], Type) -> Type;
type_array ([ H | T ], PrevType) ->
  NewType = get_type (H),
  case rank_type (NewType) > rank_type (PrevType) of
    true ->
      type_array (T, NewType);
    _    ->
      type_array (T, PrevType)
  end.

get_type (H) when is_boolean (H)  -> ?LWES_BOOLEAN_ARRAY;
%% BYTE ARRAY IS INDISTINGUISHABLE FROM
%% STRING, SO WE WILL CONSIDER BYTE ARRAYS
%% TO BE STRING TYPES BY DEFAULT, IF YOU WANT A BYTE ARRAY
%% THEN USE THE THREE-TUPLE FORM.
get_type (H) when ?is_string (H)  -> ?LWES_STRING_ARRAY;
get_type (H) when ?is_byte (H)    -> ?LWES_STRING;
get_type (H) when is_float (H)    -> ?LWES_DOUBLE_ARRAY;
get_type (H) when ?is_int16 (H)   -> ?LWES_INT_16_ARRAY;
get_type (H) when ?is_uint16 (H)  -> ?LWES_U_INT_16_ARRAY;
get_type (H) when ?is_int32 (H)   -> ?LWES_INT_32_ARRAY;
get_type (H) when ?is_uint32 (H)  -> ?LWES_U_INT_32_ARRAY;
get_type (H) when ?is_int64 (H)   -> ?LWES_INT_64_ARRAY;
get_type (H) when ?is_uint64 (H)  -> ?LWES_U_INT_64_ARRAY;
get_type (H) when ?is_ip_addr (H) -> ?LWES_IP_ADDR_ARRAY.

infer_type ([H|_]) when is_boolean(H) -> ?LWES_BOOLEAN_ARRAY;
%% BYTE ARRAY IS INDISTINGUISHABLE FROM
%% STRING, SO WE WILL CONSIDER BYTE ARRAYS
%% TO BE STRING TYPES BY DEFAULT, IF YOU WANT A BYTE ARRAY
%% THEN USE THE THREE-TUPLE FORM.
infer_type ([H|_]) when ?is_string (H)  -> ?LWES_STRING_ARRAY;
infer_type ([H|_]) when ?is_byte (H)    -> ?LWES_STRING;
infer_type ([H|_]) when is_float (H)    -> ?LWES_DOUBLE_ARRAY;
infer_type ([H|_]) when ?is_int16 (H)   -> ?LWES_INT_16_ARRAY;
infer_type ([H|_]) when ?is_uint16 (H)  -> ?LWES_U_INT_16_ARRAY;
infer_type ([H|_]) when ?is_int32 (H)   -> ?LWES_INT_32_ARRAY;
infer_type ([H|_]) when ?is_uint32 (H)  -> ?LWES_U_INT_32_ARRAY;
infer_type ([H|_]) when ?is_int64 (H)   -> ?LWES_INT_64_ARRAY;
infer_type ([H|_]) when ?is_uint64 (H)  -> ?LWES_U_INT_64_ARRAY;
infer_type ([H|_]) when ?is_ip_addr (H) -> ?LWES_IP_ADDR_ARRAY;
infer_type (H) when is_boolean (H) -> ?LWES_BOOLEAN;
%% BYTE ARRAY IS INDISTINGUISHABLE FROM
%% STRING, SO WE WILL CONSIDER BYTE ARRAYS
%% TO BE STRING TYPES BY DEFAULT, IF YOU WANT A BYTE ARRAY
%% THEN USE THE THREE-TUPLE FORM.
infer_type (H) when ?is_string (H) -> ?LWES_STRING;
infer_type (H) when is_float (H)   -> ?LWES_DOUBLE;
infer_type (H) when ?is_int16 (H)  -> ?LWES_INT_16;
infer_type (H) when ?is_uint16 (H) -> ?LWES_U_INT_16;
infer_type (H) when ?is_int32 (H)  -> ?LWES_INT_32;
infer_type (H) when ?is_uint32 (H) -> ?LWES_U_INT_32;
infer_type (H) when ?is_int64 (H)  -> ?LWES_INT_64;
infer_type (H) when ?is_uint64 (H) -> ?LWES_U_INT_64;
infer_type (H) when ?is_ip_addr (H)-> ?LWES_IP_ADDR.

rank_type (undefined)            ->  0;
rank_type (?LWES_U_INT_16_ARRAY) ->  1;
rank_type (?LWES_INT_16_ARRAY)   ->  2;
rank_type (?LWES_U_INT_32_ARRAY) ->  3;
rank_type (?LWES_INT_32_ARRAY)   ->  4;
rank_type (?LWES_U_INT_64_ARRAY) ->  5;
rank_type (?LWES_INT_64_ARRAY)   ->  6;
rank_type (?LWES_BOOLEAN_ARRAY)  ->  7;
rank_type (?LWES_DOUBLE_ARRAY)   ->  8;
rank_type (?LWES_STRING_ARRAY)   ->  9;
rank_type (?LWES_STRING)         -> 10.


write_key (Key) ->
  write_sized (1, 255, Key).

write_name (Name) ->
  write_sized (1, 127, Name).

write (?LWES_U_INT_16, V) ->
  <<?LWES_TYPE_U_INT_16:8/integer-unsigned-big, V:16/integer-unsigned-big>>;
write (?LWES_INT_16, V) ->
  <<?LWES_TYPE_INT_16:8/integer-unsigned-big, V:16/integer-signed-big>>;
write (?LWES_U_INT_32, V) ->
  <<?LWES_TYPE_U_INT_32:8/integer-unsigned-big, V:32/integer-unsigned-big>>;
write (?LWES_INT_32, V) ->
  <<?LWES_TYPE_INT_32:8/integer-unsigned-big, V:32/integer-signed-big>>;
write (?LWES_U_INT_64, V) ->
  <<?LWES_TYPE_U_INT_64:8/integer-unsigned-big, V:64/integer-unsigned-big>>;
write (?LWES_INT_64, V) ->
  <<?LWES_TYPE_INT_64:8/integer-unsigned-big, V:64/integer-signed-big>>;
write (?LWES_IP_ADDR, {V1, V2, V3, V4}) ->
  <<?LWES_TYPE_IP_ADDR:8/integer-unsigned-big,
    V4:8/integer-unsigned-big,
    V3:8/integer-unsigned-big,
    V2:8/integer-unsigned-big,
    V1:8/integer-unsigned-big>>;
write (?LWES_BOOLEAN, true) ->
  <<?LWES_TYPE_BOOLEAN:8/integer-unsigned-big, 1>>;
write (?LWES_BOOLEAN, false) ->
  <<?LWES_TYPE_BOOLEAN:8/integer-unsigned-big, 0>>;
write (?LWES_STRING, V) when is_atom (V) ->
  write (?LWES_STRING, atom_to_list (V));
write (?LWES_STRING, V) when is_list (V); is_binary (V) ->
  write (?LWES_STRING, iolist_size (V), V);
write (?LWES_BYTE, V) ->
  <<?LWES_TYPE_BYTE:8/integer-unsigned-big, V:8/integer-unsigned-big>>;
write (?LWES_FLOAT, V) ->
  <<?LWES_TYPE_FLOAT:8/integer-unsigned-big, V:32/float>>;
write (?LWES_DOUBLE, V) ->
  <<?LWES_TYPE_DOUBLE:8/integer-unsigned-big, V:64/float>>;
write (?LWES_LONG_STRING, V) when is_list(V); is_binary (V) ->
  write (?LWES_LONG_STRING, iolist_size (V), V);
write (?LWES_U_INT_16_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (X, A) when ?is_uint16 (X) -> <<A/binary, X:16/integer-unsigned-big>>;
    (_X, _A) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_U_INT_16_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_INT_16_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (X, A) when ?is_int16 (X) -> <<A/binary, X:16/integer-signed-big>>;
    (_, _) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_INT_16_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_U_INT_32_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (X, A) when ?is_uint32 (X) -> <<A/binary, X:32/integer-unsigned-big>>;
    (_, _) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_U_INT_32_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_INT_32_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (X, A) when ?is_int32 (X) -> <<A/binary, X:32/integer-signed-big>>;
    (_, _) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_INT_32_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_U_INT_64_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (X, A) when ?is_uint64 (X) -> <<A/binary, X:64/integer-unsigned-big>>;
    (_, _) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_U_INT_64_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_INT_64_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (X, A) when ?is_int64 (X) -> <<A/binary, X:64/integer-signed-big>>;
    (_, _) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_INT_64_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_STRING_ARRAY, V) ->
  Len = length (V),
  V1 = string_array_to_binary (V),
  V2 = lists:foldl (
  fun(X, A) ->
      case iolist_size (X) of
        SL when SL >= 0, SL =< 65535 ->
            <<A/binary, SL:16/integer-unsigned-big, X/binary>>;
        _ ->
          throw (string_too_big)
      end
  end, <<>>, V1),
  <<?LWES_TYPE_STRING_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_IP_ADDR_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (X, A) when ?is_ip_addr (X) ->
      {V1, V2, V3, V4} = X,
      <<A/binary,
        V4:8/integer-unsigned-big,
        V3:8/integer-unsigned-big,
        V2:8/integer-unsigned-big,
        V1:8/integer-unsigned-big>>;
    (_, _) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_IP_ADDR_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_BOOLEAN_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (true, A) -> <<A/binary, 1>>;
    (false, A) -> <<A/binary, 0>>;
    (_, _) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_BOOLEAN_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_BYTE_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (X, A) when ?is_byte (X) -> <<A/binary, X:8/integer-signed-big>>;
    (_, _) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_BYTE_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_FLOAT_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (X, A) when is_float (X) -> <<A/binary, X:32/float>>;
    (_, _) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_FLOAT_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_DOUBLE_ARRAY, V) ->
  Len = length (V),
  V2 = lists:foldl (
  fun
    (X, A) when is_float (X) -> <<A/binary, X:64/float>>;
    (_, _) -> erlang:error (badarg)
  end, <<>>, V),
  <<?LWES_TYPE_DOUBLE_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, V2/binary>>;
write (?LWES_N_U_INT_16_ARRAY, V) ->
  ?write_nullable_array(?LWES_TYPE_N_U_INT_16_ARRAY,
                          ?is_uint16, 16, integer-unsigned-big, V);
write (?LWES_N_INT_16_ARRAY, V) ->
  ?write_nullable_array(?LWES_TYPE_N_INT_16_ARRAY,
                          ?is_int16, 16, integer-signed-big, V);
write (?LWES_N_U_INT_32_ARRAY, V) ->
  ?write_nullable_array(?LWES_TYPE_N_U_INT_32_ARRAY,
                          ?is_uint32, 32, integer-unsigned-big, V);
write (?LWES_N_INT_32_ARRAY, V) ->
  ?write_nullable_array(?LWES_TYPE_N_INT_32_ARRAY,
                          ?is_int32, 32, integer-signed-big, V);
write (?LWES_N_U_INT_64_ARRAY, V) ->
  ?write_nullable_array(?LWES_TYPE_N_U_INT_64_ARRAY,
                          ?is_uint64, 64, integer-unsigned-big, V);
write (?LWES_N_INT_64_ARRAY, V) ->
  ?write_nullable_array(?LWES_TYPE_N_INT_64_ARRAY,
                          ?is_int64, 64, integer-signed-big, V);
write (?LWES_N_BYTE_ARRAY, V) ->
  ?write_nullable_array(?LWES_TYPE_N_BYTE_ARRAY,
                          ?is_byte, 8, integer-signed-big, V);
write (?LWES_N_FLOAT_ARRAY, V) ->
  ?write_nullable_array(?LWES_TYPE_N_FLOAT_ARRAY,
                          is_float, 32, float, V);
write (?LWES_N_DOUBLE_ARRAY, V) ->
  ?write_nullable_array(?LWES_TYPE_N_DOUBLE_ARRAY,
                          is_float, 64, float, V);
write (?LWES_N_BOOLEAN_ARRAY, V) ->
  Len = length (V),
  {Bitset, Data} = lists:foldl (
    fun
      (undefined, {BitAccum, DataAccum}) -> {<<0:1, BitAccum/bitstring>>, DataAccum};
      (true, {BitAccum, DataAccum}) -> {<<1:1, BitAccum/bitstring>>, <<DataAccum/binary, 1>>};
      (false,{BitAccum, DataAccum}) -> {<<1:1, BitAccum/bitstring>>, <<DataAccum/binary, 0>>};
      (_, _) -> erlang:error (badarg)
    end, {<<>>, <<>>}, V),
  LwesBitsetBin = lwes_bitset_rep (Len, Bitset),
  <<?LWES_TYPE_N_BOOLEAN_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big, Len:16/integer-unsigned-big,
    LwesBitsetBin/binary, Data/binary>>;
write (?LWES_N_STRING_ARRAY, V) ->
  Len = length (V),
  V1 = string_array_to_binary (V),
  {Bitset, Data} = lists:foldl (
  fun (undefined , {BitAccum, DataAccum}) -> {<<0:1, BitAccum/bitstring>>, DataAccum};
      (X, {BitAccum, DataAccum}) ->
      case iolist_size (X) of
        SL when SL >= 0, SL =< 65535 ->
            {<<1:1, BitAccum/bitstring>>,
            <<DataAccum/binary, SL:16/integer-unsigned-big, X/binary>>};
        _ ->
          throw (string_too_big)
      end
  end, {<<>>,<<>>}, V1),
  LwesBitsetBin = lwes_bitset_rep (Len, Bitset),
  <<?LWES_TYPE_N_STRING_ARRAY:8/integer-unsigned-big,
    Len:16/integer-unsigned-big,  Len:16/integer-unsigned-big,
    LwesBitsetBin/binary, Data/binary>>.

write (?LWES_STRING, Len, V) when is_list (V); is_binary (V) ->
  case Len of
    SL when SL >= 0, SL =< 65535 ->
      [ <<?LWES_TYPE_STRING:8/integer-unsigned-big,
          SL:16/integer-unsigned-big>>, V ];
    _ ->
      throw (string_too_big)
  end;
write (?LWES_LONG_STRING, Len, V) when is_list(V); is_binary (V) ->
  case Len of
    SL when SL =< 4294967295
       -> <<?LWES_TYPE_LONG_STRING:8/integer-unsigned-big,
            SL:32/integer-unsigned-big, V/binary>>;
     _ -> throw (string_too_big)
  end.

read_name (<<Length:8/integer-unsigned-big,
             EventName:Length/binary,
             _NumAttrs:16/integer-unsigned-big,
             Rest/binary>>) ->
  { ok, EventName, Rest };
read_name (_) ->
  { error, malformed_event }.

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
                 F when F=:= json; F =:= json_untyped ->
                   [ {K, value_from_json (Format, V) }
                     | Accum ];
                 json_typed ->
                   [ {type_to_atom(T), K, value_from_json (Format, V) }
                     | Accum ];
                 F when F =:= json_proplist; F =:= json_proplist_untyped ->
                   [ {K, value_from_json (Format, V) }
                     | Accum ];
                 json_proplist_typed ->
                   [ {type_to_atom(T), K, value_from_json (Format, V) }
                     | Accum ];
                 F when F =:= json_eep18; F =:= json_eep18_untyped ->
                   [ {K, value_from_json (Format, V) }
                     | Accum ];
                 json_eep18_typed ->
                   [ {type_to_atom(T), K, value_from_json (Format, V) }
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
read_value (?LWES_TYPE_IP_ADDR, Bin, Format)
  when Format =:= json; Format =:= json_proplist; Format =:= json_eep18  ->
  <<V1:8/integer-unsigned-big,
    V2:8/integer-unsigned-big,
    V3:8/integer-unsigned-big,
    V4:8/integer-unsigned-big, Rest/binary>> = Bin,
  { lwes_util:ip2bin ({V4,V3,V2,V1}), Rest };
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
read_value (?LWES_TYPE_BYTE, Bin, _Format) ->
  <<V:8/integer-unsigned-big, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_FLOAT, Bin, _Format) ->
  <<V:32/float, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_DOUBLE, Bin, _Format) ->
  <<V:64/float, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_LONG_STRING, Bin, _Format) ->
  <<BL:32/integer-unsigned-big, V:BL/binary, Rest/binary>> = Bin,
  { V, Rest };
read_value (?LWES_TYPE_U_INT_16_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  Count = AL*16,
  <<Ints:Count/bits, Rest2/binary>> = Rest,
  { read_array (?LWES_TYPE_U_INT_16, Ints, Format, []), Rest2 };
read_value (?LWES_TYPE_INT_16_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  Count = AL*16,
  <<Ints:Count/bits, Rest2/binary>> = Rest,
  { read_array (?LWES_TYPE_INT_16, Ints, Format, []), Rest2 };
read_value (?LWES_TYPE_U_INT_32_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  Count = AL*32,
  <<Ints:Count/bits, Rest2/binary>> = Rest,
  { read_array (?LWES_TYPE_U_INT_32, Ints, Format, []), Rest2 };
read_value (?LWES_TYPE_INT_32_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  Count = AL*32,
  <<Ints:Count/bits, Rest2/binary>> = Rest,
  { read_array (?LWES_TYPE_INT_32, Ints, Format,  []), Rest2 };
read_value (?LWES_TYPE_U_INT_64_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  Count = AL*64,
  <<Ints:Count/bits, Rest2/binary>> = Rest,
  { read_array (?LWES_TYPE_U_INT_64, Ints, Format, []), Rest2 };
read_value (?LWES_TYPE_INT_64_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  Count = AL*64,
  <<Ints:Count/bits, Rest2/binary>> = Rest,
  { read_array (?LWES_TYPE_INT_64, Ints, Format, []), Rest2 };
read_value (?LWES_TYPE_IP_ADDR_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  Count = AL*4,
  <<Ips:Count/binary, Rest2/binary>> = Rest,
  { read_array (?LWES_TYPE_IP_ADDR, Ips, Format, []), Rest2 };
read_value (?LWES_TYPE_BOOLEAN_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  Count = AL*1,
  <<Bools:Count/binary, Rest2/binary>> =  Rest,
  { read_array (?LWES_TYPE_BOOLEAN, Bools, Format, []), Rest2 };
read_value (?LWES_TYPE_STRING_ARRAY, Bin, _Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  read_string_array (AL, Rest, []);
read_value (?LWES_TYPE_BYTE_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  <<Bytes:AL/binary, Rest2/binary>> = Rest,
  { read_array (?LWES_TYPE_BYTE, Bytes, Format, []), Rest2 };
read_value (?LWES_TYPE_FLOAT_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  Count = AL*32,
  <<Floats:Count/bits, Rest2/binary>> = Rest,
  { read_array (?LWES_TYPE_FLOAT, Floats, Format, []), Rest2 };
read_value (?LWES_TYPE_DOUBLE_ARRAY, Bin, Format) ->
  <<AL:16/integer-unsigned-big, Rest/binary>> = Bin,
  Count = AL*64,
  <<Doubles:Count/bits, Rest2/binary>> = Rest,
  { read_array (?LWES_TYPE_DOUBLE, Doubles, Format, []), Rest2 };
read_value (?LWES_TYPE_N_U_INT_16_ARRAY, Bin, Format) ->
  ?read_nullable_array(Bin, ?LWES_TYPE_U_INT_16, 16);
read_value (?LWES_TYPE_N_INT_16_ARRAY, Bin, Format) ->
  ?read_nullable_array(Bin, ?LWES_TYPE_INT_16, 16);
read_value (?LWES_TYPE_N_U_INT_32_ARRAY, Bin, Format) ->
  ?read_nullable_array(Bin, ?LWES_TYPE_U_INT_32, 32);
read_value (?LWES_TYPE_N_INT_32_ARRAY, Bin, Format) ->
  ?read_nullable_array(Bin, ?LWES_TYPE_INT_32, 32);
read_value (?LWES_TYPE_N_U_INT_64_ARRAY, Bin, Format) ->
  ?read_nullable_array(Bin, ?LWES_TYPE_U_INT_64, 64);
read_value (?LWES_TYPE_N_INT_64_ARRAY, Bin, Format) ->
  ?read_nullable_array(Bin, ?LWES_TYPE_INT_64, 64);
read_value (?LWES_TYPE_N_BOOLEAN_ARRAY, Bin, Format) ->
  ?read_nullable_array(Bin, ?LWES_TYPE_BOOLEAN, 8);
read_value (?LWES_TYPE_N_STRING_ARRAY, Bin, _) ->
  <<AL:16/integer-unsigned-big, _:16, Rest/binary>> = Bin,
  {_, Bitset_Length, Bitset} = decode_bitset(AL, Rest),
  <<_:Bitset_Length, Rest2/binary>> = Rest,
  read_n_string_array (AL, 1, Bitset, Rest2, []);
read_value (?LWES_TYPE_N_BYTE_ARRAY, Bin, Format) ->
  ?read_nullable_array(Bin, ?LWES_TYPE_BYTE, 8);
read_value (?LWES_TYPE_N_FLOAT_ARRAY, Bin, Format) ->
  ?read_nullable_array(Bin, ?LWES_TYPE_FLOAT, 32);
read_value (?LWES_TYPE_N_DOUBLE_ARRAY, Bin, Format) ->
  ?read_nullable_array(Bin, ?LWES_TYPE_DOUBLE, 64);
read_value (_, _, _) ->
  throw (unknown_type).


%% ARRAY TYPE FUNCS
read_array (_Type, <<>>, _Format, Acc) -> lists:reverse (Acc);
read_array (Type, Bin, Format, Acc) ->
  { V, Rest } = read_value (Type, Bin, Format),
  read_array (Type, Rest, Format, [V] ++ Acc).

read_n_array (_Type, Count, Index, _Bitset, _Bin, _Format, Acc) when Index > Count -> lists:reverse (Acc);
read_n_array (Type, Count, Index, Bitset, Bin, Format, Acc) when Index =< Count ->
  {L, R} = split_bounds(Index, Bitset),
  << _:L/bits, X:1, _:R/bits >> = Bitset,
  { V, Rest } = case X of 0 -> {undefined, Bin};
                          1 -> read_value (Type, Bin, Format)
                end,
  read_n_array (Type, Count, Index + 1, Bitset, Rest, Format, [V] ++ Acc).

read_string_array (0, Bin, Acc) -> { lists:reverse (Acc), Bin };
read_string_array (Count, Bin, Acc) ->
  { V, Rest } = read_value (?LWES_TYPE_STRING, Bin, undefined),
  read_string_array (Count-1, Rest, [V] ++ Acc).

read_n_string_array (Count, Index, _Bitset, Bin, Acc) when Index > Count -> { lists:reverse (Acc), Bin };
read_n_string_array (Count, Index, Bitset, Bin, Acc) when Index =< Count ->
  {L, R} = split_bounds(Index, Bitset),
  << _:L/bits, X:1, _:R/bits >> = Bitset,
  { V, Rest } = case X of 0 -> {undefined, Bin};
                          1 -> read_value (?LWES_TYPE_STRING, Bin, undefined)
                end,
  read_n_string_array (Count, Index + 1,Bitset, Rest, [V] ++ Acc).

string_array_to_binary (L) -> string_array_to_binary (L, []).
string_array_to_binary ([], Acc) -> lists:reverse (Acc);
string_array_to_binary ([ H | T ], Acc) when is_binary (H) ->
  string_array_to_binary (T, [H] ++ Acc);
string_array_to_binary ([ H | T ], Acc) when is_list (H) ->
  string_array_to_binary (T, [list_to_binary (H)] ++ Acc);
string_array_to_binary ([ H | T ], Acc) when is_atom (H) ->
  case H of
    undefined -> string_array_to_binary (T, [ undefined ] ++ Acc);
    _ -> string_array_to_binary (T,
                    [ list_to_binary (atom_to_list (H)) ] ++ Acc)
  end;
string_array_to_binary (_, _) ->
  erlang:error (badarg).

to_json(Bin) when is_binary (Bin) ->
  from_binary(Bin, json_eep18_typed);
to_json(Event= #lwes_event{name=_, attrs=_}) ->
  to_json(Event, json_eep18_typed).

to_json (Bin, Format) when is_binary (Bin) ->
  from_binary (Bin, Format);
to_json (Event, Format) ->
  from_binary (to_binary (Event), Format).

export_attribute (Format, {Key, Value}) ->
  Type = infer_type (Value),
  export_attribute (Format, {Type, Key, Value});
export_attribute (json_typed, {Type, _, Value}) ->
  {struct, [ {<<"type">>, Type},
             {<<"value">>, make_binary (Type, Value)} ] };
export_attribute (json_proplist_typed, {Type, _, Value}) ->
  [{<<"type">>, Type},
   {<<"value">>, make_binary (Type, Value)}];
export_attribute (json_eep18_typed, {Type, _, Value}) ->
  {[{<<"type">>, Type},
    {<<"value">>, make_binary (Type, Value)}]}.

export_attributes(Format, Attrs) ->
  lists:foldl (
    fun
      (E = {Key, _}, A) ->
        [ {Key, export_attribute (Format, E)} | A];
      (E = {_, Key, _}, A) ->
        [ {Key, export_attribute (Format, E)} | A]
    end,
    [],
    Attrs).

from_json(Bin) when is_list(Bin); is_binary(Bin) ->
  from_json (lwes_mochijson2:decode (Bin, [{format, eep18}]));
from_json ({Json}) ->
  Name = proplists:get_value (<<"EventName">>, Json),
  {TypedAttrs} = proplists:get_value (<<"typed">>, Json),
  #lwes_event {
    name = Name,
    attrs = lists:map (fun process_one/1, TypedAttrs)
  }.

make_binary(Type, Value) ->
  case is_arr_type(Type) of
    true -> lwes_util:arr_to_binary(Value);
    false -> lwes_util:any_to_binary(Value)
  end.

value_from_json (Format, Value)
  when Format =:= json;
       Format =:= json_untyped;
       Format =:= json_typed ->
  try lwes_mochijson2:decode (Value, [{format, struct}]) of
    S -> S
  catch
    _:_ -> Value
  end;
value_from_json (Format, Value)
  when Format =:= json_proplist;
       Format =:= json_proplist_untyped;
       Format =:= json_proplist_typed ->
  try lwes_mochijson2:decode (Value, [{format, proplist}]) of
    S -> S
  catch
    _:_ -> Value
  end;
value_from_json (Format, Value)
  when Format =:= json_eep18;
       Format =:= json_eep18_untyped;
       Format =:= json_eep18_typed ->
  try lwes_mochijson2:decode (Value, [{format, eep18}]) of
    S -> S
  catch
    _:_ -> Value
  end.

process_one ({Key, {Attrs}}) ->
  Type = case proplists:get_value (<<"type">>, Attrs) of
           A when is_atom(A) -> A;
           B when is_binary(B) ->
             lwes_util:binary_to_any (B, atom)
         end,
  Value = proplists:get_value (<<"value">>, Attrs),
  NewValue =
    case Type of
      ?LWES_U_INT_16 -> lwes_util:binary_to_any (Value, integer);
      ?LWES_INT_16 -> lwes_util:binary_to_any (Value, integer);
      ?LWES_U_INT_32 -> lwes_util:binary_to_any (Value, integer);
      ?LWES_INT_32 -> lwes_util:binary_to_any (Value, integer);
      ?LWES_U_INT_64 -> lwes_util:binary_to_any (Value, integer);
      ?LWES_INT_64 -> lwes_util:binary_to_any (Value, integer);
      ?LWES_IP_ADDR -> lwes_util:binary_to_any (Value, ipaddr);
      ?LWES_BOOLEAN -> lwes_util:binary_to_any (Value, atom);
      ?LWES_STRING -> lwes_util:binary_to_any (Value, binary);
      ?LWES_BYTE -> lwes_util:binary_to_any (Value, integer);
      ?LWES_FLOAT -> lwes_util:binary_to_any (Value, float);
      ?LWES_DOUBLE -> lwes_util:binary_to_any (Value, float);
      ?LWES_U_INT_16_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_N_U_INT_16_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_INT_16_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_N_INT_16_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_U_INT_32_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_N_U_INT_32_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_INT_32_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_N_INT_32_ARRAY  -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_INT_64_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_N_INT_64_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_U_INT_64_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_N_U_INT_64_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_STRING_ARRAY -> lwes_util:binary_to_arr (Value, binary);
      ?LWES_N_STRING_ARRAY -> lwes_util:binary_to_arr (Value, binary);
      ?LWES_IP_ADDR_ARRAY -> lwes_util:binary_to_arr (Value, ipaddr);
      ?LWES_BOOLEAN_ARRAY -> lwes_util:binary_to_arr (Value, atom);
      ?LWES_N_BOOLEAN_ARRAY -> lwes_util:binary_to_arr (Value, atom);
      ?LWES_BYTE_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_N_BYTE_ARRAY -> lwes_util:binary_to_arr (Value, integer);
      ?LWES_FLOAT_ARRAY -> lwes_util:binary_to_arr (Value, float);
      ?LWES_N_FLOAT_ARRAY -> lwes_util:binary_to_arr (Value, float);
      ?LWES_DOUBLE_ARRAY -> lwes_util:binary_to_arr (Value, float);
      ?LWES_N_DOUBLE_ARRAY -> lwes_util:binary_to_arr (Value, float)
    end,
  { Type, Key, NewValue }.

is_arr_type (T) ->
    T == ?LWES_U_INT_16_ARRAY orelse T == ?LWES_N_U_INT_16_ARRAY orelse
    T == ?LWES_INT_16_ARRAY orelse T == ?LWES_N_INT_16_ARRAY orelse
    T == ?LWES_U_INT_32_ARRAY orelse T == ?LWES_N_U_INT_32_ARRAY orelse
    T == ?LWES_INT_32_ARRAY orelse T == ?LWES_N_INT_32_ARRAY orelse
    T == ?LWES_INT_64_ARRAY orelse T == ?LWES_N_INT_64_ARRAY orelse
    T == ?LWES_U_INT_64_ARRAY orelse T == ?LWES_N_U_INT_64_ARRAY orelse
    T == ?LWES_STRING_ARRAY orelse T == ?LWES_N_STRING_ARRAY orelse
    T == ?LWES_IP_ADDR_ARRAY orelse
    T == ?LWES_BOOLEAN_ARRAY orelse T == ?LWES_N_BOOLEAN_ARRAY orelse
    T == ?LWES_BYTE_ARRAY orelse T == ?LWES_N_BYTE_ARRAY orelse
    T == ?LWES_FLOAT_ARRAY orelse T == ?LWES_N_FLOAT_ARRAY orelse
    T == ?LWES_DOUBLE_ARRAY orelse T == ?LWES_N_DOUBLE_ARRAY.


%%====================================================================
%% Test functions
%%====================================================================
remove_attr (A, #lwes_event { name = N, attrs = L }) ->
  #lwes_event { name = N, attrs = remove_attr (A, L) };
remove_attr (A, {L}) when is_list (L) ->
  {remove_attr (A, L)};
remove_attr (A, {struct, L}) when is_list (L) ->
  {struct, remove_attr (A, L)};
remove_attr (A, L) when is_list (L) ->
  case lists:keyfind (<<"typed">>, 1, L) of
    false ->
      % for proplist style lists
      case lists:keydelete(A, 1, L) of
        L ->
          % for tagged style lists
          lists:keydelete(A, 2, L);
        L2 ->
          L2
      end;
    % deal with typed lists below
    {_, L2} when is_list(L2) ->
      lists:keyreplace (<<"typed">>,1,L,
                        {<<"typed">>,
                          lists:keydelete (A,1,L2)
                        });
    {_, {L2}} when is_list (L2) ->
      lists:keyreplace (<<"typed">>,1,L,
                        {<<"typed">>,
                          {lists:keydelete (A,1,L2)}
                        });
    {_,{struct,L3}} when is_list (L3) ->
      lists:keyreplace (<<"typed">>,1,L,
                        {<<"typed">>,
                         {struct, lists:keydelete (A,1,L3)}})
  end;
remove_attr (A, D) ->
  dict:erase (A, D).


-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

test_packet (binary) ->
  %% THIS IS A SERIALIZED PACKET
  %% SENT FROM THE JAVA LIBRARY
  %% THAT CONTAINS ALL TYPES
  %% captured from a java emitter at some point
  <<4,84,101,115,116,0,25,3,101,110,99,2,0,1,15,84,
    101,115,116,83,116,114,105,110,103,65,114,114,
    97,121,133,0,3,0,3,102,111,111,0,3,98,97,114,0,
    3,98,97,122,11,102,108,111,97,116,95,97,114,114,
    97,121,139,0,4,61,204,204,205,62,76,204,205,62,
    153,153,154,62,204,204,205,8,84,101,115,116,66,
    111,111,108,9,0,9,84,101,115,116,73,110,116,51,
    50,4,0,0,54,176,10,84,101,115,116,68,111,117,98,
    108,101,12,63,191,132,253,32,0,0,0,9,84,101,115,
    116,73,110,116,54,52,7,0,0,0,0,0,0,12,162,10,84,
    101,115,116,85,73,110,116,49,54,1,0,10,9,84,101,
    115,116,70,108,111,97,116,11,61,250,120,108,15,
    84,101,115,116,85,73,110,116,51,50,65,114,114,
    97,121,131,0,3,0,0,48,34,1,239,43,17,0,20,6,67,
    14,84,101,115,116,73,110,116,51,50,65,114,114,
    97,121,132,0,3,0,0,0,123,0,0,177,110,0,0,134,29,
    13,84,101,115,116,73,80,65,100,100,114,101,115,
    115,6,1,0,0,127,10,84,101,115,116,85,73,110,116,
    51,50,3,0,3,139,151,14,84,101,115,116,73,110,
    116,54,52,65,114,114,97,121,135,0,3,0,0,0,0,0,0,
    48,34,0,0,0,0,1,239,43,17,0,0,0,0,0,20,6,67,10,
    98,121,116,101,95,97,114,114,97,121,138,0,5,10,
    13,43,43,200,10,84,101,115,116,83,116,114,105,
    110,103,5,0,3,102,111,111,15,84,101,115,116,85,
    73,110,116,54,52,65,114,114,97,121,136,0,3,0,0,
    0,0,0,0,48,34,0,0,0,0,1,239,43,17,0,0,0,0,0,20,
    6,67,15,84,101,115,116,85,73,110,116,49,54,65,
    114,114,97,121,129,0,3,0,123,177,110,134,29,6,
    100,111,117,98,108,101,140,0,3,64,94,206,217,32,
    0,0,0,64,94,199,227,64,0,0,0,64,69,170,206,160,
    0,0,0,9,66,111,111,108,65,114,114,97,121,137,0,
    4,1,0,0,1,14,84,101,115,116,73,110,116,49,54,65,
    114,114,97,121,130,0,4,0,10,0,23,0,23,0,43,4,98,
    121,116,101,10,20,10,84,101,115,116,85,73,110,
    116,54,52,8,0,0,0,0,0,187,223,3,18,84,101,115,
    116,73,80,65,100,100,114,101,115,115,65,114,114,
    97,121,134,0,4,1,1,168,129,2,1,168,129,3,1,168,
    129,4,1,168,129,9,84,101,115,116,73,110,116,49,
    54,2,0,20>>;
test_packet (raw) ->
  {udp,port, {192,168,54,1}, 58206, test_packet(binary)};
test_packet (list) ->
  #lwes_event { name = <<"Test">>,
               attrs = [{<<"TestInt16">>,20},
                        {<<"TestIPAddressArray">>,
                         [{129,168,1,1},{129,168,1,2},
                          {129,168,1,3},{129,168,1,4}]},
                        {<<"TestUInt64">>,12312323},
                        {<<"byte">>,20},
                        {<<"TestInt16Array">>,[10,23,23,43]},
                        {<<"BoolArray">>,[true,false,false,true]},
                        {<<"double">>,
                         [123.23200225830078,123.12324523925781,
                          43.33443069458008]},
                        {<<"TestUInt16Array">>,[123,45422,34333]},
                        {<<"TestUInt64Array">>,[12322,32451345,1312323]},
                        {<<"TestString">>,<<"foo">>},
                        {<<"byte_array">>,[10,13,43,43,200]},
                        {<<"TestInt64Array">>,[12322,32451345,1312323]},
                        {<<"TestUInt32">>,232343},
                        {<<"TestIPAddress">>,{127,0,0,1}},
                        {<<"TestInt32Array">>,[123,45422,34333]},
                        {<<"TestUInt32Array">>,[12322,32451345,1312323]},
                        {<<"TestFloat">>,0.12229999899864197},
                        {<<"TestUInt16">>,10},
                        {<<"TestInt64">>,3234},
                        {<<"TestDouble">>,0.12312299758195877},
                        {<<"TestInt32">>,14000},
                        {<<"TestBool">>,false},
                        {<<"float_array">>,
                         [0.10000000149011612,0.20000000298023224,
                          0.30000001192092896, 0.4000000059604645]},
                        {<<"TestStringArray">>,[<<"foo">>,<<"bar">>,<<"baz">>]},
                        {<<"enc">>,1},
                        {<<"SenderIP">>,{192,168,54,1}},
                        {<<"SenderPort">>,58206},
                        {<<"ReceiptTime">>,1439588532973}]
  };
test_packet (tagged) ->
  #lwes_event { name = <<"Test">>,
               attrs = [ {int16,<<"TestInt16">>,20},
                         {ip_addr_array,<<"TestIPAddressArray">>,
                           [{129,168,1,1}, {129,168,1,2}, {129,168,1,3},
                            {129,168,1,4}]},
                        {uint64,<<"TestUInt64">>,12312323},
                        {byte,<<"byte">>,20},
                        {int16_array,<<"TestInt16Array">>,[10,23,23,43]},
                        {boolean_array,<<"BoolArray">>,
                         [true,false,false,true]},
                        {double_array,<<"double">>,
                         [123.23200225830078,123.12324523925781,
                          43.33443069458008]},
                        {uint16_array,<<"TestUInt16Array">>,[123,45422,34333]},
                        {uint64_array,<<"TestUInt64Array">>,[12322,32451345,1312323]},
                        {string,<<"TestString">>,<<"foo">>},
                        {byte_array,<<"byte_array">>,[10,13,43,43,200]},
                        {int64_array,<<"TestInt64Array">>,[12322,32451345,1312323]},
                        {uint32,<<"TestUInt32">>,232343},
                        {ip_addr,<<"TestIPAddress">>,{127,0,0,1}},
                        {int32_array,<<"TestInt32Array">>,[123,45422,34333]},
                        {uint32_array,<<"TestUInt32Array">>,[12322,32451345,1312323]},
                        {float,<<"TestFloat">>,0.12229999899864197},
                        {uint16,<<"TestUInt16">>,10},
                        {int64,<<"TestInt64">>,3234},
                        {double,<<"TestDouble">>,0.12312299758195877},
                        {int32,<<"TestInt32">>,14000},
                        {boolean,<<"TestBool">>,false},
                        {float_array,<<"float_array">>,
                         [0.10000000149011612,0.20000000298023224,
                          0.30000001192092896,0.4000000059604645]},
                        {string_array,<<"TestStringArray">>,
                         [<<"foo">>,<<"bar">>,<<"baz">>]},
                        {int16,<<"enc">>,1},
                        {ip_addr,<<"SenderIP">>,{192,168,54,1}},
                        {uint16,<<"SenderPort">>,58206},
                        {int64,<<"ReceiptTime">>,1439587738948}
                       ]
              };
test_packet (dict) ->
  #lwes_event {
    name = <<"Test">>,
    attrs =
      {dict,28,16,16,8,80,48,
       {[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]},
       {{[[<<"SenderPort">>|58206],
          [<<"ReceiptTime">>|1439588321444]],
         [[<<"TestDouble">>|0.12312299758195877],
          [<<"TestUInt32Array">>,12322,32451345,1312323]],
         [],
         [[<<"TestUInt16Array">>,123,45422,34333],
          [<<"TestInt16">>|20]],
         [[<<"TestBool">>|false],
          [<<"TestIPAddress">>|{127,0,0,1}],
          [<<"BoolArray">>,true,false,false,true],
          [<<"TestUInt64">>|12312323]],
         [[<<"enc">>|1],[<<"TestUInt32">>|232343]],
         [],[],
         [[<<"SenderIP">>|{192,168,54,1}],
          [<<"TestInt64Array">>,12322,32451345,1312323],
          [<<"TestIPAddressArray">>,
           {129,168,1,1},
           {129,168,1,2},
           {129,168,1,3},
           {129,168,1,4}]],
         [[<<"TestString">>|<<"foo">>]],
         [],
         [[<<"TestUInt16">>|10],
          [<<"TestFloat">>|0.12229999899864197],
          [<<"TestInt32Array">>,123,45422,34333]],
         [[<<"TestInt64">>|3234],
          [<<"byte_array">>,10,13,43,43,200],
          [<<"byte">>|20]],
         [[<<"TestStringArray">>,<<"foo">>,<<"bar">>,<<"baz">>],
          [<<"float_array">>,0.10000000149011612,
           0.20000000298023224,0.30000001192092896,
           0.4000000059604645],
          [<<"TestInt32">>|14000],
          [<<"TestInt16Array">>,10,23,23,43]],
         [[<<"TestUInt64Array">>,12322,32451345,1312323]],
         [[<<"double">>,123.23200225830078,123.12324523925781,
           43.33443069458008]]}}}
    };
test_packet (json) ->
  {struct,
    [ {<<"EventName">>,<<"Test">>},
      {<<"TestInt16">>,20},
      {<<"TestIPAddressArray">>,
         [<<"129.168.1.1">>, <<"129.168.1.2">>, <<"129.168.1.3">>,
          <<"129.168.1.4">>]},
      {<<"TestUInt64">>,12312323},
      {<<"byte">>,20},
      {<<"TestInt16Array">>,[10,23,23,43]},
      {<<"BoolArray">>,[true,false,false,true]},
      {<<"double">>,[123.23200225830078,123.12324523925781,43.33443069458008]},
      {<<"TestUInt16Array">>,[123,45422,34333]},
      {<<"TestUInt64Array">>,[12322,32451345,1312323]},
      {<<"TestString">>,<<"foo">>},
      {<<"byte_array">>,[10,13,43,43,200]},
      {<<"TestInt64Array">>,[12322,32451345,1312323]},
      {<<"TestUInt32">>,232343},
      {<<"TestIPAddress">>,<<"127.0.0.1">>},
      {<<"TestInt32Array">>,[123,45422,34333]},
      {<<"TestUInt32Array">>,[12322,32451345,1312323]},
      {<<"TestFloat">>,0.12229999899864197},
      {<<"TestUInt16">>,10},
      {<<"TestInt64">>,3234},
      {<<"TestDouble">>,0.12312299758195877},
      {<<"TestInt32">>,14000},
      {<<"TestBool">>,false},
      {<<"float_array">>,
         [0.10000000149011612,0.20000000298023224,0.30000001192092896,
             0.4000000059604645]},
      {<<"TestStringArray">>,[<<"foo">>,<<"bar">>,<<"baz">>]},
      {<<"enc">>,1},
      {<<"SenderIP">>,<<"192.168.54.1">>},
      {<<"SenderPort">>,58206},
      {<<"ReceiptTime">>,1439588435888}
    ]
  };
test_packet (json_untyped) ->
  test_packet (json);
test_packet (json_typed) ->
  {struct,
    [{<<"EventName">>,<<"Test">>},
     {<<"typed">>,
      {struct,
       [{<<"ReceiptTime">>,
         {struct,[{<<"type">>,int64},{<<"value">>,<<"1439585933710">>}]}
        },
        {<<"SenderPort">>,
         {struct,[{<<"type">>,uint16},{<<"value">>,<<"58206">>}]}
        },
        {<<"SenderIP">>,
         {struct,[{<<"type">>,ip_addr},{<<"value">>,<<"192.168.54.1">>}]}
        },
        {<<"enc">>,
         {struct,[{<<"type">>,int16},{<<"value">>,<<"1">>}]}
        },
        {<<"TestStringArray">>,
         {struct,[{<<"type">>,string_array},
                  {<<"value">>,[<<"foo">>,<<"bar">>,<<"baz">>]}]}
        },
        {<<"float_array">>,
         {struct,
          [{<<"type">>,float_array},
           {<<"value">>,
            [<<"1.00000001490116119385e-01">>,<<"2.00000002980232238770e-01">>,
             <<"3.00000011920928955078e-01">>,<<"4.00000005960464477539e-01">>]
           }
          ]}
        },
        {<<"TestBool">>,
         {struct,[{<<"type">>,boolean},{<<"value">>,<<"false">>}]}
        },
        {<<"TestInt32">>,
         {struct,[{<<"type">>,int32},{<<"value">>,<<"14000">>}]}
        },
        {<<"TestDouble">>,
         {struct,[{<<"type">>,double},
                  {<<"value">>,<<"1.23122997581958770752e-01">>}]}
        },
        {<<"TestInt64">>,
         {struct,[{<<"type">>,int64},{<<"value">>,<<"3234">>}]}
        },
        {<<"TestUInt16">>,
         {struct,[{<<"type">>,uint16},{<<"value">>,<<"10">>}]}},
        {<<"TestFloat">>,
         {struct,[{<<"type">>,float},
                  {<<"value">>,<<"1.22299998998641967773e-01">>}]}
        },
        {<<"TestUInt32Array">>,
         {struct, [{<<"type">>,uint32_array},
                   {<<"value">>,[<<"12322">>,<<"32451345">>,<<"1312323">>]}]}
        },
        {<<"TestInt32Array">>,
         {struct, [{<<"type">>,int32_array},
                   {<<"value">>,[<<"123">>,<<"45422">>,<<"34333">>]}]}
        },
        {<<"TestIPAddress">>,
         {struct, [{<<"type">>,ip_addr},{<<"value">>,<<"127.0.0.1">>}]}
        },
        {<<"TestUInt32">>,
         {struct, [{<<"type">>,uint32},{<<"value">>,<<"232343">>}]}
        },
        {<<"TestInt64Array">>,
         {struct, [{<<"type">>,int64_array},
                   {<<"value">>,[<<"12322">>,<<"32451345">>,<<"1312323">>]}]}
        },
        {<<"byte_array">>,
         {struct, [{<<"type">>,byte_array},
                   {<<"value">>,[<<"10">>,<<"13">>,<<"43">>,
                                 <<"43">>,<<"200">>]}]}
        },
        {<<"TestString">>,
         {struct,[{<<"type">>,string},{<<"value">>,<<"foo">>}]}
        },
        {<<"TestUInt64Array">>,
         {struct,[{<<"type">>,uint64_array},
                  {<<"value">>,[<<"12322">>,<<"32451345">>,<<"1312323">>]}]}
        },
        {<<"TestUInt16Array">>,
         {struct, [{<<"type">>,uint16_array},
                   {<<"value">>,[<<"123">>,<<"45422">>,<<"34333">>]}]}
        },
        {<<"double">>,
         {struct, [{<<"type">>,double_array},
                   {<<"value">>,
                    [<<"1.23232002258300781250e+02">>,
                     <<"1.23123245239257812500e+02">>,
                     <<"4.33344306945800781250e+01">>]}]}
        },
        {<<"BoolArray">>,
         {struct, [{<<"type">>,boolean_array},
                   {<<"value">>,[<<"true">>,<<"false">>,
                                 <<"false">>,<<"true">>]}]}
        },
        {<<"TestInt16Array">>,
         {struct, [{<<"type">>,int16_array},
                   {<<"value">>,[<<"10">>,<<"23">>,<<"23">>,<<"43">>]}]}
        },
        {<<"byte">>,{struct, [{<<"type">>,byte},{<<"value">>,<<"20">>}]}},
        {<<"TestUInt64">>,
         {struct, [{<<"type">>,uint64},{<<"value">>,<<"12312323">>}]}
        },
        {<<"TestIPAddressArray">>,
         {struct, [{<<"type">>,ip_addr_array},
                   {<<"value">>,
                    [<<"129.168.1.1">>,<<"129.168.1.2">>,<<"129.168.1.3">>,
                     <<"129.168.1.4">>]}]}
        },
        {<<"TestInt16">>,
         {struct, [{<<"type">>,int16},{<<"value">>,<<"20">>}]}}
       ]
      }
     }
    ]
  };
test_packet (json_proplist) ->
  [ {<<"EventName">>,<<"Test">>},
    {<<"TestInt16">>,20},
    {<<"TestIPAddressArray">>,
       [<<"129.168.1.1">>, <<"129.168.1.2">>, <<"129.168.1.3">>,
        <<"129.168.1.4">>]},
    {<<"TestUInt64">>,12312323},
    {<<"byte">>,20},
    {<<"TestInt16Array">>,[10,23,23,43]},
    {<<"BoolArray">>,[true,false,false,true]},
    {<<"double">>,[123.23200225830078,123.12324523925781,43.33443069458008]},
    {<<"TestUInt16Array">>,[123,45422,34333]},
    {<<"TestUInt64Array">>,[12322,32451345,1312323]},
    {<<"TestString">>,<<"foo">>},
    {<<"byte_array">>,[10,13,43,43,200]},
    {<<"TestInt64Array">>,[12322,32451345,1312323]},
    {<<"TestUInt32">>,232343},
    {<<"TestIPAddress">>,<<"127.0.0.1">>},
    {<<"TestInt32Array">>,[123,45422,34333]},
    {<<"TestUInt32Array">>,[12322,32451345,1312323]},
    {<<"TestFloat">>,0.12229999899864197},
    {<<"TestUInt16">>,10},
    {<<"TestInt64">>,3234},
    {<<"TestDouble">>,0.12312299758195877},
    {<<"TestInt32">>,14000},
    {<<"TestBool">>,false},
    {<<"float_array">>,
       [0.10000000149011612,0.20000000298023224,0.30000001192092896,
           0.4000000059604645]},
    {<<"TestStringArray">>,[<<"foo">>,<<"bar">>,<<"baz">>]},
    {<<"enc">>,1},
    {<<"SenderIP">>,<<"192.168.54.1">>},
    {<<"SenderPort">>,58206},
    {<<"ReceiptTime">>,1439588435888}
  ];
test_packet (json_proplist_untyped) ->
  test_packet (json_proplist);
test_packet (json_proplist_typed) ->
  [{<<"EventName">>,<<"Test">>},
   {<<"typed">>,
    [{<<"ReceiptTime">>,[{<<"type">>,int64},{<<"value">>,<<"1439585933710">>}]},
     {<<"SenderPort">>,[{<<"type">>,uint16},{<<"value">>,<<"58206">>}]},
     {<<"SenderIP">>,[{<<"type">>,ip_addr},{<<"value">>,<<"192.168.54.1">>}]},
     {<<"enc">>,[{<<"type">>,int16},{<<"value">>,<<"1">>}]},
     {<<"TestStringArray">>,
      [{<<"type">>,string_array},
       {<<"value">>,[<<"foo">>,<<"bar">>,<<"baz">>]}]},
     {<<"float_array">>,
      [{<<"type">>,float_array},
       {<<"value">>,
        [<<"1.00000001490116119385e-01">>,<<"2.00000002980232238770e-01">>,
         <<"3.00000011920928955078e-01">>,<<"4.00000005960464477539e-01">>]}]
     },
     {<<"TestBool">>,[{<<"type">>,boolean},{<<"value">>,<<"false">>}]},
     {<<"TestInt32">>,[{<<"type">>,int32},{<<"value">>,<<"14000">>}]},
     {<<"TestDouble">>,
      [{<<"type">>,double},{<<"value">>,<<"1.23122997581958770752e-01">>}]},
     {<<"TestInt64">>,[{<<"type">>,int64},{<<"value">>,<<"3234">>}]},
     {<<"TestUInt16">>,[{<<"type">>,uint16},{<<"value">>,<<"10">>}]},
     {<<"TestFloat">>,
      [{<<"type">>,float},{<<"value">>,<<"1.22299998998641967773e-01">>}]},
     {<<"TestUInt32Array">>,
      [{<<"type">>,uint32_array},
       {<<"value">>,[<<"12322">>,<<"32451345">>,<<"1312323">>]}]},
     {<<"TestInt32Array">>,
      [{<<"type">>,int32_array},
       {<<"value">>,[<<"123">>,<<"45422">>,<<"34333">>]}]},
     {<<"TestIPAddress">>,[{<<"type">>,ip_addr},{<<"value">>,<<"127.0.0.1">>}]},
     {<<"TestUInt32">>,[{<<"type">>,uint32},{<<"value">>,<<"232343">>}]},
     {<<"TestInt64Array">>,
      [{<<"type">>,int64_array},
       {<<"value">>,[<<"12322">>,<<"32451345">>,<<"1312323">>]}]},
     {<<"byte_array">>,
      [{<<"type">>,byte_array},
       {<<"value">>,[<<"10">>,<<"13">>,<<"43">>,<<"43">>,<<"200">>]}]},
     {<<"TestString">>,[{<<"type">>,string},{<<"value">>,<<"foo">>}]},
     {<<"TestUInt64Array">>,
      [{<<"type">>,uint64_array},
       {<<"value">>,[<<"12322">>,<<"32451345">>,<<"1312323">>]}]},
     {<<"TestUInt16Array">>,
      [{<<"type">>,uint16_array},
       {<<"value">>,[<<"123">>,<<"45422">>,<<"34333">>]}]},
     {<<"double">>,
      [{<<"type">>,double_array},
       {<<"value">>,
        [<<"1.23232002258300781250e+02">>,<<"1.23123245239257812500e+02">>,
         <<"4.33344306945800781250e+01">>]}]},
     {<<"BoolArray">>,
      [{<<"type">>,boolean_array},
       {<<"value">>,[<<"true">>,<<"false">>,<<"false">>,<<"true">>]}]},
     {<<"TestInt16Array">>,
      [{<<"type">>,int16_array},
       {<<"value">>,[<<"10">>,<<"23">>,<<"23">>,<<"43">>]}]},
     {<<"byte">>,[{<<"type">>,byte},{<<"value">>,<<"20">>}]},
     {<<"TestUInt64">>,[{<<"type">>,uint64},{<<"value">>,<<"12312323">>}]},
     {<<"TestIPAddressArray">>,
      [{<<"type">>,ip_addr_array},
       {<<"value">>,
        [<<"129.168.1.1">>,<<"129.168.1.2">>,<<"129.168.1.3">>,
         <<"129.168.1.4">>]}]},
     {<<"TestInt16">>,[{<<"type">>,int16},{<<"value">>,<<"20">>}]}]
   }
  ];
test_packet (json_eep18) ->
  {test_packet (json_proplist)};
test_packet (json_eep18_untyped) ->
  test_packet (json_eep18);
test_packet (json_eep18_typed) ->
  {[{<<"EventName">>,<<"Test">>},
    {<<"typed">>,
     {[{ <<"ReceiptTime">>,
         {[{<<"type">>,int64},{<<"value">>,<<"1439592736063">>}]}
       },
       { <<"SenderPort">>,
         {[{<<"type">>,uint16},{<<"value">>,<<"58206">>}]}
       },
       { <<"SenderIP">>,
         {[{<<"type">>,ip_addr},{<<"value">>,<<"192.168.54.1">>}]}
       },
       {<<"enc">>,{[{<<"type">>,int16},{<<"value">>,<<"1">>}]}},
       {<<"TestStringArray">>,
        {[{<<"type">>,string_array},
          {<<"value">>,[<<"foo">>,<<"bar">>,<<"baz">>]}]}
       },
       {<<"float_array">>,
        {[{<<"type">>,float_array},
          {<<"value">>,
           [<<"1.00000001490116119385e-01">>,<<"2.00000002980232238770e-01">>,
            <<"3.00000011920928955078e-01">>,<<"4.00000005960464477539e-01">>]
          }
         ]}
       },
       {<<"TestBool">>,{[{<<"type">>,boolean},{<<"value">>,<<"false">>}]}},
       {<<"TestInt32">>,{[{<<"type">>,int32},{<<"value">>,<<"14000">>}]}},
       {<<"TestDouble">>,
        {[{<<"type">>,double},{<<"value">>,<<"1.23122997581958770752e-01">>}]}},
       {<<"TestInt64">>,{[{<<"type">>,int64},{<<"value">>,<<"3234">>}]}},
       {<<"TestUInt16">>,{[{<<"type">>,uint16},{<<"value">>,<<"10">>}]}},
       {<<"TestFloat">>,
        {[{<<"type">>,float},{<<"value">>,<<"1.22299998998641967773e-01">>}]}},
       {<<"TestUInt32Array">>,
        {[{<<"type">>,uint32_array},
          {<<"value">>,[<<"12322">>,<<"32451345">>,<<"1312323">>]}]}
       },
       {<<"TestInt32Array">>,
        {[{<<"type">>,int32_array},
          {<<"value">>,[<<"123">>,<<"45422">>,<<"34333">>]}]}},
       {<<"TestIPAddress">>,
        {[{<<"type">>,ip_addr},{<<"value">>,<<"127.0.0.1">>}]}},
       {<<"TestUInt32">>,{[{<<"type">>,uint32},{<<"value">>,<<"232343">>}]}},
       {<<"TestInt64Array">>,
        {[{<<"type">>,int64_array},
          {<<"value">>,[<<"12322">>,<<"32451345">>,<<"1312323">>]}]}},
       {<<"byte_array">>,
        {[{<<"type">>,byte_array},
          {<<"value">>,[<<"10">>,<<"13">>,<<"43">>,<<"43">>,<<"200">>]}]}},
       {<<"TestString">>,{[{<<"type">>,string},{<<"value">>,<<"foo">>}]}},
       {<<"TestUInt64Array">>,
        {[{<<"type">>,uint64_array},
          {<<"value">>,[<<"12322">>,<<"32451345">>,<<"1312323">>]}]}},
       {<<"TestUInt16Array">>,
        {[{<<"type">>,uint16_array},
          {<<"value">>,[<<"123">>,<<"45422">>,<<"34333">>]}]}},
       {<<"double">>,
        {[{<<"type">>,double_array},
          {<<"value">>,
           [<<"1.23232002258300781250e+02">>,<<"1.23123245239257812500e+02">>,
            <<"4.33344306945800781250e+01">>]}]}},
       {<<"BoolArray">>,
        {[{<<"type">>,boolean_array},
          {<<"value">>,[<<"true">>,<<"false">>,<<"false">>,<<"true">>]}]}},
       {<<"TestInt16Array">>,
        {[{<<"type">>,int16_array},
          {<<"value">>,[<<"10">>,<<"23">>,<<"23">>,<<"43">>]}]}},
       {<<"byte">>,{[{<<"type">>,byte},{<<"value">>,<<"20">>}]}},
       {<<"TestUInt64">>,{[{<<"type">>,uint64},{<<"value">>,<<"12312323">>}]}},
       {<<"TestIPAddressArray">>,
        {[{<<"type">>,ip_addr_array},
          {<<"value">>,
           [<<"129.168.1.1">>,<<"129.168.1.2">>,<<"129.168.1.3">>,
            <<"129.168.1.4">>]}]}},
       {<<"TestInt16">>,{[{<<"type">>,int16},{<<"value">>,<<"20">>}]}}
      ]
     }
    }
   ]
  }.

nested_event (event) ->
  #lwes_event {
    name = <<"test">>,
    attrs =
      [ {<<"foo">>,<<"{\"bar\":\"baz\",\"bob\":5,\"inner\":{\"a\":5,\"b\":[1,2,3],\"c\":\"another\"}}">>},
        {<<"other">>,<<"[1,2,3,4]">>},
        {<<"ip">>, {127,0,0,1}}
      ]
    };
nested_event (binary) ->
  <<4,116,101,115,116,0,3,2,105,112,6,1,0,0,127,5,111,116,104,101,114,5,
    0,9,91,49,44,50,44,51,44,52,93,3,102,111,111,5,0,63,123,34,98,97,114,34,
    58,34,98,97,122,34,44,34,98,111,98,34,58,53,44,34,105,110,110,101,114,34,
    58,123,34,97,34,58,53,44,34,98,34,58,91,49,44,50,44,51,93,44,34,99,34,58,
    34,97,110,111,116,104,101,114,34,125,125>>;
nested_event (json) ->
  {struct,[{<<"EventName">>,<<"test">>},
           {<<"foo">>,
            {struct,[{<<"bar">>,<<"baz">>},
                     {<<"bob">>,5},
                     {<<"inner">>, {struct,[{<<"a">>,5},
                                            {<<"b">>,[1,2,3]},
                                            {<<"c">>,<<"another">>}]
                                   }
                     }
                    ]
            }
           },
           {<<"other">>,[1,2,3,4]},
           {<<"ip">>,<<"127.0.0.1">>}
          ]
  };
nested_event (json_untyped) ->
  nested_event (json);
nested_event (json_typed) ->
  {struct,
   [{<<"EventName">>,<<"test">>},
    {<<"typed">>,
     {struct,
      [{<<"ip">>,
        {struct,[{<<"type">>,ip_addr},{<<"value">>,<<"127.0.0.1">>}]}},
       {<<"other">>,
        {struct,[{<<"type">>,string},{<<"value">>,<<1,2,3,4>>}]}},
       {<<"foo">>,
        {struct,
         [{<<"type">>,string},
          {<<"value">>,
           {struct,
            [{<<"bar">>,<<"baz">>},
             {<<"bob">>,5},
             {<<"inner">>,
              {struct,
               [{<<"a">>,5},
                {<<"b">>,[1,2,3]},
                {<<"c">>,<<"another">>}]}}]}}]}}]}}]};
nested_event (json_proplist) ->
  [{<<"EventName">>,<<"test">>},
    {<<"foo">>,
       [{<<"bar">>,<<"baz">>},
        {<<"bob">>,5},
        {<<"inner">>,
          [{<<"a">>,5},
           {<<"b">>,[1,2,3]},
           {<<"c">>,<<"another">>}
          ]
        }
       ]
    },
    {<<"other">>,[1,2,3,4]},
    {<<"ip">>,<<"127.0.0.1">>}
  ];
nested_event (json_proplist_untyped) ->
  nested_event (json_proplist);
nested_event (json_proplist_typed) ->
  [{<<"EventName">>,<<"test">>},
   {<<"typed">>,
    [{<<"ip">>,[{<<"type">>,ip_addr},{<<"value">>,<<"127.0.0.1">>}]},
     {<<"other">>,
      [{<<"type">>,string},{<<"value">>,<<1,2,3,4>>}]},
     {<<"foo">>,
      [{<<"type">>,string},
       {<<"value">>,
        [{<<"bar">>,<<"baz">>},
         {<<"bob">>,5},
         {<<"inner">>,
          [{<<"a">>,5},
           {<<"b">>,[1,2,3]},
           {<<"c">>,<<"another">>}]}]}]}]}];
nested_event (json_eep18) ->
  {[{<<"EventName">>,<<"test">>},
    {<<"foo">>,
     {[{<<"bar">>,<<"baz">>},
       {<<"bob">>,5},
       {<<"inner">>,
        {[{<<"a">>,5},
          {<<"b">>,[1,2,3]},
          {<<"c">>,<<"another">>}]}}]}},
    {<<"other">>,[1,2,3,4]},
    {<<"ip">>,<<"127.0.0.1">>}]};
nested_event (json_eep18_untyped) ->
  nested_event (json_eep18);
nested_event (json_eep18_typed) ->
  {[{<<"EventName">>,<<"test">>},
    {<<"typed">>,
     {[{<<"ip">>,{[{<<"type">>,ip_addr},{<<"value">>,<<"127.0.0.1">>}]}},
       {<<"other">>,{[{<<"type">>,string},{<<"value">>,<<1,2,3,4>>}]}},
       {<<"foo">>,
        {[{<<"type">>,string},
          {<<"value">>,
           {[{<<"bar">>,<<"baz">>},
             {<<"bob">>,5},
             {<<"inner">>,
              {[{<<"a">>,5},
                {<<"b">>,[1,2,3]},
                {<<"c">>,<<"another">>}]}}]}}]}}]}}]}.

test_text () ->
  <<"{\"EventName\":\"Test\",\"typed\":{\"SenderPort\":{\"type\":\"uint16\",\"value\":\"58206\"},\"SenderIP\":{\"type\":\"ip_addr\",\"value\":\"192.168.54.1\"},\"enc\":{\"type\":\"int16\",\"value\":\"1\"},\"TestStringArray\":{\"type\":\"string_array\",\"value\":[\"foo\",\"bar\",\"baz\"]},\"float_array\":{\"type\":\"float_array\",\"value\":[\"1.00000001490116119385e-01\",\"2.00000002980232238770e-01\",\"3.00000011920928955078e-01\",\"4.00000005960464477539e-01\"]},\"TestBool\":{\"type\":\"boolean\",\"value\":\"false\"},\"TestInt32\":{\"type\":\"int32\",\"value\":\"14000\"},\"TestDouble\":{\"type\":\"double\",\"value\":\"1.23122997581958770752e-01\"},\"TestInt64\":{\"type\":\"int64\",\"value\":\"3234\"},\"TestUInt16\":{\"type\":\"uint16\",\"value\":\"10\"},\"TestFloat\":{\"type\":\"float\",\"value\":\"1.22299998998641967773e-01\"},\"TestUInt32Array\":{\"type\":\"uint32_array\",\"value\":[\"12322\",\"32451345\",\"1312323\"]},\"TestInt32Array\":{\"type\":\"int32_array\",\"value\":[\"123\",\"45422\",\"34333\"]},\"TestIPAddress\":{\"type\":\"ip_addr\",\"value\":\"127.0.0.1\"},\"TestUInt32\":{\"type\":\"uint32\",\"value\":\"232343\"},\"TestInt64Array\":{\"type\":\"int64_array\",\"value\":[\"12322\",\"32451345\",\"1312323\"]},\"byte_array\":{\"type\":\"byte_array\",\"value\":[\"10\",\"13\",\"43\",\"43\",\"200\"]},\"TestString\":{\"type\":\"string\",\"value\":\"foo\"},\"TestUInt64Array\":{\"type\":\"uint64_array\",\"value\":[\"12322\",\"32451345\",\"1312323\"]},\"TestUInt16Array\":{\"type\":\"uint16_array\",\"value\":[\"123\",\"45422\",\"34333\"]},\"double\":{\"type\":\"double_array\",\"value\":[\"1.23232002258300781250e+02\",\"1.23123245239257812500e+02\",\"4.33344306945800781250e+01\"]},\"BoolArray\":{\"type\":\"boolean_array\",\"value\":[\"true\",\"false\",\"false\",\"true\"]},\"TestInt16Array\":{\"type\":\"int16_array\",\"value\":[\"10\",\"23\",\"23\",\"43\"]},\"byte\":{\"type\":\"byte\",\"value\":\"20\"},\"TestUInt64\":{\"type\":\"uint64\",\"value\":\"12312323\"},\"TestIPAddressArray\":{\"type\":\"ip_addr_array\",\"value\":[\"129.168.1.1\",\"129.168.1.2\",\"129.168.1.3\",\"129.168.1.4\"]},\"TestInt16\":{\"type\":\"int16\",\"value\":\"20\"}}}">>.


json_formats () ->
  [
    json, json_untyped, json_typed,
    json_proplist, json_proplist_untyped, json_proplist_typed,
    json_eep18, json_eep18_untyped, json_eep18_typed
  ].
formats() -> [ list, tagged, dict | json_formats() ].


new_test_ () ->
  [
    ?_assertEqual (#lwes_event {name = "foo", attrs = []},
                   new (foo)),
    % INT16 tests
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_INT_16,cat,0}]},
                   set_int16 (new(foo),cat,0)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_INT_16,cat,-5}]},
                   set_int16 (new(foo),cat,-5)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_INT_16,cat,50}]},
                   set_int16 (new(foo),cat,50)),
    % INT16 bounds tests
    ?_assertError (badarg,
                   set_int16 (new(foo),cat,32768)),
    ?_assertError (badarg,
                   set_int16 (new(foo),cat,-32769)),
    % UINT16 tests
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_U_INT_16,cat,0}]},
                   set_uint16 (new(foo),cat,0)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_U_INT_16,cat,5}]},
                   set_uint16 (new(foo),cat,5)),
    % UINT16 bounds tests
    ?_assertError (badarg,
                   set_uint16 (new(foo),cat,-5)),
    ?_assertError (badarg,
                   set_uint16 (new(foo),cat,65536)),
    % INT32 tests
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_INT_32,cat,0}]},
                   set_int32 (new(foo),cat,0)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_INT_32,cat,-5}]},
                   set_int32 (new(foo),cat,-5)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_INT_32,cat,50}]},
                   set_int32 (new(foo),cat,50)),
    % INT32 bounds tests
    ?_assertError (badarg,
                   set_int32 (new(foo),cat,2147483648)),
    ?_assertError (badarg,
                   set_int32 (new(foo),cat,-2147483649)),
    % UINT32 tests
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_U_INT_32,cat,0}]},
                   set_uint32 (new(foo),cat,0)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_U_INT_32,cat,50}]},
                   set_uint32 (new(foo),cat,50)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_U_INT_32,cat,4294967295}]},
                   set_uint32 (new(foo),cat,4294967295)),
    % UINT32 bounds tests
    ?_assertError (badarg,
                   set_uint32 (new(foo),cat,-5)),
    ?_assertError (badarg,
                   set_uint32 (new(foo),cat,4294967296)),
    % INT64 tests
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_INT_64,cat,0}]},
                   set_int64 (new(foo),cat,0)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_INT_64,cat,-5}]},
                   set_int64 (new(foo),cat,-5)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_INT_64,cat,-9223372036854775808}]},
                   set_int64 (new(foo),cat,-9223372036854775808)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_INT_64,cat,9223372036854775807}]},
                   set_int64 (new(foo),cat,9223372036854775807)),
    % INT64 bounds tests
    ?_assertError (badarg,
                   set_int64 (new(foo),cat,9223372036854775808)),
    ?_assertError (badarg,
                   set_int64 (new(foo),cat,-9223372036854775809)),
    % UINT64 tests
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_U_INT_64,cat,0}]},
                   set_uint64 (new(foo),cat,0)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_U_INT_64,cat,50}]},
                   set_uint64 (new(foo),cat,50)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_U_INT_64,cat,18446744073709551615}]},
                   set_uint64 (new(foo),cat,18446744073709551615)),
    % UINT64 bounds tests
    ?_assertError (badarg,
                   set_uint64 (new(foo),cat,-5)),
    ?_assertError (badarg,
                   set_uint64 (new(foo),cat,18446744073709551616)),
    % BOOLEAN tests
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_BOOLEAN,cat,true}]},
                   set_boolean (new(foo),cat,true)),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_BOOLEAN,cat,false}]},
                   set_boolean (new(foo),cat,false)),
    ?_assertError (badarg,
                   set_boolean (new(foo),cat,kinda)),
    % IP_ADDR test
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_IP_ADDR,cat,{127,0,0,1}}]},
                   set_ip_addr (new(foo),cat,{127,0,0,1})),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_IP_ADDR,cat,{127,0,0,1}}]},
                   set_ip_addr (new(foo),cat,"127.0.0.1")),
    ?_assertEqual (#lwes_event{name = "foo",
                               attrs = [{?LWES_IP_ADDR,cat,{127,0,0,1}}]},
                   set_ip_addr (new(foo),cat,<<"127.0.0.1">>)),
    ?_assertError (badarg,
                   set_ip_addr (new(foo),cat,"300.300.300.300"))
  ].

long_string_test () ->
  B = large_bin (),
  ?assertEqual (
     #lwes_event {name = <<"foo">>, attrs = [{<<"bar">>, B}]},
     from_binary (
       to_binary (
         #lwes_event {name = <<"foo">>, attrs = [{ "bar", B}]}
         ))).

large_bin () ->
  lists:foldl (fun (_, A) ->
                 <<1:8, A/binary>>
               end,
               << >>,
               lists:seq (1, 99999)).

allow_atom_and_binary_for_strings_test_ () ->
  [ ?_assertEqual (
      #lwes_event {name = <<"foo">>,attrs=[{<<"bar">>,<<"baz">>}]},
      from_binary(
        to_binary(
          #lwes_event {name="foo", attrs=[{"bar",baz}]}
        )
      )
    ),
    ?_assertEqual (
      #lwes_event {name = <<"foo">>,attrs=[{<<"bar">>,<<"baz">>}]},
      from_binary(
        to_binary(
          #lwes_event {name="foo", attrs=[{"bar",<<"baz">>}]}
        )
      )
    ),
    ?_assertEqual (
      #lwes_event {name = <<"foo">>,attrs=[{<<"bar">>,<<"baz">>}]},
      from_binary(
        to_binary(
          #lwes_event {name="foo", attrs=[{"bar","baz"}]}
        )
      )
    )
  ].

set_nullable_array_test_ () ->
  [
    ?_assertEqual (#lwes_event {name = "foo",
                                attrs = [ { ?LWES_N_INT_16_ARRAY, key1,
                                            [1, -1, undefined, 3, undefined, -4]}]},
                    set_nint16_array(new(foo),
                      key1, [1, -1, undefined, 3, undefined, -4])
                    )
  ].

write_read_nullarrays_test_() ->
  [ fun () ->
      W = write(Type, Arr),
      <<_:8/bits, Data/binary>> = W,
      ?assertEqual ({Arr, <<>>}, read_value(Type_Id, Data, 0))
    end
    || {Type, Type_Id, Arr}
    <- [
        {?LWES_N_U_INT_16_ARRAY, 141, [3, undefined, undefined, 500, 10]},
        {?LWES_N_INT_16_ARRAY, 142, [undefined, -1, undefined, -500, 10]},
        {?LWES_N_U_INT_32_ARRAY, 143, [3, undefined, undefined, 500, 10]},
        {?LWES_N_INT_32_ARRAY, 144, [undefined, -1, undefined, -500, 10]},
        {?LWES_N_U_INT_64_ARRAY, 148, [3, 1844674407370955161, undefined, 10]},
        {?LWES_N_INT_64_ARRAY, 147, [undefined, undefined, -72036854775808]},
        {?LWES_N_BOOLEAN_ARRAY, 149, [true, false, undefined, true, true, false]},
        {?LWES_N_BYTE_ARRAY, 150, [undefined, undefined, undefined, 23, 72, 9]},
        {?LWES_N_FLOAT_ARRAY, 151, [undefined, -2.25, undefined, 2.25]},
        {?LWES_N_DOUBLE_ARRAY, 152, [undefined, undefined, -1.25, 2.25]},
        {?LWES_N_STRING_ARRAY, 145, [undefined, <<"test">>, <<"should ">>, <<"pass">>]}
      ]
  ].

string_nullable_arrays_test_ () ->
  [
    ?_assertEqual(write(?LWES_N_STRING_ARRAY, [undefined, "test", "should ", "pass"]),
                  <<145,0,4,0,4,14,0,4,"test",0,7,"should ",0,4,"pass">>),

    ?_assertEqual({[undefined, <<"test">>, <<"should ">>, <<"pass">>], <<>>},
                  read_value(?LWES_TYPE_N_STRING_ARRAY,
                    <<0,4,0,4,14,0,4,"test",0,7,"should ",0,4,"pass">>, 0))
  ].

serialize_test_ () ->
  [
    ?_assertEqual (test_packet(binary),
                   to_binary(
                     remove_attr(<<"ReceiptTime">>,
                       remove_attr(<<"SenderIP">>,
                         remove_attr(<<"SenderPort">>,
                                     test_packet(tagged)))))
                   ),
    fun () ->
      E = #lwes_event { name = <<"test">>,
                        attrs = dict:from_list([{<<"a">>,<<"b">>},{<<"c">>,1}])
                      },
      ?assertEqual (E, from_binary (to_binary(E),dict))
    end
  ].

deserialize_test_ () ->
  { setup,
    fun () ->
      {ok, Port} = gen_udp:open(0,[binary]),
      {udp, Port, {192,168,54,1}, 58206, test_packet(binary)}
    end,
    fun ({udp, Port, _, _, _}) ->
      gen_udp:close(Port)
    end,
    fun (Packet) ->
      [
        % peek name check
        ?_assertEqual (<<"Test">>, peek_name_from_udp (Packet)),
        % peek name failure
        fun () ->
          {ok, Port} = gen_udp:open(0,[binary]),
          ?assertEqual ({error, malformed_event},
                        peek_name_from_udp ({udp, Port,
                                             {192,168,54,1}, 58206,
                                             <<4,84,101,115>>})),
          gen_udp:close(Port)
        end
        | [
            ?_assertEqual (
              remove_attr (<<"ReceiptTime">>, from_udp_packet(Packet, Format)),
              remove_attr (<<"ReceiptTime">>, test_packet (Format))
            ) || Format <- formats()
          ]
      ]
    end
  }.

nested_json_test_ () ->
  [
    ?_assertEqual (to_binary(nested_event(event)), nested_event(binary)),
    % check that all untyped types encode to the same json structure
    ?_assertEqual (lwes_mochijson2:encode(nested_event(json)),
                   lwes_mochijson2:encode(nested_event(json_untyped))),
    ?_assertEqual (lwes_mochijson2:encode(nested_event(json)),
                   lwes_mochijson2:encode(nested_event(json_proplist))),
    ?_assertEqual (lwes_mochijson2:encode(nested_event(json)),
                   lwes_mochijson2:encode(nested_event(json_proplist_untyped))),
    ?_assertEqual (lwes_mochijson2:encode(nested_event(json)),
                   lwes_mochijson2:encode(nested_event(json_eep18))),
    ?_assertEqual (lwes_mochijson2:encode(nested_event(json)),
                   lwes_mochijson2:encode(nested_event(json_eep18_untyped))),
    % check that all typed typed encode to the same json structure
    ?_assertEqual (lwes_mochijson2:encode(nested_event(json_typed)),
                   lwes_mochijson2:encode(nested_event(json_proplist_typed))),
    ?_assertEqual (lwes_mochijson2:encode(nested_event(json_typed)),
                   lwes_mochijson2:encode(nested_event(json_eep18_typed)))
    | [
        ?_assertEqual (to_json(nested_event(binary), Format),
                       nested_event (Format))
        || Format
        <- json_formats()
      ]
  ].

from_json_test () ->
  #lwes_event{name=_,attrs=AttributesFromJson} = from_json(test_text()),
  #lwes_event{name=_,attrs=AttributesFromTest} = test_packet(tagged),
  ?assertEqual(lists:sort(AttributesFromJson),
               lists:sort(
                 remove_attr(<<"ReceiptTime">>,AttributesFromTest))).

check_headers_test_ () ->
  PacketWithHeaders =
    <<19,77,111,110,68,101,109,97,110,100,58,58,83,116,97,116,115,
      77,115,103,0,14,7,99,116,120,116,95,118,48,5,0,15,111,112,101,
      110,120,45,100,101,118,46,108,111,99,97,108,7,99,116,120,116,
      95,107,48,5,0,4,104,111,115,116,8,99,116,120,116,95,110,117,
      109,1,0,1,2,116,49,5,0,5,103,97,117,103,101,2,118,49,7,0,0,0,
      0,0,0,2,208,2,107,49,5,0,9,114,116,113,95,98,121,116,101,115,
      2,116,48,5,0,5,103,97,117,103,101,2,118,48,7,0,0,0,0,6,64,0,0,
      2,107,48,5,0,13,114,116,113,95,109,97,120,95,98,121,116,101,
      115,3,110,117,109,1,0,2,7,112,114,111,103,95,105,100,5,0,4,
      114,105,97,107,11,82,101,99,101,105,112,116,84,105,109,101,7,
      0,0,1,79,66,126,41,176,8,83,101,110,100,101,114,73,80,6,128,
      101,16,172,10,83,101,110,100,101,114,80,111,114,116,1,135,217>>,
  PacketWithoutHeaders =
    <<19,77,111,110,68,101,109,97,110,100,58,58,83,116,97,116,115,
      77,115,103,0,14,7,99,116,120,116,95,118,48,5,0,15,111,112,101,
      110,120,45,100,101,118,46,108,111,99,97,108,7,99,116,120,116,
      95,107,48,5,0,4,104,111,115,116,8,99,116,120,116,95,110,117,
      109,1,0,1,2,116,49,5,0,5,103,97,117,103,101,2,118,49,7,0,0,0,
      0,0,0,2,208,2,107,49,5,0,9,114,116,113,95,98,121,116,101,115,
      2,116,48,5,0,5,103,97,117,103,101,2,118,48,7,0,0,0,0,6,64,0,0,
      2,107,48,5,0,13,114,116,113,95,109,97,120,95,98,121,116,101,
      115,3,110,117,109,1,0,2,7,112,114,111,103,95,105,100,5,0,4,
      114,105,97,107>>,
  [
    ?_assertEqual (true, has_header_fields (PacketWithHeaders)),
    ?_assertEqual (false, has_header_fields (PacketWithoutHeaders)),
    fun () ->
      E1 = #lwes_event{name = <<"test">>,
                       attrs = [
                          {uint16,<<"SenderPort">>,58206},
                          {ip_addr,<<"SenderIP">>,{192,168,54,1}},
                          {int64,<<"ReceiptTime">>,1439587738948},
                          {string,<<"foo">>,<<"bar">>}
                       ]},
      B1 = to_binary(E1),
      ?assertEqual (true, has_header_fields (B1)),
      {ok, Port} = gen_udp:open (0, [binary]), % so the packets below work
      E2 = from_udp_packet({udp,Port,{127,0,0,1},24442,B1},tagged),
      gen_udp:close(Port),
      ?assertEqual (E1, E2)
    end,
    fun () ->
      H = header_fields_to_iolist(12345253,{127,0,0,1},20202),
      ?assertEqual(true,
                   has_header_fields(erlang:iolist_to_binary(H))),
      EventNoHeaders =
        #lwes_event { name = <<"foo">>,
                      attrs = [{string, <<"bar">>,<<"baz">>}]},
      ExpectedEvent =
        #lwes_event { name = <<"foo">>,
                      attrs = [{uint16,<<"SenderPort">>,20202},
                               {ip_addr,<<"SenderIP">>,{127,0,0,1}},
                               {int64,<<"ReceiptTime">>,12345253},
                               {string,<<"bar">>,<<"baz">>}] },
      ?assertEqual (ExpectedEvent,
                    from_binary (
                      erlang:iolist_to_binary (
                        [ to_binary (EventNoHeaders), H ]
                      ),
                      tagged
                    ))
    end
  ].

-endif.
