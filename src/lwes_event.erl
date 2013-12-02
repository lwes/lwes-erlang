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
         from_udp_packet/2,
         from_binary/2,
         peek_name_from_udp/1,
         json/1,
         untyped_json/1,
         export_attributes/2,
         from_json/1,
         from_json/2]).

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
        [ { <<"SenderIP">>,   lwes_util:ip2bin (SenderIP) },
          { <<"SenderPort">>, SenderPort },
          { <<"ReceiptTime">>, millisecond_since_epoch () } ];
      json_proplist ->
        [ { <<"SenderIP">>,   lwes_util:ip2bin (SenderIP) },
          { <<"SenderPort">>, SenderPort },
          { <<"ReceiptTime">>, millisecond_since_epoch () } ];
      json_eep18 ->
        [ { <<"SenderIP">>,   lwes_util:ip2bin (SenderIP) },
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
      { [{<<"EventName">>, EventName} | AttrList ] };
    _ ->
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
  case iolist_size (V) of
    SL when SL >= 0, SL =< 65535 ->
      [ <<?LWES_TYPE_STRING:8/integer-unsigned-big,
          SL:16/integer-unsigned-big>>, V ];
    _ ->
      throw (string_too_big)
  end;
write (?LWES_BYTE, V) ->
  <<?LWES_TYPE_BYTE:8/integer-unsigned-big, V:8/integer-unsigned-big>>;
write (?LWES_FLOAT, V) ->
  <<?LWES_TYPE_FLOAT:8/integer-unsigned-big, V:32/float>>;
write (?LWES_DOUBLE, V) ->
  <<?LWES_TYPE_DOUBLE:8/integer-unsigned-big, V:64/float>>;
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

json (Event = #lwes_event {name=Name, attrs=_}) ->
  Accum = {[{ <<"name">>, Name } | export_attributes([typed], Event)]},
  lwes_mochijson2:encode (Accum).

untyped_json (Event = #lwes_event {name=Name, attrs=_}) ->
  Accum = {[{ <<"name">>, Name } | export_attributes([untyped], Event)]},
  lwes_mochijson2:encode (Accum).

export_attributes([],_) ->
  [];
export_attributes ([Type | Rest], Event) ->
  [export_attributes(Type, Event)] ++ export_attributes(Rest, Event);
export_attributes(typed,#lwes_event{name=_, attrs = Attrs}) ->
  { <<"typed">>,
    { lists:foldl (
      fun ({T,N,V}, A) when T =:= ?LWES_IP_ADDR ->
            [ { N, [ {<<"type">>, T},
                     {<<"value">>, lwes_util:ip2bin (V) }
                   ]
              }
              | A
            ];
          ({T,N,V}, A) ->
            [ { N, [ {<<"type">>, T},
                     {<<"value">>, make_binary (T, V) }
                   ]
              }
              | A
            ]
      end,
      [],
      Attrs)
    }
  };
export_attributes(untyped, Event = #lwes_event{name=_, attrs=_}) ->
  { <<"attributes">>,
    from_binary (to_binary (Event), json_eep18)
  }.

from_json (Bin, Format) when is_list(Bin); is_binary(Bin)->
  from_json (mochijson2:decode (Bin, [{format, Format}])).

from_json(Bin) when is_list(Bin); is_binary(Bin) ->
  from_json(Bin, eep18);
from_json ({Json}) ->
  Name = proplists:get_value (<<"name">>, Json),
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


process_one ({Key, {Attrs}}) ->
  Type =
    lwes_util:binary_to_any (proplists:get_value (<<"type">>, Attrs), atom),
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
      ?LWES_STRING -> lwes_util:binary_to_any (Value, list);
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
      ?LWES_STRING_ARRAY -> lwes_util:binary_to_arr (Value, list);
      ?LWES_N_STRING_ARRAY -> lwes_util:binary_to_arr (Value, list);
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
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

new_test () ->
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

set_nullable_array_test() ->
  [
    ?_assertEqual (#lwes_event {name = "foo", 
                              attrs = [{nint16_array, key1, [1, -1, undefined, 3, undefined, -4]}]},
                    lwes_event:set_nint16_array(lwes_event:new(foo), 
                      key1, [1, -1, undefined, 3, undefined, -4])
                    )
  ].

write_read_nullarrays_test() ->
  [ begin
      W = write(Type, Arr),
      <<_:8/bits, Data/binary>> = W,
      ?assertEqual({Arr, <<>>}, read_value(Type_Id, Data, 0))
    end
      || {Type, Type_Id, Arr} <- 
           [{nuint16_array, 141, [3, undefined, undefined, 500, 10]},
            {nint16_array, 142, [undefined, -1, undefined, -500, 10]},
            {nuint32_array, 143, [3, undefined, undefined, 500, 10]}, 
            {nint32_array, 144, [undefined, -1, undefined, -500, 10]},
            {nuint64_array, 148, [3, 1844674407370955161, undefined, 10]},
            {nint64_array, 147, [undefined, undefined, -72036854775808]}, 
            {nboolean_array, 149, [true, false, undefined, true, true, false]},
            {nbyte_array, 150, [undefined, undefined, undefined, 23, 72, 9]},
            {nfloat_array, 151, [undefined, -2.25, undefined, 2.25]}, 
            {ndouble_array, 152, [undefined, undefined, -1.25, 2.25]},
            {nstring_array, 145, [undefined, <<"test">>, <<"should ">>, <<"pass">>]}]].

string_nullable_arrays_test() ->
  [
    ?assertEqual(write(nstring_array, [undefined, "test", "should ", "pass"]), 
                  <<145,0,4,0,4,14,0,4,"test",0,7,"should ",0,4,"pass">>),

    ?assertEqual({[undefined, <<"test">>, <<"should ">>, <<"pass">>], <<>>},
                  read_value(?LWES_TYPE_N_STRING_ARRAY,
                    <<0,4,0,4,14,0,4,"test",0,7,"should ",0,4,"pass">>, 0))
  ].

deserialize_test () ->
  %% THIS IS A SERIALIZED PACKET
  %% SENT FROM THE JAVA LIBRARY
  %% THAT CONTAINS ALL TYPES
  JavaPacket = {udp,port,
                            {192,168,54,1},
                            58206,
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
                              54,2,0,20>>},
  lwes_event:from_udp_packet(JavaPacket, json).

-endif.
