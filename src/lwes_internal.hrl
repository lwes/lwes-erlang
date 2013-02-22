-ifndef(_lwes_internal_included).
-define(_lwes_internal_included, yup).

-define (is_int16 (V), V >= -32768, V =< 32767).
-define (is_uint16 (V), V >= 0, V =< 65535).
-define (is_int32 (V), V >= -2147483648, V =< 2147483647).
-define (is_uint32 (V), V >= 0, V =< 4294967295).
-define (is_int64 (V), V >= -9223372036854775808, V =< 9223372036854775807).
-define (is_uint64 (V), V >= 0, V =< 18446744073709551615).
-define (is_byte (V), is_integer(V), V >= 1, V =< 255).
-define (is_string (V), is_list (V); is_binary (V); is_atom (V)).
-define (is_ip_addr (V),
         (is_tuple (V) andalso
          tuple_size (V) =:= 4 andalso
          is_integer (element (1,V)) andalso
          element (1,V) >= 0 andalso
          element (1,V) =< 255 andalso
          is_integer (element (2,V)) andalso
          element (2,V) >= 0 andalso
          element (2,V) =< 255 andalso
          is_integer (element (3,V)) andalso
          element (3,V) >= 0 andalso
          element (3,V) =< 255 andalso
          is_integer (element (4,V)) andalso
          element (4,V) >= 0 andalso
          element (4,V) =< 255)).


-record (lwes_channel, {ip, port, is_multicast, type, ref}).
-record (lwes_multi_emitter, {type, m, n}).

-endif.
