-ifndef(_lwes_internal_included).
-define(_lwes_internal_included, yup).

-define (is_ttl (V), is_integer(V), V >= 0, V =< 32).
-define (is_int16 (V), is_integer(V), V >= -32768, V =< 32767).
-define (is_uint16 (V), is_integer(V), V >= 0, V =< 65535).
-define (is_int32 (V), is_integer(V), V >= -2147483648, V =< 2147483647).
-define (is_uint32 (V), is_integer(V), V >= 0, V =< 4294967295).
-define (is_int64 (V), is_integer(V), V >= -9223372036854775808, V =< 9223372036854775807).
-define (is_uint64 (V), is_integer(V), V >= 0, V =< 18446744073709551615).
-define (is_byte (V), is_integer(V), V >= 0, V =< 255).
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

-record (lwes_channel, { type,
                         config,
                         ref
                       }).
-record (lwes_multi_emitter, {type, m, n}).

-define (LWES_TYPE_U_INT_16,            1).
-define (LWES_TYPE_INT_16,              2).
-define (LWES_TYPE_U_INT_32,            3).
-define (LWES_TYPE_INT_32,              4).
-define (LWES_TYPE_STRING,              5).
-define (LWES_TYPE_IP_ADDR,             6).
-define (LWES_TYPE_INT_64,              7).
-define (LWES_TYPE_U_INT_64,            8).
-define (LWES_TYPE_BOOLEAN,             9).
-define (LWES_TYPE_BYTE,               10).
-define (LWES_TYPE_FLOAT,              11).
-define (LWES_TYPE_DOUBLE,             12).
-define (LWES_TYPE_LONG_STRING,        13).
-define (LWES_TYPE_U_INT_16_ARRAY,    129).
-define (LWES_TYPE_INT_16_ARRAY,      130).
-define (LWES_TYPE_U_INT_32_ARRAY,    131).
-define (LWES_TYPE_INT_32_ARRAY,      132).
-define (LWES_TYPE_STRING_ARRAY,      133).
-define (LWES_TYPE_IP_ADDR_ARRAY,     134).
-define (LWES_TYPE_INT_64_ARRAY,      135).
-define (LWES_TYPE_U_INT_64_ARRAY,    136).
-define (LWES_TYPE_BOOLEAN_ARRAY,     137).
-define (LWES_TYPE_BYTE_ARRAY,        138).
-define (LWES_TYPE_FLOAT_ARRAY,       139).
-define (LWES_TYPE_DOUBLE_ARRAY,      140).
-define (LWES_TYPE_N_U_INT_16_ARRAY,  141).
-define (LWES_TYPE_N_INT_16_ARRAY,    142).
-define (LWES_TYPE_N_U_INT_32_ARRAY,  143).
-define (LWES_TYPE_N_INT_32_ARRAY,    144).
-define (LWES_TYPE_N_STRING_ARRAY,    145).
-define (LWES_TYPE_N_INT_64_ARRAY,    147).
-define (LWES_TYPE_N_U_INT_64_ARRAY,  148).
-define (LWES_TYPE_N_BOOLEAN_ARRAY,   149).
-define (LWES_TYPE_N_BYTE_ARRAY,      150).
-define (LWES_TYPE_N_FLOAT_ARRAY,     151).
-define (LWES_TYPE_N_DOUBLE_ARRAY,    152).

-endif.
