-ifndef(_lwes_included).
-define(_lwes_included, yup).

-record (lwes_event, {name, attrs}).

-define (LWES_U_INT_16,         uint16).
-define (LWES_INT_16,           int16).
-define (LWES_U_INT_32,         uint32).
-define (LWES_INT_32,           int32).
-define (LWES_INT_64,           int64).
-define (LWES_U_INT_64,         uint64).
-define (LWES_STRING,           string).
-define (LWES_IP_ADDR,          ip_addr).
-define (LWES_BOOLEAN,          boolean).
-define (LWES_BYTE,             byte).
-define (LWES_FLOAT,            float).
-define (LWES_DOUBLE,           double).
-define (LWES_LONG_STRING,      long_string).
-define (LWES_U_INT_16_ARRAY,   uint16_array).
-define (LWES_INT_16_ARRAY,     int16_array).
-define (LWES_U_INT_32_ARRAY,   uint32_array).
-define (LWES_INT_32_ARRAY,     int32_array).
-define (LWES_INT_64_ARRAY,     int64_array).
-define (LWES_U_INT_64_ARRAY,   uint64_array).
-define (LWES_STRING_ARRAY,     string_array).
-define (LWES_IP_ADDR_ARRAY,    ip_addr_array).
-define (LWES_BOOLEAN_ARRAY,    boolean_array).
-define (LWES_BYTE_ARRAY,       byte_array).
-define (LWES_FLOAT_ARRAY,      float_array).
-define (LWES_DOUBLE_ARRAY,     double_array).
-define (LWES_N_U_INT_16_ARRAY, nullable_uint16_array).
-define (LWES_N_INT_16_ARRAY,   nullable_int16_array).
-define (LWES_N_U_INT_32_ARRAY, nullable_uint32_array).
-define (LWES_N_INT_32_ARRAY,   nullable_int32_array).
-define (LWES_N_INT_64_ARRAY,   nullable_int64_array).
-define (LWES_N_U_INT_64_ARRAY, nullable_uint64_array).
-define (LWES_N_STRING_ARRAY,   nullable_string_array).
% TODO: this is not implemented
% -define (LWES_N_IP_ADDR_ARRAY,  nullable_ip_addr_array).
-define (LWES_N_BOOLEAN_ARRAY,  nullable_boolean_array).
-define (LWES_N_BYTE_ARRAY,     nullable_byte_array).
-define (LWES_N_FLOAT_ARRAY,    nullable_float_array).
-define (LWES_N_DOUBLE_ARRAY,   nullable_double_array).

-endif.
