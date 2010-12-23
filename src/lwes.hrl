-ifndef(_lwes_included).
-define(_lwes_included, yup).

-record (lwes_event, {name, attrs = []}).

-define (LWES_TYPE_U_INT_16, 1).
-define (LWES_TYPE_INT_16,   2).
-define (LWES_TYPE_U_INT_32, 3).
-define (LWES_TYPE_INT_32,   4).
-define (LWES_TYPE_STRING,   5).
-define (LWES_TYPE_IP_ADDR,  6).
-define (LWES_TYPE_INT_64,   7).
-define (LWES_TYPE_U_INT_64, 8).
-define (LWES_TYPE_BOOLEAN,  9).

-define (LWES_U_INT_16, uint16).
-define (LWES_INT_16,   int16).
-define (LWES_U_INT_32, uint32).
-define (LWES_INT_32,   int32).
-define (LWES_STRING,   string).
-define (LWES_IP_ADDR,  ip_addr).
-define (LWES_INT_64,   int64).
-define (LWES_U_INT_64, uint64).
-define (LWES_BOOLEAN,  boolean).

-endif.
