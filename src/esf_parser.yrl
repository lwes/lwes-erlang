Nonterminals
eventlist event attributelist attribute.

Terminals '{' '}' '[' ']' '=' ';' type identifier value qualifier.

Rootsymbol eventlist.

eventlist -> event : [ '$1' ].
eventlist -> event eventlist : [ '$1' | '$2' ].

event -> identifier '{' attributelist '}' : { event, unwrap ('$1'), '$3' }.

attributelist -> attribute : [ '$1' ].
attributelist -> attribute attributelist : [ '$1' | '$2' ].

attribute -> type identifier ';' :
              { attribute, { unwrap_to_atom ('$1'), unwrap_to_binary ('$2'), undefined, undefined } }.
attribute -> type identifier '[' value ']' ';' :
              { attribute, { list_to_atom(unwrap ('$1') ++ "_array"), unwrap_to_binary ('$2'), undefined, undefined } }.
attribute -> type identifier '=' value ';' :
              { attribute, { unwrap_to_atom ('$1'), unwrap_to_binary ('$2'), unwrap ('$4'), undefined } }.
attribute -> qualifier type identifier ';' :
              { attribute, { unwrap_to_atom ('$2'), unwrap_to_binary ('$3'), undefined,  unwrap_to_atom ('$1') } }.
attribute -> qualifier type identifier '[' value ']' ';' :
              { attribute, { list_to_atom (array_prefix (unwrap_to_atom ('$1')) ++ unwrap ('$2') ++ "_array"), unwrap_to_binary ('$3'), undefined, unwrap_to_atom('$1') } }.
attribute -> qualifier type identifier '=' value ';' :
              { attribute, { unwrap_to_atom ('$2'), unwrap_to_binary ('$3'), unwrap ('$5') , unwrap_to_atom ('$1') } }.

Erlang code.

unwrap ({_, _, V}) -> V.

unwrap_to_atom ({_, _, V}) -> list_to_atom(V).

unwrap_to_binary({_, _, V}) -> list_to_binary(V).

array_prefix (nullable) -> "nullable_";
array_prefix (_) -> [].
