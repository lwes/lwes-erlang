% Tokenizer definitions for ESF (refer : http://www.lwes.org/docs/)

Definitions.

% white space
WS = [\000-\s]

% literals
INTEGER_LITERAL = [-+]?[1-9]([0-9])*
BIG_INTEGER_LITERAL = [-+]?[1-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]([0-9])*
DOUBLE_LITERAL = [-+]?([0-9])*\.([0-9])+
STRING_LITERAL = \"(\\.|[^"])*\"
IPADDR_LITERAL = ([0-9])+\.([0-9])+\.([0-9])+\.([0-9])+
MACROS = \$\{[-A-Za-z0-9_.]+\}

WORD = [a-zA-Z_]([-_:.a-zA-Z0-9])*
COMMENT = #[^\r\n]*(\r\n|\r|\n)?

Rules.

{WORD} : { token, { token_type(list_to_atom(TokenChars)) , TokenLine, TokenChars } }.
{INTEGER_LITERAL}|{BIG_INTEGER_LITERAL}|{DOUBLE_LITERAL}|{STRING_LITERAL}|{IPADDR_LITERAL}|{MACROS} :
       { token, { value , TokenLine, TokenChars } }.
[\{\};=\[\]] : { token, { list_to_atom(TokenChars), TokenLine } }.

% ignore comment
{COMMENT} : skip_token.

% ignore whitespace
{WS}+ : skip_token.

Erlang code.

-export([token_type/1]).

token_type (true) -> value;
token_type (false) -> value;

token_type (required) -> qualifier;
token_type (nullable) -> qualifier;
token_type (optional) -> qualifier;

token_type (byte) -> type;
token_type (uint16) -> type;
token_type (int16) -> type;
token_type (uint32) -> type;
token_type (int32) -> type;
token_type (uint64) -> type;
token_type (int64) -> type;
token_type (float) -> type;
token_type (double) -> type;
token_type (boolean) -> type;
token_type (string) -> type;
token_type (ip_addr) -> type;

token_type (_) -> identifier.
