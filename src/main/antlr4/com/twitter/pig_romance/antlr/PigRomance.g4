grammar PigRomance;

options {
  language=Java;
}

// LEXER

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];

fragment SPACE : ' ';
UNDERSCORE : '_';
COLON : ':';
SEMICOLON : ';';
LEFT_PAREN : '(';
RIGHT_PAREN : ')';
LEFT_BRACK : '{';
RIGHT_BRACK : '}';
LEFT_SQUARE : '[';
RIGHT_SQUARE : ']';
COMMA : ',';
EQUALS : '=';
SLASH : '/';
DOT : '.';
DOLLAR : '$';
MULT : '*';
DIV : '/';
ADD : '+';
NEG : '-';
POW : '^';
AND : '&&' | A N D;
OR : '||' | O R;

LETTER : [a-zA-Z];
DIGIT  : [0-9];

FOREACH : F O R E A C H;
GENERATE : G E N E R A T E;
LOAD : L O A D;
AS : A S;
USING : U S I N G;
REGISTER : R E G I S T E R;
GLOBAL : G L O B A L;

TYPE_INT : I N T;
TYPE_LONG : L O N G;
TYPE_FLOAT : F L O A T;
TYPE_DOUBLE : D O U B L E;
TYPE_STRING : C H A R A R R A Y;
TYPE_BYTEARRAY : B Y T E A R R A Y;
TYPE_TUPLE : T U P L E;
TYPE_BAG : B A G;
TYPE_MAP : M A P;

FILE_SCHEMA : F I L E COLON SLASH SLASH;
HDFS_PREFIX : H D F S COLON SLASH SLASH;

QUOTE : '\'';

WS : [ \t\r\n]+ -> skip;

// PARSER

//TODO it'd be nice if this stuff was in the lexer instead, but it's hard to get the precedence correct
letter_digit : LETTER | DIGIT | UNDERSCORE
             ;

integer : NEG? DIGIT+
        ;

number : integer
       ;

identifier : LETTER letter_digit*
           ;

hdfs_schema : HDFS_PREFIX namenode?
            ;

namenode : url
         ;

url : identifier ( DOT identifier )*
    ;

relative_path : path_piece ( SLASH path_piece )* SLASH?
              ;

path_piece : identifier
           ;

absolute_path : SLASH relative_path
              ;

path : FILE_SCHEMA absolute_path
     | hdfs_schema absolute_path
     | absolute_path
     | relative_path
     ;

quoted_path : QUOTE path QUOTE
            ;

start : (global_command? SEMICOLON)* EOF
      ;

//TODO perhaps command should be relation, since that's sort of what they are generating?
relation : identifier EQUALS
         ;

anywhere_command : relation? command_inner
                 //| relation? command_outer
                 | nested_block
                 ;

global_command : anywhere_command
               | shell_command
               ;

nested_block_command : anywhere_command
                     | make_global
                     ;

nested_block : LEFT_BRACK (nested_block_command? SEMICOLON)* RIGHT_BRACK
             ;

make_global : GLOBAL identifier
            ;

//TODO IMPORTANT: need to decide how we want to deal with defines. Preprocessor? Or actual macro type thing?
shell_command : register
              ;

register : REGISTER quoted_path
         ;

nested_command : LEFT_PAREN command_inner RIGHT_PAREN
               | identifier
               ;

command_inner : foreach
              | identifier
              | load
              ;

foreach : FOREACH nested_command GENERATE column_transformations
        ;

column_transformations : column_transform ( COMMA column_transform )*
                       ;

column_transform : column_identifier schema?
                 | udf schema?
                 | arithmetic_expression
                 ;

column_identifier : identifier
                  | relative_identifier
                  ;

relative_identifier : DOLLAR integer
                    ;

arithmetic_expression : NEG arithmetic_expression
                      | LEFT_PAREN arithmetic_expression RIGHT_PAREN
                      | arithmetic_expression POW<assoc=right> arithmetic_expression
                      | arithmetic_expression MULT arithmetic_expression
                      | arithmetic_expression DIV arithmetic_expression
                      | arithmetic_expression ADD arithmetic_expression
                      | arithmetic_expression NEG arithmetic_expression
                      | number
                      | column_identifier
                      ;



udf : clazz
    ;

clazz : identifier ( DOT identifier )* LEFT_PAREN arguments RIGHT_PAREN
      ;


arguments : string_literal ( COMMA string_literal )*
          | identifier ( COMMA identifier )*
          ;

string_literal : QUOTE identifier QUOTE
               ;

schema : AS LEFT_PAREN schema_fields RIGHT_PAREN
       | AS schema_field
       ;

schema_fields : schema_field ( COMMA schema_field )*;

schema_field : identifier ( COLON type )?
             ;

//TODO do we want to make types required for loads and such? either from the loader or the user?
type : TYPE_INT
     | TYPE_LONG
     | TYPE_FLOAT
     | TYPE_DOUBLE
     | TYPE_STRING
     | TYPE_BYTEARRAY
     | type_tuple
     | type_bag
     | type_map
     ;

type_tuple : TYPE_TUPLE ( LEFT_BRACK LEFT_PAREN schema_fields RIGHT_BRACK RIGHT_PAREN)?
           ;

type_bag : TYPE_BAG ( LEFT_PAREN schema_fields RIGHT_PAREN )?
         ;

//TODO think about the syntax we want
type_map : TYPE_MAP ( LEFT_SQUARE schema_fields RIGHT_SQUARE )?
         ;
/*
command_outer : load
              ;
*/
load : LOAD quoted_path using? schema?
     ;

using : USING clazz
      ;
