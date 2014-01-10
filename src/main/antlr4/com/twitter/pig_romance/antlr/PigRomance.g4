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
NOT : '!' | N O T;
PREVIOUS_RELATION : '@';
EXEC : E X E C;
fragment NEWLINE : '\r'? '\n';
LINE_COMMENT : '--' ~[\r\n]* NEWLINE -> channel(HIDDEN);
BLOCK_COMMENT : '/*' .*? '*/' -> channel(HIDDEN);

FOREACH : F O R E A C H;
GENERATE : G E N E R A T E;
LOAD : L O A D;
AS : A S;
USING : U S I N G;
REGISTER : R E G I S T E R;
GLOBAL : G L O B A L;
MATCHES : M A T C H E S;
DISTINCT : D I S T I N C T;
GROUP : G R O U P;
COGROUP : C O G R O U P;
JOIN : J O I N;
DUMP : D U M P;
ON : O N;
BY : B Y;
REPLICATED : QUOTE R E P L I C A T E D QUOTE;
SKEWED : QUOTE S K E W E D QUOTE;
MERGE : QUOTE M E R G E QUOTE;
STORE : S T O R E;
INTO : I N T O;
DESCRIBE : D E S C R I B E;
//TODO should this be a keyword?
FLATTEN : F L A T T E N;

TRUE : T R U E;
FALSE : F A L S E;

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

fragment LETTER : [a-zA-Z];
fragment DIGIT  : [0-9];

POSITIVE_INTEGER : DIGIT+;
IDENTIFIER : LETTER ( LETTER | DIGIT | UNDERSCORE )*;

WS : [ \t\r\n]+ -> skip;

// PARSER

//TODO it'd be nice if this stuff was in the lexer instead, but it's hard to get the precedence correct
integer : NEG? POSITIVE_INTEGER
        ;

number_literal : integer
               ;

//TODO consider getting rid of the parser rule and propagating the IDENTIFIER
identifier : IDENTIFIER
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

path : FILE_SCHEMA absolute_path  # FilePath
     | hdfs_schema absolute_path  # HdfsPath
     | absolute_path              # AbsPath
     | relative_path              # RelPath
     ;

quoted_path : QUOTE path QUOTE
            ;

start : global_command* EOF
      ;

//TODO perhaps command should be relation, since that's sort of what they are generating?
relation : identifier EQUALS
         ;

//TODO should we support dump and whatnot within nested_blocks? Probably
anywhere_command : relation? command_inner SEMICOLON    # AnywhereCommandInner
                 //| relation? command_outer SEMICOLON
                 | nested_block                         # AnywhereNestedBlock
                 ;

global_command : anywhere_command
               | shell_command SEMICOLON
               | execution_command SEMICOLON
               ;

nested_block_command : anywhere_command
                     | make_global SEMICOLON
                     ;

//TODO we shouldn't need a semicolon after a nested_block
nested_block : LEFT_BRACK nested_block_command* RIGHT_BRACK
             ;

make_global : GLOBAL identifier
            ;

//TODO IMPORTANT: need to decide how we want to deal with defines. Preprocessor? Or actual macro type thing?
shell_command : register  # ShellRegister
              | describe  # ShellDescribe
              ;

describe : DESCRIBE nested_command
         ;

// These are shell commands that will force an execution immediately. Note that it is a critical objective
// that when in script mode, we will scan the whole script and make sure that we keep around information that will
// be useful to later executions so that we do not have to recalculate everything.
execution_command : EXEC   # Exec
                  | dump   # DumpExec
                  | store  # StoreExec
                  ;

register : REGISTER quoted_path
         ;

nested_command : LEFT_PAREN command_inner RIGHT_PAREN  # NestedCommandInner
               | identifier                            # NestedCommandIdentifier
               | PREVIOUS_RELATION                     # NestedCommandPrevious
               ;

command_inner : foreach     # CommandInnerForeach
              | identifier  # CommandInnerIdentifier
              | load        # CommandInnerLoad
              | distinct    # CommandInnerDistinct
              | group       # CommandInnerGroup
              | join        # CommandInnerJoin
              | cogroup     # CommandInnerCogroup
              ;

distinct : DISTINCT nested_command
         ;

group : GROUP commands_by_key
      ;

join : JOIN commands_on_key
     ;

cogroup : COGROUP commands_on_key
        ;

dump : DUMP nested_command
     ;

store : STORE nested_command INTO quoted_path using?
      ;

commands_by_key : relations BY column_transformations
                ;

commands_on_key : relations ON column_transformations join_qualifier?
                ;

join_qualifier : USING join_optimizations
               ;

join_optimizations : REPLICATED
                   | MERGE
                   | SKEWED
                   ;

relations : nested_command ( COMMA nested_command )*
          ;

foreach : FOREACH nested_command GENERATE column_transformations
        ;

column_transformations : column_expression_realias ( COMMA column_expression_realias )*
                       ;

column_expression_realias : column_expression realias?
                          ;

column_expression : column_transform       # ColumnExpressionTransform
                  | arithmetic_expression  # ColumnExpressionArithmeticExpr
                  | boolean_expression     # ColumnExpressionBooleanExpr
                  | string_literal         # ColumnExpressionStringLit
                  | tuple                  # ColumnExpressionTuple
                  | MULT                   # ColumnExpressionStar
                  | flatten                # ColumnExpressionFlatten
                  // TODO need a range selector
                  ;

tuple : LEFT_PAREN column_transformations RIGHT_PAREN
      ;

//TODO should support bags as well...do we want this to be a keyword?
flatten : FLATTEN tuple
        ;

column_transform : column_identifier  # ColumnTransformColIdentifier
                 | udf                # ColumnTransformUdf
                 ;

column_identifier : identifier           # ColumnIdentifierName
                  | relative_identifier  # ColumnIdentifierPos
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
                      | column_transform
                      | number_literal
                      ;

boolean_expression : NOT boolean_expression
                   | LEFT_PAREN boolean_expression RIGHT_PAREN
                   | boolean_expression AND boolean_expression
                   | boolean_expression OR boolean_expression
                   | column_transform
                   | boolean_literal
                   | matches_expression
                   ;

boolean_literal : TRUE
                | FALSE;

matches_expression : column_identifier MATCHES regex
                   ;

//TODO need to refine this big time
regex : QUOTE identifier QUOTE
      ;

udf : identifier ( DOT identifier )* LEFT_PAREN column_transformations RIGHT_PAREN
    ;

load_class : identifier ( DOT identifier )* LEFT_PAREN load_arguments RIGHT_PAREN
           ;

load_arguments : string_literal ( COMMA string_literal )*
               ;

string_literal : QUOTE identifier QUOTE
               ;

// In pig romance, ALL STATEMENTS MUST BE TYPED. UDFs and load funcs must be typed. So while realiasing is ok,
// there should never be a need to explicitly retype something (use a typesafe cast for that!)
realias : AS LEFT_PAREN realias_fields RIGHT_PAREN
        | AS identifier
        ;

realias_fields : identifier ( COMMA identifier )*;

/*
Without casting, these shouldn't be necessary

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
*/
/*
command_outer : load
              ;
*/
load : LOAD quoted_path using? realias?
     ;

using : USING load_class
      ;
