package ecnu.db.analyzer.online.select.tidb;

import ecnu.db.utils.TouchstoneToolChainException;
import ecnu.db.utils.exception.IllegalCharacterException;
import ecnu.db.analyzer.online.select.Token;
import ecnu.db.analyzer.online.select.TokenType;
%%

%public
%class TidbSelectOperatorInfoLexer
/* throws TouchstoneToolChainException */
%yylexthrow{
ecnu.db.utils.TouchstoneToolChainException
%yylexthrow}

%{
  private int comment_count = 0;
  private StringBuilder str_buff = new StringBuilder();
%}

%line
%char
%state STRING
%unicode
%type Token

/* tokens */
DIGIT=[0-9]
WHITE_SPACE_CHAR=[\n\r\ \t\b\012]
SCHEMA_NAME_CHAR=[A-Za-z0-9$_]
ISNULL_OPERATOR="isnull"
ARITHMETIC_OPERATOR=(plus|minus|mul|div)
LOGICAL_OPERATOR=(and|or)
NOT_OPERATOR=not
UNI_COMPARE_OPERATOR=(le|ge|lt|gt|eq|ne)
MULTI_COMPARE_OPERATOR=(in|like)
CANONICAL_COL_NAME=({SCHEMA_NAME_CHAR})+\.({SCHEMA_NAME_CHAR})+\.({SCHEMA_NAME_CHAR})+
FLOAT=(0|([1-9]({DIGIT}*)))\.({DIGIT}*)
INTEGER=(0|[1-9]({DIGIT}*))
DATE=(({DIGIT}{4}-{DIGIT}{2}-{DIGIT}{2} {DIGIT}{2}:{DIGIT}{2}:{DIGIT}{2}\.{DIGIT}{6})|({DIGIT}{4}-{DIGIT}{2}-{DIGIT}{2} {DIGIT}{2}:{DIGIT}{2}:{DIGIT}{2})|({DIGIT}{4}-{DIGIT}{2}-{DIGIT}{2}))
%%

<YYINITIAL> {
  {ARITHMETIC_OPERATOR}\( {
    String str = yytext();
    return (new Token(TokenType.ARITHMETIC_OPERATOR, str.substring(0, str.length() - 1), yyline, yychar));
  }
  {LOGICAL_OPERATOR}\( {
    String str = yytext();
    return (new Token(TokenType.LOGICAL_OPERATOR, str.substring(0, str.length() -1), yyline, yychar));
  }
  {ISNULL_OPERATOR}\( {
    String str = yytext();
    return (new Token(TokenType.ISNULL_OPERATOR, str.substring(0, str.length() -1), yyline, yychar));
  }
  {NOT_OPERATOR}\( {
    String str = yytext();
    return (new Token(TokenType.NOT_OPERATOR, str.substring(0, str.length() -1), yyline, yychar));
  }
  {UNI_COMPARE_OPERATOR}\( {
    String str = yytext();
    return (new Token(TokenType.UNI_COMPARE_OPERATOR, str.substring(0, str.length() -1), yyline, yychar));
  }
  {MULTI_COMPARE_OPERATOR}\( {
    String str = yytext();
    return (new Token(TokenType.MULTI_COMPARE_OPERATOR, str.substring(0, str.length() -1), yyline, yychar));
  }
  {CANONICAL_COL_NAME} {
    return (new Token(TokenType.CANONICAL_COL_NAME, yytext(), yyline, yychar));
  }
  {DATE} {
    return (new Token(TokenType.CONSTANT, yytext(), "DATE", yyline, yychar));
  }
  {FLOAT} {
    return (new Token(TokenType.CONSTANT, yytext(), "FLOAT", yyline, yychar));
  }
  {INTEGER} {
    return (new Token(TokenType.CONSTANT, yytext(), "INTEGER", yyline, yychar));
  }
  \" {
    str_buff.setLength(0); yybegin(STRING);
  }
  ", " {}
  {WHITE_SPACE_CHAR}+ {}
  \) {
     return (new Token(TokenType.RIGHT_PARENTHESIS, yytext(), yyline, yychar));
  }
}
<STRING> {
  \" {
    yybegin(YYINITIAL);
    return (new Token(TokenType.CONSTANT, "\"" + str_buff.toString() + "\"", "STRING", yyline, yychar));
  }
  [^\n\r\"\\]+                   { str_buff.append( yytext() ); }
  \\t                            { str_buff.append('\t'); }
  \\n                            { str_buff.append('\n'); }
  \\r                            { str_buff.append('\r'); }
  \\\"                           { str_buff.append('\"'); }
  \\                             { str_buff.append('\\'); }
}

. {
   throw new IllegalCharacterException(yytext(), yyline, yychar);
}

