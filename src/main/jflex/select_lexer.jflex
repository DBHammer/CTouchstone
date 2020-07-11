package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;
%%

%public
%class SelectOperatorInfoLexer
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
    return (new Yytoken(TokenType.ARITHMETIC_OPERATOR, str.substring(0, str.length() - 1)));
  }
  {LOGICAL_OPERATOR}\( {
    String str = yytext();
    return (new Yytoken(TokenType.LOGICAL_OPERATOR, str.substring(0, str.length() -1)));
  }
  {ISNULL_OPERATOR}\( {
    String str = yytext();
    return (new Yytoken(TokenType.ISNULL_OPERATOR, str.substring(0, str.length() -1)));
  }
  {NOT_OPERATOR}\( {
    String str = yytext();
    return (new Yytoken(TokenType.NOT_OPERATOR, str.substring(0, str.length() -1)));
  }
  {UNI_COMPARE_OPERATOR}\( {
    String str = yytext();
    return (new Yytoken(TokenType.UNI_COMPARE_OPERATOR, str.substring(0, str.length() -1)));
  }
  {MULTI_COMPARE_OPERATOR}\( {
    String str = yytext();
    return (new Yytoken(TokenType.MULTI_COMPARE_OPERATOR, str.substring(0, str.length() -1)));
  }
  {CANONICAL_COL_NAME} {
    return (new Yytoken(TokenType.CANONICAL_COL_NAME, yytext()));
  }
  {DATE} {
    return (new Yytoken(TokenType.CONSTANT, yytext(), "DATE"));
  }
  {FLOAT} {
    return (new Yytoken(TokenType.CONSTANT, yytext(), "FLOAT"));
  }
  {INTEGER} {
    return (new Yytoken(TokenType.CONSTANT, yytext(), "INTEGER"));
  }
  \" {
    str_buff.setLength(0); yybegin(STRING);
  }
  ", " {}
  {WHITE_SPACE_CHAR}+ {}
  \) {
     return (new Yytoken(TokenType.RIGHT_PARENTHESIS, yytext()));
  }
}
<STRING> {
  \" {
    yybegin(YYINITIAL);
    return (new Yytoken(TokenType.CONSTANT, str_buff.toString(), "STRING"));
  }
  [^\n\r\"\\]+                   { str_buff.append( yytext() ); }
  \\t                            { str_buff.append('\t'); }
  \\n                            { str_buff.append('\n'); }
  \\r                            { str_buff.append('\r'); }
  \\\"                           { str_buff.append('\"'); }
  \\                             { str_buff.append('\\'); }
}

. {
   throw new TouchstoneToolChainException(String.format("非法字符 %s", yytext()));
}

