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
ARITHMETIC_OPERATOR=(add|mul|mul|div)
LOGIC_OPERATOR=(and|or|not)
COMPARE_OPERATOR=(le|ge|lt|gt|eq|ne|like|in)
CANONICAL_COL_NAME=({SCHEMA_NAME_CHAR})+\.({SCHEMA_NAME_CHAR})+\.({SCHEMA_NAME_CHAR})+
FLOAT=(0|([1-9]({DIGIT}*)))\.({DIGIT}*)
INTEGER=(0|[1-9]({DIGIT}*))
DATE=(({DIGIT}{4}-{DIGIT}{2}-{DIGIT}{2} {DIGIT}{2}:{DIGIT}{2}:{DIGIT}{2}\.{DIGIT}{6})|({DIGIT}{4}-{DIGIT}{2}-{DIGIT}{2} {DIGIT}{2}:{DIGIT}{2}:{DIGIT}{2})|({DIGIT}{4}-{DIGIT}{2}-{DIGIT}{2}))
%%

<YYINITIAL> {
  {ARITHMETIC_OPERATOR} {
    return (new Yytoken(TokenType.ARITHMETIC_OPERATOR, yytext().split("\\(")[0]));
  }
  {LOGIC_OPERATOR} {
    return (new Yytoken(TokenType.LOGIC_OPERATOR, yytext().split("\\(")[0]));
  }
  {ISNULL_OPERATOR} {
    return (new Yytoken(TokenType.ISNULL_OPERATOR, yytext().split("\\(")[0]));
  }
  {COMPARE_OPERATOR} {
      return (new Yytoken(TokenType.COMPARE_OPERATOR, yytext().split("\\(")[0]));
  }
  \( {
    return (new Yytoken(TokenType.LEFT_PARANTHESIS, yytext()));
  }
  {CANONICAL_COL_NAME} {
    return (new Yytoken(TokenType.CANONICAL_COL_NAME, yytext()));
  }
  {DATE} {
    return (new Yytoken(TokenType.DATE, yytext()));
  }
  {FLOAT} {
    return (new Yytoken(TokenType.FLOAT, yytext()));
  }
  {INTEGER} {
    return (new Yytoken(TokenType.INTEGER, yytext()));
  }
  \" {
    str_buff.setLength(0); yybegin(STRING);
  }
  ", " {}
  {WHITE_SPACE_CHAR}+ {}
  \) {
     return (new Yytoken(TokenType.RIGHT_PARANTHESIS, yytext()));
  }
}
<STRING> {
  \" {
    yybegin(YYINITIAL);
    return (new Yytoken(TokenType.STRING, str_buff.toString()));
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

