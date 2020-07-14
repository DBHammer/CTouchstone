package ecnu.db.analyzer.online.select.tidb;

import ecnu.db.utils.TouchstoneToolChainException;
import ecnu.db.utils.exception.IllegalCharacterException;
import ecnu.db.analyzer.online.select.Token;
import java_cup.runtime.*;
%%

%public
%class TidbSelectOperatorInfoLexer
/* throws TouchstoneToolChainException */
%yylexthrow{
ecnu.db.utils.TouchstoneToolChainException
%yylexthrow}

%{
  private StringBuilder str_buff = new StringBuilder();
  private Symbol symbol(int type) {
    return new Token(type, yyline+1, yycolumn+1);
  }

  private Symbol symbol(int type, Object value) {
    return new Token(type, yyline+1, yycolumn+1, value);
  }

  public void init() {
    System.out.println("initialized");
  }
%}

%implements TidbSelectSymbol
%line
%column
%state STRING_LITERAL
%unicode
%cup
%cupsym TidbSelectSymbol

/* tokens */
DIGIT=[0-9]
WHITE_SPACE_CHAR=[\n\r\ \t\b\012]
SCHEMA_NAME_CHAR=[A-Za-z0-9$_]
CANONICAL_COL_NAME=({SCHEMA_NAME_CHAR})+\.({SCHEMA_NAME_CHAR})+\.({SCHEMA_NAME_CHAR})+
FLOAT=(0|([1-9]({DIGIT}*)))\.({DIGIT}*)
INTEGER=(0|[1-9]({DIGIT}*))
DATE=(({DIGIT}{4}-{DIGIT}{2}-{DIGIT}{2} {DIGIT}{2}:{DIGIT}{2}:{DIGIT}{2}\.{DIGIT}{6})|({DIGIT}{4}-{DIGIT}{2}-{DIGIT}{2} {DIGIT}{2}:{DIGIT}{2}:{DIGIT}{2})|({DIGIT}{4}-{DIGIT}{2}-{DIGIT}{2}))
%%

<YYINITIAL> {
  /* logical operators */
  "and" {
    return symbol(AND, symbol(AND));
  }
  "or" {
    return symbol(OR, symbol(OR));
  }

  /* compare operators */
  "in" {
    return symbol(IN, symbol(IN));
  }
  "like" {
    return symbol(LIKE, symbol(LIKE));
  }
  "lt" {
    return symbol(LT, symbol(LT));
  }
  "gt" {
    return symbol(GT, symbol(GT));
  }
  "le" {
    return symbol(LE, symbol(LE));
  }
  "ge" {
    return symbol(GE, symbol(GE));
  }
  "eq" {
    return symbol(EQ, symbol(EQ));
  }
  "ne" {
    return symbol(NE, symbol(NE));
  }

  /* isnull operators */
  "isnull" {
    return symbol(ISNULL, symbol(ISNULL));
  }

  /* arithmetic operators */
  "plus" {
    return symbol(PLUS, symbol(PLUS));
  }
  "minus" {
    return symbol(MINUS, symbol(MINUS));
  }
  "div" {
    return symbol(DIV, symbol(DIV));
  }
  "mul" {
    return symbol(MUL, symbol(MUL));
  }

  /* not operators */
  "not" {
    return symbol(NOT, symbol(NOT));
  }

  /* canonical column names */
  {CANONICAL_COL_NAME} {
    return symbol(CANONICAL_COLUMN_NAME, symbol(CANONICAL_COLUMN_NAME, yytext()));
  }

  /* constants */
  {DATE} {
    return symbol(DATE, symbol(DATE, yytext()));
  }
  {FLOAT} {
    return symbol(FLOAT, symbol(FLOAT, Float.valueOf(yytext())));
  }
  {INTEGER} {
    return symbol(INTEGER, symbol(INTEGER, Integer.valueOf(yytext())));
  }

  /* delimiters */
  ", " {}

  /* white spaces */
  {WHITE_SPACE_CHAR}+ {}

  /* parentheses */
  \( {
     return symbol(LPAREN);
  }
  \) {
     return symbol(RPAREN);
  }

  /* string start */
  \" {
    str_buff.setLength(0); yybegin(STRING_LITERAL);
  }

}
<STRING_LITERAL> {
  \" {
    yybegin(YYINITIAL);
    return symbol(STRING, symbol(STRING, str_buff.toString()));
  }
  [^\n\r\"\\]+                   { str_buff.append( yytext() ); }
  \\t                            { str_buff.append('\t'); }
  \\n                            { str_buff.append('\n'); }
  \\r                            { str_buff.append('\r'); }
  \\\"                           { str_buff.append('\"'); }
  \\                             { str_buff.append('\\'); }
}

<<EOF>>                          { return symbol(EOF); }

. {
   throw new IllegalCharacterException(yytext(), yyline + 1, yycolumn + 1);
}

