package ecnu.db.analyzer.online.adapter.pg.parser;

import ecnu.db.utils.exception.analyze.IllegalCharacterException;
import ecnu.db.analyzer.online.adapter.pg.Token;
import java_cup.runtime.*;
import ecnu.db.generator.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
%%

%public
%class PgSelectOperatorInfoLexer
/* throws TouchstoneException */
%yylexthrow{
ecnu.db.utils.exception.TouchstoneException
%yylexthrow}

%{
  private StringBuilder str_buff = new StringBuilder();
  private Symbol symbol(int type) {
    return new Token(type, yycolumn+1);
  }

  private Symbol symbol(int type, Object value) {
    return new Token(type, yycolumn+1, value);
  }

  public void init() {
    System.out.println("initialized");
  }
%}

%implements PgSelectSymbol
%line
%column
%state STRING_LITERAL
%state STRING_LITERAL_DOUBLE_QUOTATION
%state IN_LIST
%unicode
%cupsym PgSelectSymbol
%cup

/* tokens */
DIGIT=[0-9]
STRING=[A-Za-z0-9$_]+
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
    return symbol(AND);
  }
  "or" {
    return symbol(OR);
  }

  /* compare operators */
  "= ANY" {
    return symbol(IN, CompareOperator.IN);
  }
  "<> ALL" {
    return symbol(NOT_IN, CompareOperator.NOT_IN);
  }
  "~~" {
    return symbol(LIKE, CompareOperator.LIKE);
  }
  "<" {
    return symbol(LT, CompareOperator.LT);
  }
  ">" {
    return symbol(GT, CompareOperator.GT);
  }
  "<=" {
    return symbol(LE, CompareOperator.LE);
  }
  ">=" {
    return symbol(GE, CompareOperator.GE);
  }
  "=" {
    return symbol(EQ, CompareOperator.EQ);
  }
  "<>" {
    return symbol(NE, CompareOperator.NE);
  }

  /* isnull operators */
  "IS NULL" {
    return symbol(ISNULL, CompareOperator.ISNULL);
  }

  /* isnull operators */
  "IS NOT NULL" {
    return symbol(IS_NOT_NULL, CompareOperator.IS_NOT_NULL);
  }

  /* arithmetic operators */
  "+" {
    return symbol(PLUS, ArithmeticNodeType.PLUS);
  }
  "-" {
    return symbol(MINUS, ArithmeticNodeType.MINUS);
  }
  "/" {
    return symbol(DIV, ArithmeticNodeType.DIV);
  }
  "*" {
    return symbol(MUL, ArithmeticNodeType.MUL);
  }

  /* not like operators */
  "!~~" {
    return symbol(NOT_LIKE);
  }

  /* canonical column names */
  {CANONICAL_COL_NAME} {
    return symbol(CANONICAL_COLUMN_NAME, yytext());
  }

  /* constants */
  {DATE} {
    return symbol(DATE, yytext());
  }
  {FLOAT} {
    return symbol(FLOAT, Float.valueOf(yytext()));
  }
  {INTEGER} {
    return symbol(INTEGER, Integer.valueOf(yytext()));
  }

  /* delimiters */
  ", " {}

  /* type */
  "::text" {}
  "::bpchar" {}
  "::integer[]" {}
  "::date" {}
  "::timestamp without time zone" {}

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
  \' {
    str_buff.setLength(0); yybegin(STRING_LITERAL);
  }

  /* string by double quotation start */
  \" {
    str_buff.setLength(0); yybegin(STRING_LITERAL_DOUBLE_QUOTATION);
  }

  /* inlist start */
  "'{" {
    str_buff.setLength(0);
    yybegin(IN_LIST);
  }

}
<STRING_LITERAL> {
  \' {
    yybegin(YYINITIAL);
    return symbol(STRING, str_buff.toString());
  }
  [^\n\r'\\]+                   { str_buff.append( yytext() ); }
  \\t                            { str_buff.append('\t'); }
  \\n                            { str_buff.append('\n'); }
  \\r                            { str_buff.append('\r'); }
  \\\"                           { str_buff.append('\"'); }
  \\                             { str_buff.append('\\'); }
}

<STRING_LITERAL_DOUBLE_QUOTATION> {
  \" {
    yybegin(IN_LIST);
    return symbol(STRING, str_buff.toString());
  }
  [^\n\r\"\\]+                   { str_buff.append( yytext() ); }
  \\t                            { str_buff.append('\t'); }
  \\n                            { str_buff.append('\n'); }
  \\r                            { str_buff.append('\r'); }
  \\\"                           { str_buff.append('\"'); }
  \\                             { str_buff.append('\\'); }
}

<IN_LIST> {
  "}'" {
    yybegin(YYINITIAL);
  }

  {STRING} {
    str_buff.append( yytext() );
    return symbol(STRING, str_buff.toString());
  }

  \" {
    str_buff.setLength(0); yybegin(STRING_LITERAL_DOUBLE_QUOTATION);
  }

  {DATE} {
    return symbol(DATE, yytext());
  }

  {FLOAT} {
    return symbol(FLOAT, Float.valueOf(yytext()));
  }

  {INTEGER} {
    return symbol(INTEGER, Integer.valueOf(yytext()));
  }

  "," {str_buff.setLength(0); }

  {WHITE_SPACE_CHAR}+ {}
}

<<EOF>>                          { return symbol(EOF); }

. {
   throw new IllegalCharacterException(yytext(), yyline + 1, yycolumn + 1);
}