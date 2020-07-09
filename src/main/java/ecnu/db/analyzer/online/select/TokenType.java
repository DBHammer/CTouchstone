package ecnu.db.analyzer.online.select;

/** The tokens returned by the scanner. */

enum TokenType {
    /*
     */
    LOGIC_OPERATOR,
    ARITHMETIC_OPERATOR,
    ISNULL_OPERATOR,
    COMPARE_OPERATOR,
    CANONICAL_COL_NAME,
    DATE,
    FLOAT,
    INTEGER,
    STRING,
    LEFT_PARANTHESIS,
    RIGHT_PARANTHESIS
}
