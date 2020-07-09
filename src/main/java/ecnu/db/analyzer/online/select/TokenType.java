package ecnu.db.analyzer.online.select;

/**
 * @author alan
 */
enum TokenType {
    /**
     * logical_oprator: and, or, not
     * arithmetic_operator: mul, plus, minus, div
     * isnull_operator: isnull
     * compare_operator: le, ge, lt, gt, eq, ne, like, in
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
