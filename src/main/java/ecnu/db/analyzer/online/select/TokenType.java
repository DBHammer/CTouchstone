package ecnu.db.analyzer.online.select;

/**
 * @author alan
 */
public enum TokenType {
    /**
     * logical_operator: and, or
     * arithmetic_operator: mul, plus, minus, div
     * isnull_operator: isnull
     * uni_compare_operator: eq, ne, lt, gt, le, ge
     * multi_compare_operator: like, in
     */
    LOGICAL_OPERATOR,
    NOT_OPERATOR,
    ARITHMETIC_OPERATOR,
    ISNULL_OPERATOR,
    UNI_COMPARE_OPERATOR,
    MULTI_COMPARE_OPERATOR,
    CANONICAL_COL_NAME,
    CONSTANT,
    RIGHT_PARENTHESIS
}
