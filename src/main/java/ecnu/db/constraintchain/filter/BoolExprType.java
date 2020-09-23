package ecnu.db.constraintchain.filter;

/**
 * @author alan
 */
public enum BoolExprType {
    /*
     * bool expression types
     */
    AND,
    OR,
    UNI_FILTER_OPERATION,
    MULTI_FILTER_OPERATION,
    ISNULL_FILTER_OPERATION,
    BETWEEN_FILTER_OPERATION
}
