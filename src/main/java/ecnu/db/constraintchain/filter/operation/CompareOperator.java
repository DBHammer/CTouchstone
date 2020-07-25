package ecnu.db.constraintchain.filter.operation;

/**
 * @author wangqingshuai
 * 比较算子
 */

public enum CompareOperator {
    /**
     * 比较运算符，小于
     */
    LT,
    /**
     * 比较运算符，小于
     */
    GT,
    /**
     * 比较运算符，等于
     */
    EQ,
    /**
     * 比较运算符，不等于
     */
    NE,
    /**
     * 比较运算符，相似
     */
    LIKE,
    /**
     * 比较运算符，包含
     */
    IN,
    /**
     * ISNULL运算符
     */
    ISNULL,
    /**
     * 小于或等于
     */
    LE,
    /**
     * 大于或等于
     */
    GE
}
