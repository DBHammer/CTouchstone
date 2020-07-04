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
     * 比较运算符，区间
     */
    BET,
    /**
     * 比较运算符，等于
     */
    EQ,
    /**
     * 比较运算符，相似
     */
    LIKE,
    /**
     * 比较运算符，包含
     */
    IN
}
