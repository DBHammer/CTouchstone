package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.logical.LogicalNode;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 */
public abstract class AbstractFilterOperation implements LogicalNode {
    /**
     * 此filter operation在约束链中的位置，用于自动化填充query模版
     */
    protected final int id;
    /**
     * 此filter operation的操作符
     */
    protected final CompareOperator operator;
    /**
     * 此filter operation的过滤比
     */
    protected double probability;

    public AbstractFilterOperation(int id, CompareOperator operator) {
        this.id = id;
        this.operator = operator;
    }

    /**
     * 计算Filter Operation实例化的参数
     */
    public abstract void calculateParameter();

    @Override
    public void calculateProbability(BigDecimal probability) {
        this.probability = probability.doubleValue();
    }
}
