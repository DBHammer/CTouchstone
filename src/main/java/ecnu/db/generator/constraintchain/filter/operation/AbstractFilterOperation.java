package ecnu.db.generator.constraintchain.filter.operation;

import ecnu.db.generator.constraintchain.filter.BoolExprNode;
import ecnu.db.generator.constraintchain.filter.Parameter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangqingshuai
 */
public abstract class AbstractFilterOperation implements BoolExprNode {
    /**
     * 此filter包含的参数
     */
    protected List<Parameter> parameters = new ArrayList<>();
    /**
     * 此filter operation的操作符
     */
    protected CompareOperator operator;
    /**
     * 此filter operation的过滤比
     */
    protected BigDecimal probability;

    /**
     * 是否在化简的过程中被reverse过，默认为false
     */
    public boolean isReverse = false;

    AbstractFilterOperation(CompareOperator operator) {
        this.operator = operator;
    }

    @Override
    public List<AbstractFilterOperation> pushDownProbability(BigDecimal probability) {
        if (probability.compareTo(BigDecimal.ONE) == 0) {
            probability = BigDecimal.ZERO;
        }
        if (isReverse) {
            probability = BigDecimal.ONE.subtract(probability);
        }
        this.probability = probability;
        List<AbstractFilterOperation> chain = new ArrayList<>();
        chain.add(this);
        return chain;
    }

    @Override
    public void reverse() {
        isReverse = !isReverse;
    }

    @Override
    public boolean isTrue() {
        probability = isReverse ? BigDecimal.ZERO : BigDecimal.ONE;
        return switch (operator) {
            case LE, GE, GT, LT -> true;
            case EQ, LIKE, IN -> isReverse;
            case NE, NOT_LIKE, NOT_IN -> !isReverse;
            case IS_NOT_NULL, ISNULL, RANGE -> throw new UnsupportedOperationException();
        };
    }

    public List<Parameter> getParameters() {
        return this.parameters;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    public CompareOperator getOperator() {
        return operator;
    }

    public void setOperator(CompareOperator operator) {
        this.operator = operator;
    }

    public BigDecimal getProbability() {
        return probability;
    }

    public void setProbability(BigDecimal probability) {
        this.probability = probability;
    }
}
