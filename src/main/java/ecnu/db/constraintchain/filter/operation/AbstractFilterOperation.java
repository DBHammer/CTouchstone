package ecnu.db.constraintchain.filter.operation;

import com.google.common.collect.Lists;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.Parameter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

    public AbstractFilterOperation(CompareOperator operator) {
        this.operator = operator;
    }

    @Override
    public List<AbstractFilterOperation> pushDownProbability(BigDecimal probability, Set<String> columns) {
        this.probability = probability;
        return Lists.newArrayList(this);
    }

    public void addParameter(Parameter parameter) {
        parameters.add(parameter);
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
