package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.Parameter;

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
    protected final CompareOperator operator;
    /**
     * 此filter operation的过滤比
     */
    protected BigDecimal probability;

    /**
     * 计算Filter Operation实例化的参数
     */
    public abstract void instantiateParameter();

    @Override
    public void calculateProbability(BigDecimal probability) {
        this.probability = probability;
    }

    public AbstractFilterOperation(CompareOperator operator) {
        this.operator = operator;
    }

    public void addParameter(Parameter parameter) {
        parameters.add(parameter);
    }

    public List<Parameter> getParameters() {
        return this.parameters;
    }
}
