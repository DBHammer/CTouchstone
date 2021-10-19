package ecnu.db.generator.constraintchain.filter;

import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.utils.exception.schema.CannotFindColumnException;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author wangqingshuai
 */
public interface BoolExprNode {


    /**
     * 计算所有子节点的概率
     *
     * @param probability 当前节点的总概率
     */
    List<AbstractFilterOperation> pushDownProbability(BigDecimal probability);

    /**
     * 获得当前布尔表达式节点的类型
     *
     * @return 类型
     */
    BoolExprType getType();

    /**
     * 获取生成好column以后，evaluate表达式的布尔值
     *
     * @return evaluate表达式的布尔值
     */
    boolean[] evaluate() throws CannotFindColumnException;

    /**
     * 获取该filter条件中的所有参数
     *
     * @return 所有的参数
     */
    List<Parameter> getParameters();

    void reverse();

    boolean isTrue();

    List<BoolExprNode>  initProbability();
}
