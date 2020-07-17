package ecnu.db.constraintchain.filter;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 * todo 当前认为所有的BoolExprNode都是相互独立的
 */
public interface BoolExprNode {
    /**
     * 计算所有子节点的概率
     *
     * @param probability 当前节点的总概率
     */
    void calculateProbability(BigDecimal probability);

    /**
     * 获得当前布尔表达式节点的类型
     * @return 类型
     */
    BoolExprType getType();
}
