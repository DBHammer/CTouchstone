package ecnu.db.constraintchain.logical;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 * todo 当前认为所有的LogicalNode都是相互独立的
 */
public interface LogicalNode {
    /**
     * 计算所有子节点的概率
     *
     * @param probability 当前节点的总概率
     */
    void calculateProbability(BigDecimal probability);
}
