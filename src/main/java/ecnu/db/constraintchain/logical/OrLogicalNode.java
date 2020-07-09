package ecnu.db.constraintchain.logical;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 */
public class OrLogicalNode implements LogicalNode {
    private LogicalNode leftNode;
    private LogicalNode rightNode;

    public void setLeftNode(LogicalNode leftNode) {
        this.leftNode = leftNode;
    }

    public void setRightNode(LogicalNode rightNode) {
        this.rightNode = rightNode;
    }

    /**
     * todo 计算所有子节点的 概率
     *
     * @param probability 当前节点的总概率值
     */
    @Override
    public void calculateProbability(BigDecimal probability) {

    }
}
