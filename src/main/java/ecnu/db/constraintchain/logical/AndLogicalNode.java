package ecnu.db.constraintchain.logical;

import java.math.BigDecimal;
import java.util.LinkedList;

/**
 * @author wangqingshuai
 */
public class AndLogicalNode implements LogicalNode {
    LinkedList<LogicalNode> logicalNodes;

    public AndLogicalNode() {
        this.logicalNodes = new LinkedList<LogicalNode>();
    }

    public void addLogicalNode(LogicalNode logicalNode) {
        logicalNodes.add(logicalNode);
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
