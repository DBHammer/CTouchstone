package ecnu.db.constraintchain.filter.logical;

import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprType;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 */
public class OrNode implements BoolExprNode {
    private BoolExprNode leftNode;
    private BoolExprNode rightNode;
    private final BoolExprType type = BoolExprType.OR;

    public void setLeftNode(BoolExprNode leftNode) {
        this.leftNode = leftNode;
    }

    public void setRightNode(BoolExprNode rightNode) {
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

    @Override
    public BoolExprType getType() {
        return type;
    }

    @Override
    public String toString() {
        return String.format("or(%s, %s)", leftNode.toString(), rightNode.toString());
    }
}
