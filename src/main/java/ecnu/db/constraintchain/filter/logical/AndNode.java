package ecnu.db.constraintchain.filter.logical;

import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprType;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * @author wangqingshuai
 */
public class AndNode implements BoolExprNode {
    private BoolExprType type = BoolExprType.AND;
    private LinkedList<BoolExprNode> children;

    public AndNode() {
        this.children = new LinkedList<>();
    }

    public void addChild(BoolExprNode logicalNode) {
        children.add(logicalNode);
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

    public void setType(BoolExprType type) {
        this.type = type;
    }

    public LinkedList<BoolExprNode> getChildren() {
        return children;
    }

    public void setChildren(LinkedList<BoolExprNode> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        return String.format("and(%s)", children.stream().map(BoolExprNode::toString).collect(Collectors.joining(", ")));
    }
}
