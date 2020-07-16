package ecnu.db.constraintchain.arithmetic;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author wangqingshuai
 */
public abstract class ArithmeticNode {
    protected ArithmeticNode leftNode;
    protected ArithmeticNode rightNode;
    protected ArithmeticNodeType type;
    private static Integer size;

    protected ArithmeticNode() {}

    protected ArithmeticNode(ArithmeticNode leftNode, ArithmeticNode rightNode) {
        this.leftNode = leftNode;
        this.rightNode = rightNode;
    }

    /**
     * 获取当前节点的计算结果
     *
     * @return 返回float类型的计算结果
     * @throws TouchstoneToolChainException 无法获取值向量
     */
    public abstract float[] getVector() throws TouchstoneToolChainException;

    public ArithmeticNodeType getType() {
        return this.type;
    }

    public ArithmeticNode getLeftNode() {
        return leftNode;
    }

    public void setLeftNode(ArithmeticNode leftNode) {
        this.leftNode = leftNode;
    }

    public ArithmeticNode getRightNode() {
        return rightNode;
    }

    public void setRightNode(ArithmeticNode rightNode) {
        this.rightNode = rightNode;
    }

    public static void setSize(Integer size) {
        ArithmeticNode.size = size;
    }

    public static Integer getSize() {
        return size;
    }
}
