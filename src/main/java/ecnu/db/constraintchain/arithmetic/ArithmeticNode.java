package ecnu.db.constraintchain.arithmetic;

/**
 * @author wangqingshuai
 */
public abstract class ArithmeticNode {
    protected ArithmeticNode leftNode;
    protected ArithmeticNode rightNode;
    protected ArithmeticNodeType type;
    private static Integer size;

    /**
     * 获取当前节点的计算结果
     *
     * @return 返回float类型的计算结果
     */
    public abstract float[] getVector();

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
