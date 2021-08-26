package ecnu.db.generator.constraintchain.arithmetic;

/**
 * @author wangqingshuai
 */
public abstract class ArithmeticNode {
    protected static int size = -1;
    protected ArithmeticNode leftNode;
    protected ArithmeticNode rightNode;
    protected ArithmeticNodeType type;

    public ArithmeticNode(ArithmeticNodeType type) {
        this.type = type;
    }

    public static void setSize(int size) {
        ArithmeticNode.size = size;
    }

    public ArithmeticNodeType getType() {
        return this.type;
    }

    public void setType(ArithmeticNodeType type) {
        this.type = type;
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

    /**
     * 获取当前节点在column生成好数据以后的计算结果
     *
     * @return 返回double类型的计算结果
     */
    abstract public double[] calculate();
}
