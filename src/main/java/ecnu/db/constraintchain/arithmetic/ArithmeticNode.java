package ecnu.db.constraintchain.arithmetic;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author wangqingshuai
 */
public abstract class ArithmeticNode {
    protected ArithmeticNode leftNode;
    protected ArithmeticNode rightNode;
    protected ArithmeticNodeType type;
    protected static int size = -1;

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

    public static void setSize(int size) throws TouchstoneToolChainException {
        if (ArithmeticNode.size == -1){
            ArithmeticNode.size = size;
        }else{
             throw new TouchstoneToolChainException("不应该重复设置size");
        }
    }
}
