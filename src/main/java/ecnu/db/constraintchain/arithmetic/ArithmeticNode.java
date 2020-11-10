package ecnu.db.constraintchain.arithmetic;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ecnu.db.exception.schema.CannotFindColumnException;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.schema.Schema;

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

    public static void setSize(int size) throws TouchstoneException {
        if (ArithmeticNode.size == -1) {
            ArithmeticNode.size = size;
        } else {
            throw new TouchstoneException("不应该重复设置size");
        }
    }

    /**
     * 获取当前节点的计算结果
     *
     * @param schema filter所在的schema，用于查找column
     * @return 返回float类型的计算结果
     */
    @JsonIgnore
    public abstract float[] getVector(Schema schema) throws TouchstoneException;

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
     * @param schema filter所在的schema，用于查找column
     * @param size 生成数据的size
     * @return 返回double类型的计算结果
     * @throws CannotFindColumnException 找不的column
     */
    abstract public double[] calculate(Schema schema, int size) throws CannotFindColumnException;
}
