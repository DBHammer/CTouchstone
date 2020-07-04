package ecnu.db.constraintchain.arithmetic.operator;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author wangqingshuai
 */
public abstract class AbstractArithmeticOperator implements ArithmeticNode {
    protected ArithmeticNode leftNode;
    protected ArithmeticNode rightNode;

    public AbstractArithmeticOperator(ArithmeticNode leftNode, ArithmeticNode rightNode) {
        this.leftNode = leftNode;
        this.rightNode = rightNode;
    }

    /**
     * 返回操作符计算的值
     *
     * @return 操作符计算的值
     */
    @Override
    public float[] getValue() throws TouchstoneToolChainException {
        float[] leftValue = leftNode.getValue();
        float[] rightValue = rightNode.getValue();
        if (leftValue.length != rightValue.length) {
            throw new TouchstoneToolChainException("向量长度不一致，无法计算");
        }
        return getValue(leftValue, rightValue);
    }


    /**
     * 根据左值和右值计算结果
     *
     * @param leftValue  左向量
     * @param rightValue 右向量
     * @return 计算后的向量
     */
    abstract float[] getValue(float[] leftValue, float[] rightValue);
}
