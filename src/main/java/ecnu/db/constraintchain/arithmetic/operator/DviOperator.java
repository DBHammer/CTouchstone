package ecnu.db.constraintchain.arithmetic.operator;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;

/**
 * @author wangqingshuai
 */
public class DviOperator extends AbstractArithmeticOperator {
    public DviOperator(ArithmeticNode leftNode, ArithmeticNode rightNode) {
        super(leftNode, rightNode);
    }

    @Override
    float[] getValue(float[] leftValue, float[] rightValue) {
        for (int i = 0; i < leftValue.length; i++) {
            leftValue[i] /= rightValue[i];
        }
        return leftValue;
    }
}
