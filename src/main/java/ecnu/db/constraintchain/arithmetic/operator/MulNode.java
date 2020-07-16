package ecnu.db.constraintchain.arithmetic.operator;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;

/**
 * @author wangqingshuai
 */
public class MulNode extends ArithmeticNode {
    @Override
    public float[] getVector() {
        float[] leftValue = leftNode.getVector(), rightValue = rightNode.getVector();
        for (int i = 0; i < leftValue.length; i++) {
            leftValue[i] *= rightValue[i];
        }
        return leftValue;
    }

    @Override
    public String toString() {
        return String.format("mul(%s, %s)", leftNode.toString(), rightNode.toString());
    }
}
