package ecnu.db.constraintchain.arithmetic.operator;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author wangqingshuai
 */
public class DivNode extends ArithmeticNode {
    @Override
    public float[] getVector() throws TouchstoneToolChainException {
        float[] leftValue = leftNode.getVector(), rightValue = rightNode.getVector();

        for (int i = 0; i < leftValue.length; i++) {
            leftValue[i] /= rightValue[i];
        }
        return leftValue;
    }

    @Override
    public String toString() {
        return String.format("div(%s, %s)", leftNode.toString(), rightNode.toString());
    }
}
