package ecnu.db.constraintchain.arithmetic.operator;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.utils.exception.schema.CannotFindColumnException;

/**
 * @author wangqingshuai
 * todo 处理rightValue可能出现的0或很小的值
 */
public class DivNode extends ArithmeticNode {
    public DivNode() {
        super(ArithmeticNodeType.DIV);
    }

    @Override
    public float[] getVector() throws TouchstoneException {
        float[] leftValue = leftNode.getVector(), rightValue = rightNode.getVector();
        for (int i = 0; i < leftValue.length; i++) {
            leftValue[i] /= rightValue[i];
        }
        return leftValue;
    }

    @Override
    public double[] calculate() throws CannotFindColumnException {
        double[] leftValue = leftNode.calculate(), rightValue = rightNode.calculate();
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
