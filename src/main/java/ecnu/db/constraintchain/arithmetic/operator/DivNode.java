package ecnu.db.constraintchain.arithmetic.operator;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;

import java.util.stream.IntStream;

/**
 * @author wangqingshuai
 */
public class DivNode extends ArithmeticNode {
    public DivNode() {
        super(ArithmeticNodeType.DIV);
    }

    @Override
    public double[] calculate() {
        double[] leftValue = leftNode.calculate(), rightValue = rightNode.calculate();
        IntStream.range(0, leftValue.length).parallel().forEach(i -> leftValue[i] /= rightValue[i] == 0 ? Double.MIN_NORMAL : rightValue[i]);
        return leftValue;
    }

    @Override
    public String toString() {
        return String.format("div(%s, %s)", leftNode.toString(), rightNode.toString());
    }
}
