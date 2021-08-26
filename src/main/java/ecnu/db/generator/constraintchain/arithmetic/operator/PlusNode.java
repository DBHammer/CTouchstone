package ecnu.db.generator.constraintchain.arithmetic.operator;

import ecnu.db.generator.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.generator.constraintchain.arithmetic.ArithmeticNodeType;

import java.util.stream.IntStream;

/**
 * @author wangqingshuai
 */
public class PlusNode extends ArithmeticNode {
    public PlusNode() {
        super(ArithmeticNodeType.PLUS);
    }

    @Override
    public double[] calculate() {
        double[] leftValue = leftNode.calculate(), rightValue = rightNode.calculate();
        IntStream.range(0, leftValue.length).parallel().forEach(i -> leftValue[i] += rightValue[i]);
        return leftValue;
    }

    @Override
    public String toString() {
        return String.format("plus(%s, %s)", leftNode.toString(), rightNode.toString());
    }
}
