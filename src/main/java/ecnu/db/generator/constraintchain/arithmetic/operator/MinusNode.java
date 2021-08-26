package ecnu.db.generator.constraintchain.arithmetic.operator;

import ecnu.db.generator.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.generator.constraintchain.arithmetic.ArithmeticNodeType;

import java.util.stream.IntStream;

/**
 * @author wangqingshuai
 */
public class MinusNode extends ArithmeticNode {
    public MinusNode() {
        super(ArithmeticNodeType.MINUS);
    }

    @Override
    public double[] calculate() {
        double[] leftValue = leftNode.calculate(), rightValue = rightNode.calculate();
        IntStream.range(0, leftValue.length).parallel().forEach(i -> leftValue[i] -= rightValue[i]);
        return leftValue;
    }

    @Override
    public String toString() {
        return String.format("minus(%s, %s)", leftNode.toString(), rightNode.toString());
    }
}
