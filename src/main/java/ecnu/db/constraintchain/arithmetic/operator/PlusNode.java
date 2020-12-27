package ecnu.db.constraintchain.arithmetic.operator;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.utils.exception.schema.CannotFindColumnException;

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
