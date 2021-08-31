package ecnu.db.generator.constraintchain.arithmetic;

import java.util.stream.IntStream;

public class MathNode extends ArithmeticNode {

    public MathNode(ArithmeticNodeType type) {
        super(type);
    }

    @Override
    public double[] calculate() {
        double[] leftValue = leftNode.calculate();
        double[] rightValue = rightNode.calculate();
        IntStream indexStream = IntStream.range(0, leftValue.length);
        switch (type) {
            case MUL -> indexStream.forEach(i -> leftValue[i] *= rightValue[i]);
            case DIV -> indexStream.forEach(i -> leftValue[i] /= rightValue[i] == 0 ? Double.MIN_NORMAL : rightValue[i]);
            case PLUS -> indexStream.forEach(i -> leftValue[i] += rightValue[i]);
            case MINUS -> indexStream.forEach(i -> leftValue[i] -= rightValue[i]);
            default -> throw new UnsupportedOperationException();
        }
        return leftValue;
    }

    @Override
    public String toString() {
        return String.format("%s(%s, %s)", type, leftNode.toString(), rightNode.toString());
    }
}
