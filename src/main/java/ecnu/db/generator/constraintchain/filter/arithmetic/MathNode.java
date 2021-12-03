package ecnu.db.generator.constraintchain.filter.arithmetic;

import com.fasterxml.jackson.annotation.JsonIgnore;

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

    @JsonIgnore
    @Override
    public boolean isDifferentTable(String tableName) {
        return leftNode.isDifferentTable(tableName) || rightNode.isDifferentTable(tableName);
    }

    @Override
    public String toSQL() {
        String mathType = switch (type) {
            case MINUS -> "-";
            case DIV -> "/";
            case MUL -> "*";
            case PLUS -> "+";
            case MAX -> "max";
            case MIN -> "min";
            case AVG -> "avg";
            case SUM -> "sum";
            default -> throw new UnsupportedOperationException();
        };
        return String.format("%s %s %s", leftNode.toSQL(), mathType, rightNode.toSQL());
    }

    @Override
    public String toString() {
        if (rightNode == null) {
            return String.format("%s(%s)", type, leftNode.toString());
        } else {
            return String.format("%s(%s, %s)", type, leftNode.toString(), rightNode.toString());
        }
    }
}
