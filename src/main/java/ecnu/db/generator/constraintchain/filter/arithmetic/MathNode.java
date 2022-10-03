package ecnu.db.generator.constraintchain.filter.arithmetic;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.stream.IntStream;

public class MathNode extends ArithmeticNode {

    public MathNode(ArithmeticNodeType type) {
        super(type);
    }

    public MathNode() {
        super(ArithmeticNodeType.MINUS);
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
    public List<String> getColumns() {
        List<String> columnNames = leftNode.getColumns();
        if (rightNode != null) {
            columnNames.addAll(rightNode.getColumns());
        }
        return columnNames;
    }

    @Override
    public String toString() {
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
        if (type.isUniComparator()) {
            return String.format("%s(%s)", mathType, leftNode.toString());
        } else {
            return String.format("%s %s %s", leftNode.toString(), mathType, rightNode.toString());
        }
    }
}
