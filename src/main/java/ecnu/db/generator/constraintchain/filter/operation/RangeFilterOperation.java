package ecnu.db.generator.constraintchain.filter.operation;

import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.schema.ColumnManager;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static ecnu.db.generator.constraintchain.filter.operation.CompareOperator.GT;
import static ecnu.db.generator.constraintchain.filter.operation.CompareOperator.LT;

public class RangeFilterOperation extends UniVarFilterOperation {
    private final CompareOperator leftOperator;
    private final CompareOperator rightOperator;

    private final Parameter rightParameter;

    public RangeFilterOperation(UniVarFilterOperation leftOperation, UniVarFilterOperation rightOperation, BigDecimal probability) {
        super(leftOperation.canonicalColumnName, CompareOperator.EQ, new ArrayList<>(leftOperation.parameters));
        leftOperator = leftOperation.operator;
        rightOperator = rightOperation.operator;
        assert parameters.size() == 1;
        if (rightOperation.getParameters().size() > 1) {
            throw new IllegalArgumentException();
        }
        this.rightParameter = rightOperation.getParameters().get(0);
        this.probability = probability;
    }

    @Override
    public String toString() {
        List<String> content = parameters.stream().map(Parameter::toString).collect(Collectors.toList());
        content.add(0, String.format("%s", canonicalColumnName));
        return String.format("%s(%s)", operator.toString().toLowerCase(), String.join(", ", content));
    }

    @Override
    public void amendParameters() {
        long leftData = parameters.get(0).getData();
        if (rightOperator == LT) {
            rightParameter.setData(leftData + 1);
        } else {
            rightParameter.setData(leftData);
        }
        if (leftOperator == GT) {
            parameters.get(0).setData(leftData - 1);
        }
        for (Parameter parameter : parameters) {
            String value = ColumnManager.getInstance().getColumn(canonicalColumnName).transferDataToValue(parameter.getData());
            parameter.setDataValue(value);
        }
    }

    @Override
    public String toSQL() {
        return canonicalColumnName + " between " + parameters.get(0).getDataValue() + " and " + parameters.get(1).getDataValue();
    }
}
