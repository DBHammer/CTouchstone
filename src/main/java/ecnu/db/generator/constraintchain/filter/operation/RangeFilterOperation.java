package ecnu.db.generator.constraintchain.filter.operation;

import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.schema.ColumnManager;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

import static ecnu.db.generator.constraintchain.filter.operation.CompareOperator.GT;
import static ecnu.db.generator.constraintchain.filter.operation.CompareOperator.LT;

public class RangeFilterOperation extends UniVarFilterOperation {
    private final CompareOperator leftOperator;
    private final CompareOperator rightOperator;

    public RangeFilterOperation(UniVarFilterOperation leftOperation, UniVarFilterOperation rightOperation, BigDecimal probability) {
        super(leftOperation.canonicalColumnName, CompareOperator.EQ, leftOperation.parameters);
        this.parameters.addAll(rightOperation.parameters);
        leftOperator = leftOperation.operator;
        rightOperator = rightOperation.operator;
        assert parameters.size() == 2;
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
        if (leftOperator == GT) {
            Parameter leftParameter = parameters.get(0);
            leftParameter.setData(leftParameter.getData() - 1);
        }
        if (rightOperator == LT) {
            Parameter rightParameter = parameters.get(1);
            rightParameter.setData(rightParameter.getData() + 1);
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
