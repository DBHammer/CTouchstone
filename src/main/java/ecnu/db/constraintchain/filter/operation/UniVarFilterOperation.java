package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author wangqingshuai
 */
public class UniVarFilterOperation extends AbstractFilterOperation {
    private final String columnName;
    private Boolean hasNot = false;

    public UniVarFilterOperation(String columnName, CompareOperator operator) {
        super(operator);
        this.columnName = columnName;
    }

    @Override
    public String toString() {
        List<String> content = parameters.stream().map(Parameter::toString).collect(Collectors.toList());
        content.add(0, String.format("column(%s)", columnName));
        if (hasNot) {
            return String.format("not_%s(%s)", operator.toString().toLowerCase(), String.join(", ", content));
        }
        return String.format("%s(%s)", operator.toString().toLowerCase(), String.join(", ", content));
    }

    /**
     * todo 参数实例化
     */
    @Override
    public void instantiateParameter() {

    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
    }

    public Boolean getHasNot() {
        return hasNot;
    }

    public void setHasNot(Boolean hasNot) {
        this.hasNot = hasNot;
    }
}
