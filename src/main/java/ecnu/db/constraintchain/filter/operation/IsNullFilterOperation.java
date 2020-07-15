package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.filter.BoolExprType;

/**
 * @author alan
 */
public class IsNullFilterOperation extends AbstractFilterOperation {
    private final String columnName;
    private Boolean hasNot = false;

    public IsNullFilterOperation(String columnName) {
        super(CompareOperator.ISNULL);
        this.columnName = columnName;
    }

    @Override
    public void instantiateParameter() {}

    @Override
    public BoolExprType getType() {
        return BoolExprType.ISNULL_FILTER_OPERATION;
    }

    @Override
    public String toString() {
        if (hasNot) {
            return String.format("not_isnull(column(%s))", this.columnName);
        }
        return String.format("isnull(column(%s))", this.columnName);
    }

    public Boolean getHasNot() {
        return hasNot;
    }

    public void setHasNot(Boolean hasNot) {
        this.hasNot = hasNot;
    }
}
