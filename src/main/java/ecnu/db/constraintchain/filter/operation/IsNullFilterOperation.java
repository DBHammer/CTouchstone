package ecnu.db.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.constraintchain.filter.BoolExprType;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

/**
 * @author alan
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IsNullFilterOperation extends AbstractFilterOperation {
    private String columnName;
    private Boolean hasNot = false;

    public IsNullFilterOperation() {
        super(CompareOperator.ISNULL);
    }

    public IsNullFilterOperation(String columnName, BigDecimal probability) {
        super(CompareOperator.ISNULL);
        this.columnName = columnName;
        this.probability = probability;
    }

    @Override
    public List<AbstractFilterOperation> pushDownProbability(BigDecimal probability, Set<String> columns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.ISNULL_FILTER_OPERATION;
    }

    @Override
    public String toString() {
        if (hasNot) {
            return String.format("not(isnull(%s))", this.columnName);
        }
        return String.format("isnull(%s)", this.columnName);
    }

    public Boolean getHasNot() {
        return hasNot;
    }

    public void setHasNot(Boolean hasNot) {
        this.hasNot = hasNot;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

}
