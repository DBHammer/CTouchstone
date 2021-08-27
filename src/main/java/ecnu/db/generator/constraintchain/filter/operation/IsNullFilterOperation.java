package ecnu.db.generator.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.generator.constraintchain.filter.BoolExprType;
import ecnu.db.schema.ColumnManager;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

/**
 * @author alan
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IsNullFilterOperation extends AbstractFilterOperation {
    private String canonicalColumnName;
    private boolean hasNot = false;

    public IsNullFilterOperation() {
        super(CompareOperator.ISNULL);
    }

    public IsNullFilterOperation(String canonicalColumnName, BigDecimal probability) {
        super(CompareOperator.ISNULL);
        this.canonicalColumnName = canonicalColumnName;
        this.probability = probability;
    }

    public void setCanonicalColumnName(String canonicalColumnName) {
        this.canonicalColumnName = canonicalColumnName;
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
            return String.format("not(isnull(%s))", this.canonicalColumnName);
        }
        return String.format("isnull(%s)", this.canonicalColumnName);
    }

    public boolean getHasNot() {
        return hasNot;
    }

    public void setHasNot(boolean hasNot) {
        this.hasNot = hasNot;
    }

    public String getColumnName() {
        return canonicalColumnName;
    }

    public void setColumnName(String columnName) {
        this.canonicalColumnName = columnName;
    }

    @Override
    public boolean[] evaluate() {
        return ColumnManager.getInstance().evaluate(canonicalColumnName, CompareOperator.ISNULL, null, hasNot);
    }
}
