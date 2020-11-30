package ecnu.db.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.schema.ColumnManager;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

/**
 * @author alan
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IsNullFilterOperation extends AbstractFilterOperation {
    public void setCanonicalColumnName(String canonicalColumnName) {
        this.canonicalColumnName = canonicalColumnName;
    }

    private String canonicalColumnName;
    private Boolean hasNot = false;

    public IsNullFilterOperation() {
        super(CompareOperator.ISNULL);
    }

    public IsNullFilterOperation(String canonicalColumnName, BigDecimal probability) {
        super(CompareOperator.ISNULL);
        this.canonicalColumnName = canonicalColumnName;
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
            return String.format("not(isnull(%s))", this.canonicalColumnName);
        }
        return String.format("isnull(%s)", this.canonicalColumnName);
    }

    public Boolean getHasNot() {
        return hasNot;
    }

    public void setHasNot(Boolean hasNot) {
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
        boolean[] columnIsnullEvaluations = ColumnManager.getInstance().getIsnullEvaluations(canonicalColumnName);
        boolean[] value = new boolean[columnIsnullEvaluations.length];
        for (int i = 0; i < columnIsnullEvaluations.length; i++) {
            value[i] = (hasNot ^ columnIsnullEvaluations[i]);
        }
        return value;
    }
}
