package ecnu.db.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;

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

    @Override
    public boolean[] evaluate(Schema schema, int size) throws TouchstoneException {
        AbstractColumn column = schema.getColumn(columnName.split("\\.")[2]);
        boolean[] value = new boolean[size], columnIsnullEvaluations = column.getIsnullEvaluations();
        for (int i = 0; i < size; i++) {
            value[i] = (hasNot ^ columnIsnullEvaluations[i]);
        }
        return value;
    }
}
