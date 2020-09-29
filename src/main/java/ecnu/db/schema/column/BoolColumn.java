package ecnu.db.schema.column;

import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.operation.CompareOperator;
import org.apache.commons.lang3.NotImplementedException;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author qingshuai.wang
 */
public class BoolColumn extends AbstractColumn {
    private BigDecimal trueProbability;

    public BoolColumn() {
        super(null, ColumnType.BOOL);
    }

    public BoolColumn(String columnName) {
        super(columnName, ColumnType.BOOL);
    }

    @Override
    public int getNdv() {
        return -1;
    }

    @Override
    protected String generateEqParamData(BigDecimal minProbability, BigDecimal maxProbability) {
        String data;
        do {
            data = Boolean.toString(BigDecimal.valueOf(Math.random() * (maxProbability.subtract(minProbability).doubleValue())).add(minProbability).doubleValue() > 0.5);
        } while (eqCandidates.contains(data));
        eqCandidates.add(data);
        return data;
    }

    @Override
    public String generateNonEqParamData(BigDecimal probability) {
        throw new NotImplementedException();
    }

    @Override
    public void prepareTupleData(int size) {
        throw new NotImplementedException();
    }

    @Override
    public boolean[] evaluate(CompareOperator operator, List<Parameter> parameters, boolean hasNot) {
        throw new NotImplementedException();
    }

    public boolean[] getTupleData() {
        throw new NotImplementedException();
    }

    @Override
    public void setTupleByRefColumn(AbstractColumn column, int i, int j) {
        throw new NotImplementedException();
    }
}
