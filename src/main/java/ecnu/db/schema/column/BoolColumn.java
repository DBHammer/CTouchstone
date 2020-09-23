package ecnu.db.schema.column;

import java.math.BigDecimal;

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
    protected String generateEqData(BigDecimal minProbability, BigDecimal maxProbability) {
        String data;
        do {
            data = Boolean.toString(BigDecimal.valueOf(Math.random() * (maxProbability.subtract(minProbability).doubleValue())).add(minProbability).doubleValue() > 0.5);
        } while (eqCandidates.contains(data));
        eqCandidates.add(data);
        return data;
    }
}
