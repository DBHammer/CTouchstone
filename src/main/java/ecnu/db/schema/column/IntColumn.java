package ecnu.db.schema.column;

import java.math.BigDecimal;

/**
 * @author qingshuai.wang
 */
public class IntColumn extends AbstractColumn {
    private int min;
    private int max;
    private int ndv;

    public IntColumn() {
        super(null, ColumnType.INTEGER);
    }

    public IntColumn(String columnName) {
        super(columnName, ColumnType.INTEGER);
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    @Override
    public int getNdv() {
        return this.ndv;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }

    @Override
    protected String generateEqData(BigDecimal minProbability, BigDecimal maxProbability) {
        String data;
        double minP = minProbability.doubleValue(), maxP = maxProbability.doubleValue();
        do {
            data = Integer.toString((int) (Math.random() * (maxP - minP) * (max - min) + minP * (max - min) + min));
        } while (eqCandidates.contains(data));
        eqCandidates.add(data);
        return data;
    }

    public int generateData(BigDecimal probability) {
        return BigDecimal.valueOf(getMax() - getMin()).multiply(probability).intValue() + getMin();
    }
}
