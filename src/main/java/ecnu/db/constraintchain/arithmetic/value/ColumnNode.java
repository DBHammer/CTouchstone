package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.column.bucket.EqBucket;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author wangqingshuai
 */
public class ColumnNode extends ArithmeticNode {
    private String canonicalTableName;
    private String columnName;
    private float min;
    private float max;

    public ColumnNode() {
        super(ArithmeticNodeType.COLUMN);
    }

    public void setMinMax(float min, float max) throws TouchstoneException {
        if (min > max) {
            throw new TouchstoneException("非法的随机生成定义");
        }
        this.min = min;
        this.max = max;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getCanonicalTableName() {
        return canonicalTableName;
    }

    public void setCanonicalTableName(String canonicalTableName) {
        this.canonicalTableName = canonicalTableName;
    }

    @Override
    public float[] getVector() throws TouchstoneException {
        setMinMax(ColumnManager.getInstance().getMin(columnName), ColumnManager.getInstance().getMax(columnName));
        List<EqBucket> eqBuckets = ColumnManager.getInstance().getEqBuckets(columnName);
        eqBuckets.sort(Comparator.comparing(o -> o.leftBorder));
        BigDecimal cumBorder = BigDecimal.ZERO, size = BigDecimal.valueOf(ArithmeticNode.size);
        float[] value = new float[ArithmeticNode.size];
        for (EqBucket eqBucket : eqBuckets) {
            for (Map.Entry<BigDecimal, Parameter> entry : eqBucket.eqConditions.entries()) {
                BigDecimal newCum = cumBorder.add(entry.getKey().multiply(size));
                float eqValue = Float.parseFloat(entry.getValue().getData());
                for (int j = cumBorder.intValue(); j < newCum.intValue() && j < ArithmeticNode.size; j++) {
                    value[j] = eqValue;
                }
                cumBorder = newCum;
            }
        }
        float bound = max - min;
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int i = cumBorder.intValue(); i < ArithmeticNode.size; i++) {
            value[i] = (1 - rand.nextFloat()) * bound + min;
        }
        if (cumBorder.compareTo(BigDecimal.ZERO) > 0) {
            // shuffle
            float tmp;
            for (int i = ArithmeticNode.size; i > 1; i--) {
                int idx = rand.nextInt(i);
                tmp = value[i - 1];
                value[i - 1] = value[idx];
                value[idx] = tmp;
            }
        }
        return value;
    }

    @Override
    public double[] calculate() {
        return ColumnManager.getInstance().calculate(columnName);
    }

    @Override
    public String toString() {
        return String.format("%s.%s", canonicalTableName, columnName);
    }
}
