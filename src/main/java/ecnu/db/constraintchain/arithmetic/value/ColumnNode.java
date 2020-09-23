package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.exception.InstantiateParameterException;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.DecimalColumn;
import ecnu.db.schema.column.IntColumn;
import ecnu.db.schema.column.bucket.EqBucket;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
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

    public void setMinMax(float min, float max) throws TouchstoneToolChainException {
        if (min > max) {
            throw new TouchstoneToolChainException("非法的随机生成定义");
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
    public float[] getVector(Schema schema) throws TouchstoneToolChainException {
        AbstractColumn column = schema.getColumn(columnName);
        if (column instanceof IntColumn) {
            setMinMax((float) ((IntColumn) column).getMin(), (float) ((IntColumn) column).getMax());
        } else if (column instanceof DecimalColumn) {
            setMinMax((float) ((DecimalColumn) column).getMin(), (float) ((DecimalColumn) column).getMax());
        } else {
            throw new InstantiateParameterException(String.format("计算节点出现非法的column'%s'", column));
        }
        List<EqBucket> eqBuckets = column.getEqBuckets();
        eqBuckets.sort(Comparator.comparing(o -> o.leftBorder));
        BigDecimal cumBorder = BigDecimal.ZERO, size = BigDecimal.valueOf(ArithmeticNode.size);
        float[] value = new float[ArithmeticNode.size];
        for (EqBucket eqBucket : eqBuckets) {
            for (Map.Entry<BigDecimal, Parameter> entry : eqBucket.eqConditions.entries()) {
                BigDecimal newCum = cumBorder.add(entry.getKey()).multiply(size);
                float eqValue = Float.parseFloat(entry.getValue().getData());
                for (int j = cumBorder.intValue(); j < newCum.intValue() && j < ArithmeticNode.size; j++) {
                    value[j] = eqValue;
                }
                cumBorder = newCum;
            }
        }
        float bound = max - min;
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        for (int i = cumBorder.intValue(); i < ArithmeticNode.size; i++) {
            value[i] = threadLocalRandom.nextFloat() * bound + min;
        }
        if (cumBorder.compareTo(BigDecimal.ZERO) > 0) {
            Random rand = new Random();
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
    public String toString() {
        return String.format("%s.%s", canonicalTableName, columnName);
    }
}
