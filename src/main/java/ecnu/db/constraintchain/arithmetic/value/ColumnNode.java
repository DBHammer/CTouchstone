package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.utils.TouchstoneToolChainException;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author wangqingshuai
 */
public class ColumnNode extends ArithmeticNode {
    private String columnName;
    private Number min;
    private Number max;

    public ColumnNode() {
        super(null, null);
    }

    public ColumnNode(String columnName) {
        super(null, null);
        this.columnName = columnName;
    }

    public void setMinMax(@NonNull Float min, @NonNull Float max) throws TouchstoneToolChainException {
        if (min > max) {
            throw new TouchstoneToolChainException("非法的随机生成定义");
        }
        this.columnName = columnName;
        this.min = min;
        this.max = max;
    }

    public void setMinMax(@NonNull Integer min, @NonNull Integer max) throws TouchstoneToolChainException {
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

    public float[] getValue(Integer max, Integer min) {
        int size = ArithmeticNode.getSize();
        float[] value = new float[size];
        int bound = max - min;
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            value[i] = threadLocalRandom.nextInt(bound) + min;
        }
        return value;
    }

    public float[] getValue(Float max, Float min) {
        int size = ArithmeticNode.getSize();
        float[] value = new float[size];
        float bound = max - min;
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            value[i] = threadLocalRandom.nextFloat() * bound + min;
        }
        return value;
    }

    @Override
    public float[] getVector() throws TouchstoneToolChainException {
        if (max instanceof Integer && min instanceof Integer) {
            return getValue((Integer) max, (Integer) min);
        } else if (max instanceof Float && min instanceof Float) {
            return getValue((Float) max, (Float) min);
        }
        return new float[0];
    }

    @Override
    public String toString() {
        return String.format("column(%s)", columnName);
    }
}
