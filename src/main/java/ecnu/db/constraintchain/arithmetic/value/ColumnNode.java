package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.utils.TouchstoneToolChainException;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author wangqingshuai
 */
public class ColumnNode extends ArithmeticNode {
    private String columnName;
    private float min;
    private float max;

    public ColumnNode() {}

    public ColumnNode(String columnName) {
        this.columnName = columnName;
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

    @Override
    public float[] getVector() {
        int size = ArithmeticNode.size;
        float[] value = new float[size];
        float bound = max - min;
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            value[i] = threadLocalRandom.nextFloat() * bound + min;
        }
        return value;
    }

    @Override
    public String toString() {
        return String.format("column(%s)", columnName);
    }
}
