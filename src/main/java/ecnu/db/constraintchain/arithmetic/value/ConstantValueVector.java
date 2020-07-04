package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.utils.TouchstoneToolChainException;

import java.util.Arrays;

/**
 * @author wangqingshuai
 */
public class ConstantValueVector implements ArithmeticNode {
    private final float determinedValue;
    private final int size;

    public ConstantValueVector(float determinedValue, int size) {
        this.determinedValue = determinedValue;
        this.size = size;
    }

    public ConstantValueVector(int determinedValue, int size) {
        this.determinedValue = determinedValue;
        this.size = size;
    }

    @Override
    public float[] getValue() throws TouchstoneToolChainException {
        float[] value = new float[size];
        Arrays.fill(value, determinedValue);
        return value;
    }
}
