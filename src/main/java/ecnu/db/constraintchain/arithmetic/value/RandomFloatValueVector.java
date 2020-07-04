package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.utils.TouchstoneToolChainException;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author wangqingshuai
 */
public class RandomFloatValueVector implements ArithmeticNode {
    private final float min;
    private final float max;
    private final int size;

    public RandomFloatValueVector(float min, float max, int size) throws TouchstoneToolChainException {
        if (min > max) {
            throw new TouchstoneToolChainException("非法的随机生成定义");
        }
        this.min = min;
        this.max = max;
        this.size = size;
    }

    @Override
    public float[] getValue() {
        float[] value = new float[size];
        float bound = max - min;
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            value[i] = threadLocalRandom.nextFloat() * bound + min;
        }
        return value;
    }
}
