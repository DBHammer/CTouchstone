package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.utils.TouchstoneToolChainException;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author wangqingshuai
 */
public class RandomIntValueVector implements ArithmeticNode {
    private final int min;
    private final int max;
    private final int size;

    public RandomIntValueVector(int min, int max, int size) throws TouchstoneToolChainException {
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
        int bound = max - min;
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            value[i] = threadLocalRandom.nextInt(bound) + min;
        }
        return value;
    }
}
