package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;

import java.util.Arrays;

/**
 * @author wangqingshuai
 */
public class NumericNode extends ArithmeticNode {
    private Float constant;

    public void setConstant(float constant) {
        this.constant = constant;
    }

    public void setConstant(int constant) {
        this.constant = (float) constant;
    }

    @Override
    public float[] getVector() {
        int size = ArithmeticNode.size;
        float[] value = new float[size];
        Arrays.fill(value, constant);
        return value;
    }

    @Override
    public String toString() {
        return constant.toString();
    }
}
