package ecnu.db.constraintchain.arithmetic.value;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.utils.TouchstoneToolChainException;

import java.util.Arrays;

/**
 * @author wangqingshuai
 */
public class NumericNode extends ArithmeticNode {
    private Float constant;

    public NumericNode() {
        super(null, null);
    }

    public void setConstant(float constant) {
        this.constant = constant;
    }

    public void setConstant(int constant) {
        this.constant = (float) constant;
    }

    @Override
    public float[] getVector() throws TouchstoneToolChainException {
        int size = ArithmeticNode.getSize();
        float[] value = new float[size];
        Arrays.fill(value, constant);
        return value;
    }

    @Override
    public String toString() {
        return constant.toString();
    }
}
