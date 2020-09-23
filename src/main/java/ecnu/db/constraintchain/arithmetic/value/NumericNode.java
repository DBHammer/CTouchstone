package ecnu.db.constraintchain.arithmetic.value;

import com.fasterxml.jackson.annotation.JsonSetter;
import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.schema.Schema;

import java.util.Arrays;

/**
 * @author wangqingshuai
 */
public class NumericNode extends ArithmeticNode {
    private Float constant;

    public NumericNode() {
        super(ArithmeticNodeType.CONSTANT);
    }

    public Float getConstant() {
        return constant;
    }

    public void setConstant(float constant) {
        this.constant = constant;
    }

    public void setConstant(int constant) {
        this.constant = (float) constant;
    }

    @JsonSetter
    public void setConstant(String constant) {
        this.constant = Float.parseFloat(constant);
    }

    @Override
    public float[] getVector(Schema schema) {
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
