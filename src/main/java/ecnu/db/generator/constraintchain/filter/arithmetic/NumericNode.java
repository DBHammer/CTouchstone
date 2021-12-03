package ecnu.db.generator.constraintchain.filter.arithmetic;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.Arrays;

/**
 * @author wangqingshuai
 */
public class NumericNode extends ArithmeticNode {
    private Float constant;
    private String strVal;

    public NumericNode() {
        super(ArithmeticNodeType.CONSTANT);
    }

    public Float getConstant() {
        return constant;
    }

    public void setConstant(float constant) {
        this.strVal = Float.toString(constant);
        this.constant = constant;
    }

    public void setConstant(int constant) {
        this.strVal = Integer.toString(constant);
        this.constant = (float) constant;
    }

    @JsonSetter
    public void setConstant(String constant) {
        this.strVal = constant;
        this.constant = Float.parseFloat(constant);
    }

    @Override
    public double[] calculate() {
        double[] value = new double[size];
        Arrays.fill(value, constant);
        return value;
    }

    @JsonIgnore
    @Override
    public boolean isDifferentTable(String tableName) {
        return false;
    }

    @Override
    public String toSQL() {
        return constant.toString();
    }

    @Override
    public String toString() {
        return strVal;
    }
}
