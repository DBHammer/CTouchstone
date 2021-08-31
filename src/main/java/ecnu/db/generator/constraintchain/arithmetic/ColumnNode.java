package ecnu.db.generator.constraintchain.arithmetic;

import ecnu.db.schema.ColumnManager;

import java.util.Arrays;

/**
 * @author wangqingshuai
 */
public class ColumnNode extends ArithmeticNode {
    private String canonicalColumnName;

    public ColumnNode() {
        super(ArithmeticNodeType.COLUMN);
    }

    public String getCanonicalColumnName() {
        return canonicalColumnName;
    }

    public void setCanonicalColumnName(String canonicalColumnName) {
        this.canonicalColumnName = canonicalColumnName;
    }

    @Override
    public double[] calculate() {
        double[] columnValue = ColumnManager.getInstance().calculate(canonicalColumnName);
        return Arrays.copyOf(columnValue, columnValue.length);
    }

    @Override
    public String toString() {
        return canonicalColumnName;
    }
}
