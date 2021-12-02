package ecnu.db.generator.constraintchain.filter.arithmetic;

import ecnu.db.schema.ColumnManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.analyze.IllegalQueryColumnNameException;

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

    public void setCanonicalColumnName(String canonicalColumnName) throws IllegalQueryColumnNameException {
        if(!CommonUtils.isCanonicalColumnName(canonicalColumnName)){
            throw new IllegalQueryColumnNameException();
        }
        this.canonicalColumnName = canonicalColumnName;
    }

    @Override
    public double[] calculate() {
        double[] columnValue = ColumnManager.getInstance().calculate(canonicalColumnName);
        return Arrays.copyOf(columnValue, columnValue.length);
    }

    @Override
    public boolean isDifferentTable(String tableName) {
        return !canonicalColumnName.contains(tableName);
    }

    @Override
    public String toString() {
        return canonicalColumnName;
    }
}
