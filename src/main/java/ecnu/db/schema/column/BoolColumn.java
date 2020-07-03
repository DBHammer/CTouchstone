package ecnu.db.schema.column;

import java.math.BigDecimal;

/**
 * @author qingshuai.wang
 */
public class BoolColumn extends AbstractColumn {
    /**
     * TODO add Bool Column
     */
    private BigDecimal trueProbability;

    public BoolColumn(String columnName) {
        super(columnName, ColumnType.BOOL);
    }

    @Override
    public int getNdv() {
        return -1;
    }

    @Override
    public String formatDataDistribution() {
        return columnName + ";" + nullPercentage + "," + trueProbability;
    }
}
