package ecnu.db.schema.column;


import java.text.ParseException;

/**
 * @author qingshuai.wang
 */
public abstract class AbstractColumn {
    private final ColumnType columnType;
    protected float nullPercentage;
    protected String columnName;

    public AbstractColumn(String columnName, ColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    /**
     * 获取该列非重复值的个数
     *
     * @return 非重复值的个数
     */
    public abstract int getNdv();

    public String getColumnName() {
        return columnName;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public float getNullPercentage() {
        return nullPercentage;
    }

    public void setNullPercentage(float nullPercentage) {
        this.nullPercentage = nullPercentage;
    }

    public String formatColumnType() {
        return columnName + ',' + columnType + ';';
    }

    /**
     * 该列的配置信息在输出时的格式
     *
     * @return 输出在配置文件中的格式
     * @throws ParseException 不能解析为相应的格式
     */
    public abstract String formatDataDistribution() throws ParseException;
}
