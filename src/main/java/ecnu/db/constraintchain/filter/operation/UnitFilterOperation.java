package ecnu.db.constraintchain.filter.operation;

/**
 * @author wangqingshuai
 */
public class UnitFilterOperation extends AbstractFilterOperation {
    private final String columnName;

    public UnitFilterOperation(int id, String columnName, CompareOperator operator) {
        super(id, operator);
        this.columnName = columnName;
    }

    @Override
    public String toString() {
        return "UnitFilterOperation{" +
                "id=" + id +
                ", columnName='" + columnName +
                ", probability=" + probability +
                ", operator=" + operator +
                '}';
    }

    /**
     * todo 参数实例化
     */
    @Override
    public void calculateParameter() {

    }
}
