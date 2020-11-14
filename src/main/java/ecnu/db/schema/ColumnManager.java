package ecnu.db.schema;

import ecnu.db.exception.TouchstoneException;
import ecnu.db.exception.schema.CannotFindColumnException;
import ecnu.db.schema.column.AbstractColumn;

import java.util.HashMap;

public class ColumnManager {
    private static final HashMap<String, AbstractColumn> columns = new HashMap<>();

    public static void addColumn(String columnName, AbstractColumn column) throws TouchstoneException {
        if (columnName.split("\\.").length != 3) {
            throw new TouchstoneException("非canonicalColumnName格式");
        }
        columns.put(columnName, column);
    }

    public static AbstractColumn getColumn(String columnName) throws CannotFindColumnException {
        AbstractColumn column = columns.get(columnName);
        if (column == null) {
            throw new CannotFindColumnException(columnName);
        }
        return column;
    }
}
