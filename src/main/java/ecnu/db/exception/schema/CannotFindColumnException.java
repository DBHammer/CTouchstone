package ecnu.db.exception.schema;

import ecnu.db.exception.TouchstoneException;

/**
 * @author alan
 */
public class CannotFindColumnException extends TouchstoneException {
    public CannotFindColumnException(String tableName, String columnName) {
        super(String.format("表'%s'找不到'%s'对应的Column", tableName, columnName));
    }
}
