package ecnu.db.exception.schema;

import ecnu.db.exception.TouchstoneException;

/**
 * @author alan
 */
public class CannotFindSchemaException extends TouchstoneException {
    public CannotFindSchemaException(String tableName) {
        super(String.format("找不到表名'%s'对应的Schema", tableName));
    }
}
