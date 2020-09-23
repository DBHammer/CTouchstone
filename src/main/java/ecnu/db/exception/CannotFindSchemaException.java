package ecnu.db.exception;

/**
 * @author alan
 */
public class CannotFindSchemaException extends TouchstoneToolChainException {
    public CannotFindSchemaException(String tableName) {
        super(String.format("找不到表名'%s'对应的Schema", tableName));
    }
}
