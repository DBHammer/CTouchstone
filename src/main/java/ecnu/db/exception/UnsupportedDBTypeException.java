package ecnu.db.exception;

import ecnu.db.utils.TouchstoneSupportedDatabaseVersion;

/**
 * @author alan
 */
public class UnsupportedDBTypeException extends TouchstoneToolChainException {

    public UnsupportedDBTypeException(TouchstoneSupportedDatabaseVersion dbType) {
        super(String.format("暂时不支持的数据库连接类型%s", dbType));
    }
}
