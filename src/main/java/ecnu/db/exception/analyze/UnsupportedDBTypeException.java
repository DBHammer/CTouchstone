package ecnu.db.exception.analyze;

import ecnu.db.exception.TouchstoneException;
import ecnu.db.utils.TouchstoneSupportedDatabaseVersion;

/**
 * @author alan
 */
public class UnsupportedDBTypeException extends TouchstoneException {

    public UnsupportedDBTypeException(TouchstoneSupportedDatabaseVersion dbType) {
        super(String.format("暂时不支持的数据库连接类型%s", dbType));
    }
}
