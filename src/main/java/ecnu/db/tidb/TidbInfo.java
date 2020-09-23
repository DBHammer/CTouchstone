package ecnu.db.tidb;

import com.alibaba.druid.util.JdbcConstants;
import ecnu.db.exception.UnsupportedDBTypeException;
import ecnu.db.utils.AbstractDatabaseInfo;
import ecnu.db.utils.TouchstoneSupportedDatabaseVersion;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static ecnu.db.utils.TouchstoneSupportedDatabaseVersion.TiDB3;
import static ecnu.db.utils.TouchstoneSupportedDatabaseVersion.TiDB4;

/**
 * @author wangqingshuai
 */
public class TidbInfo extends AbstractDatabaseInfo {
    public TidbInfo(TouchstoneSupportedDatabaseVersion touchstoneSupportedDatabaseVersion) {
        super(touchstoneSupportedDatabaseVersion);
    }

    @Override
    public String[] getSqlInfoColumns() throws UnsupportedDBTypeException {
        switch (touchstoneSupportedDatabaseVersion) {
            case TiDB3:
                return new String[]{"id", "operator info", "execution info"};
            case TiDB4:
                return new String[]{"id", "operator info", "actRows", "access object"};
            default:
                throw new UnsupportedDBTypeException(touchstoneSupportedDatabaseVersion);
        }
    }

    @Override
    public String getStaticalDbVersion() {
        return JdbcConstants.MYSQL;
    }

    @Override
    public Set<TouchstoneSupportedDatabaseVersion> getSupportedDatabaseVersions() {
        return new HashSet<>(Arrays.asList(TiDB3, TiDB4));
    }

    @Override
    public String getJdbcType() {
        return "mysql";
    }

    @Override
    public String getJdbcProperties() {
        return "useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    }
}
