package ecnu.db.pg;

import com.alibaba.druid.DbType;
import com.alibaba.druid.util.JdbcConstants;
import ecnu.db.exception.analyze.UnsupportedDBTypeException;
import ecnu.db.utils.AbstractDatabaseInfo;
import ecnu.db.utils.TouchstoneSupportedDatabaseVersion;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static ecnu.db.utils.TouchstoneSupportedDatabaseVersion.*;

public class PgInfo extends AbstractDatabaseInfo {
    public PgInfo(TouchstoneSupportedDatabaseVersion touchstoneSupportedDatabaseVersion) {
        super(touchstoneSupportedDatabaseVersion);
    }

    @Override
    public String[] getSqlInfoColumns() throws UnsupportedDBTypeException {
        String[] col = {"1"};
        return col;
    }

    @Override
    public DbType getStaticalDbVersion() {
        return JdbcConstants.MYSQL;
    }

    @Override
    public Set<TouchstoneSupportedDatabaseVersion> getSupportedDatabaseVersions() {
        return new HashSet<>(Arrays.asList(TiDB3, TiDB4, PG));
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
