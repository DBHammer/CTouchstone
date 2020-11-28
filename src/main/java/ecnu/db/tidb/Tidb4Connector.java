package ecnu.db.tidb;

import com.alibaba.druid.DbType;
import ecnu.db.dbconnector.DbConnector;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.utils.config.DatabaseConnectorConfig;

import java.util.Arrays;
import java.util.HashSet;


/**
 * @author wangqingshuai
 */
public class Tidb4Connector extends DbConnector {
    private final static String DB_DRIVER_TYPE = "mysql";
    private final static String JDBC_PROPERTY = "useSSL=false&allowPublicKeyRetrieval=true";


    public Tidb4Connector(DatabaseConnectorConfig config) throws TouchstoneException {
        super(config, DB_DRIVER_TYPE, JDBC_PROPERTY);
    }

    @Override
    protected String[] getSqlInfoColumns() {
        return new String[]{"id", "operator info", "actRows", "access object"};
    }

    /**
     * 获取节点上查询计划的信息
     *
     * @param data 需要处理的数据
     * @return 返回plan_id, operator_info, execution_info
     */
    @Override
    protected String[] formatQueryPlan(String[] data) {
        String[] ret = new String[3];
        ret[0] = data[0];
        ret[1] = data[3].isEmpty() ? data[1] : String.format("%s,%s", data[3], data[1]);
        ret[2] = "rows:" + data[2];
        return ret;
    }

    @Override
    public DbType getDbType() {
        return DbType.mysql;
    }
}
