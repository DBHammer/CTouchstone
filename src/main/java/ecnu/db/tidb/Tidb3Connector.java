package ecnu.db.tidb;

import com.alibaba.druid.DbType;
import ecnu.db.dbconnector.DbConnector;
import ecnu.db.utils.config.DatabaseConnectorConfig;
import ecnu.db.utils.exception.TouchstoneException;

public class Tidb3Connector extends DbConnector {
    private final static String DB_DRIVER_TYPE = "mysql";
    private final static String JDBC_PROPERTY = "useSSL=false&allowPublicKeyRetrieval=true";

    public Tidb3Connector(DatabaseConnectorConfig config) throws TouchstoneException {
        super(config, DB_DRIVER_TYPE, JDBC_PROPERTY);
    }

    @Override
    public String[] getSqlInfoColumns() {
        return new String[]{"id", "operator info", "execution info"};
    }

    @Override
    public DbType getDbType() {
        return DbType.mysql;
    }

    /**
     * 获取节点上查询计划的信息
     *
     * @param data 需要处理的数据
     * @return 返回plan_id, operator_info, execution_info
     */
    @Override
    protected String[] formatQueryPlan(String[] data) {
        return data;
    }
}
