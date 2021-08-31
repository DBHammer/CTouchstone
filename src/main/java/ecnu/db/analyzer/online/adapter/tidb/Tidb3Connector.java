package ecnu.db.analyzer.online.adapter.tidb;

import ecnu.db.dbconnector.DbConnector;
import ecnu.db.utils.DatabaseConnectorConfig;
import ecnu.db.utils.exception.TouchstoneException;

public class Tidb3Connector extends DbConnector {
    private static final String DB_DRIVER_TYPE = "mysql";
    private static final String JDBC_PROPERTY = "useSSL=false&allowPublicKeyRetrieval=true";

    public Tidb3Connector(DatabaseConnectorConfig config) throws TouchstoneException {
        super(config, DB_DRIVER_TYPE, JDBC_PROPERTY);
    }

    @Override
    public int[] getSqlInfoColumns() {
        return new int[]{1, 4, 5};
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
