package ecnu.db.analyzer.online.adapter.pg;

import ecnu.db.dbconnector.DbConnector;
import ecnu.db.utils.DatabaseConnectorConfig;
import ecnu.db.utils.exception.TouchstoneException;

public class PgConnector extends DbConnector {

    private static final String DB_DRIVER_TYPE = "postgre";
    private static final String JDBC_PROPERTY = "";

    public PgConnector(DatabaseConnectorConfig config) throws TouchstoneException {
        super(config, DB_DRIVER_TYPE, JDBC_PROPERTY);
    }

    @Override
    protected int[] getSqlInfoColumns() {
        return new int[]{1};
    }

    @Override
    protected String[] formatQueryPlan(String[] queryPlan) {
        return queryPlan;
    }
}
