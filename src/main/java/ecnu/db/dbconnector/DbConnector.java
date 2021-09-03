package ecnu.db.dbconnector;

import ecnu.db.schema.Column;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.ColumnType;
import ecnu.db.utils.DatabaseConnectorConfig;
import ecnu.db.utils.exception.TouchstoneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wangqingshuai 数据库驱动连接器
 */
public abstract class DbConnector {
    private final Logger logger = LoggerFactory.getLogger(DbConnector.class);
    private final HashMap<String, Integer> multiColNdvMap = new HashMap<>();
    private final int[] sqlInfoColumns;
    private final DatabaseMetaData databaseMetaData;
    // 数据库连接
    protected Statement stmt;

    protected DbConnector(DatabaseConnectorConfig config, String dbType, String databaseConnectionConfig)
            throws TouchstoneException {
        String url;
        if (config.getDatabaseName() != null) {
            url = String.format("jdbc:%s://%s:%s/%s?%s", dbType, config.getDatabaseIp(), config.getDatabasePort(),
                    config.getDatabaseName(), databaseConnectionConfig);
        } else {
            url = String.format("jdbc:%s://%s:%s?%s", dbType, config.getDatabaseIp(), config.getDatabasePort(),
                    databaseConnectionConfig);
        }
        // 数据库的用户名与密码
        String user = config.getDatabaseUser();
        String pass = config.getDatabasePwd();
        try {
            Connection conn = DriverManager.getConnection(url, user, pass);
            stmt = conn.createStatement();
            for (String command : preExecutionCommands()) {
                stmt.execute(command);
            }
            databaseMetaData = conn.getMetaData();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new TouchstoneException(String.format("无法建立数据库连接,连接信息为: '%s'", url));
        }
        sqlInfoColumns = getSqlInfoColumns();
    }

    /**
     * @return 获取查询计划的列索引
     */
    protected abstract int[] getSqlInfoColumns();

    protected abstract String getExplainFormat();

    protected abstract String[] formatQueryPlan(String[] queryPlan);

    protected abstract String[] preExecutionCommands();

    public List<String> getColumnMetadata(String canonicalTableName) throws SQLException, TouchstoneException {
        String[] schemaAndTable = canonicalTableName.split("\\.");
        List<String> columnNames = new ArrayList<>();
        ResultSet rs = databaseMetaData.getColumns(null, schemaAndTable[0], schemaAndTable[1], null);
        while (rs.next()) {
            String canonicalColumnName = canonicalTableName + "." + rs.getString("COLUMN_NAME");
            columnNames.add(canonicalColumnName);
            ColumnManager.getInstance().addColumn(canonicalColumnName,
                    new Column(ColumnType.getColumnType(rs.getInt("DATA_TYPE"))));
        }
        return columnNames;
    }

    public List<String> getPrimaryKeyList(String canonicalTableName) throws SQLException {
        String[] schemaAndTable = canonicalTableName.split("\\.");
        ResultSet rs = databaseMetaData.getPrimaryKeys(schemaAndTable[0], null, schemaAndTable[1]);
        List<String> keys = new ArrayList<>();
        while (rs.next()) {
            keys.add(canonicalTableName + "." + rs.getString("COLUMN_NAME").toLowerCase());
        }
        return keys;
    }

    public String[] getDataRange(String canonicalTableName, List<String> canonicalColumnNames)
            throws SQLException, TouchstoneException {
        ResultSet rs = stmt.executeQuery(String.format("select %s from %s", getColumnDistributionSql(canonicalColumnNames), canonicalTableName));
        rs.next();
        String[] infos = new String[rs.getMetaData().getColumnCount()];
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            try {
                infos[i - 1] = rs.getString(i).trim().toLowerCase();
            } catch (NullPointerException e) {
                logger.error("所查列数据为空");
                infos[i - 1] = "0";
            }
        }
        return infos;
    }

    public List<String[]> explainQuery(String sql) throws SQLException {
        ResultSet rs = stmt.executeQuery(String.format(getExplainFormat(), sql));
        ArrayList<String[]> result = new ArrayList<>();
        while (rs.next()) {
            String[] infos = new String[sqlInfoColumns.length];
            for (int i = 0; i < sqlInfoColumns.length; i++) {
                infos[i] = rs.getString(sqlInfoColumns[i]);
            }
            result.add(formatQueryPlan(infos));
        }
        return result;
    }

    public int getMultiColNdv(String canonicalTableName, String columns) throws SQLException {
        ResultSet rs = stmt.executeQuery(String.format("select count(distinct %s) as cnt from %s", columns, canonicalTableName));
        rs.next();
        int result = rs.getInt("cnt");
        multiColNdvMap.put(String.format("%s.%s", canonicalTableName, columns), result);
        return result;
    }

    public Map<String, Integer> getMultiColNdvMap() {
        return this.multiColNdvMap;
    }

    public int getTableSize(String canonicalTableName) throws SQLException {
        String countQuery = String.format("select count(*) as cnt from %s", canonicalTableName);
        ResultSet rs = stmt.executeQuery(countQuery);
        if (rs.next()) {
            return rs.getInt("cnt");
        }
        throw new SQLException(String.format("table'%s'的size为0", canonicalTableName));
    }

    /**
     * 获取col分布所需的查询SQL语句
     *
     * @param canonicalColumnNames 需要查询的col
     * @return SQL
     * @throws TouchstoneException 获取失败
     */
    public String getColumnDistributionSql(List<String> canonicalColumnNames) throws TouchstoneException {
        StringBuilder sql = new StringBuilder();
        for (String canonicalColumnName : canonicalColumnNames) {
            switch (ColumnManager.getInstance().getColumnType(canonicalColumnName)) {
                case DATE, DATETIME, DECIMAL -> sql.append(String.format("min(%1$s), max(%1$s), ", canonicalColumnName));
                case INTEGER -> sql.append(String.format("min(%1$s), max(%1$s), count(distinct %1$s),", canonicalColumnName));
                case VARCHAR -> sql.append(String.format("min(length(%1$s)), max(length(%1$s)), count(distinct %1$s),", canonicalColumnName));
                case BOOL -> sql.append(String.format("avg(%s)", canonicalColumnName));
                default -> throw new TouchstoneException("未匹配到的类型");
            }
            sql.append(String.format("avg(case when %s IS NULL then 1 else 0 end),", canonicalColumnName));
        }
        return sql.substring(0, sql.length() - 1);
    }
}