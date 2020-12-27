package ecnu.db.dbconnector;

import com.google.common.collect.Lists;
import ecnu.db.schema.ColumnManager;
import ecnu.db.utils.DatabaseConnectorConfig;
import ecnu.db.utils.exception.TouchstoneException;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author wangqingshuai
 * 数据库驱动连接器
 */
public abstract class DbConnector {
    private final HashMap<String, Integer> multiColNdvMap = new HashMap<>();
    private final String[] sqlInfoColumns;
    public DatabaseMetaData databaseMetaData;
    // 数据库连接
    protected Statement stmt;

    public DbConnector(DatabaseConnectorConfig config, String dbType, String databaseConnectionConfig) throws TouchstoneException {
        String url;
        if (config.getDatabaseName() != null) {
            url = String.format("jdbc:%s://%s:%s/%s?%s", dbType, config.getDatabaseIp(), config.getDatabasePort(), config.getDatabaseName(), databaseConnectionConfig);
        } else {
            url = String.format("jdbc:%s://%s:%s?%s", dbType, config.getDatabaseIp(), config.getDatabasePort(), databaseConnectionConfig);
        }
        // 数据库的用户名与密码
        String user = config.getDatabaseUser();
        String pass = config.getDatabasePwd();
        try {
            stmt = DriverManager.getConnection(url, user, pass).createStatement();
            databaseMetaData = DriverManager.getConnection(url, user, pass).getMetaData();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new TouchstoneException(String.format("无法建立数据库连接,连接信息为: '%s'", url));
        }
        sqlInfoColumns = getSqlInfoColumns();
    }

    /**
     * @return 获取查询计划的列名
     */
    protected abstract String[] getSqlInfoColumns();

    protected abstract String[] formatQueryPlan(String[] queryPlan);

    public List<String> getColumnMetadata(String canonicalTableName) throws SQLException {
        ResultSet rs = stmt.executeQuery("show create table " + canonicalTableName);
        rs.next();
        String tableMetadata = rs.getString(2).trim().toLowerCase();
        tableMetadata = tableMetadata.toLowerCase();
        tableMetadata = tableMetadata.substring(tableMetadata.indexOf(System.lineSeparator()) + 1, tableMetadata.lastIndexOf(")"));
        tableMetadata = tableMetadata.replaceAll("`", "");
        List<String> sqls = Lists.newArrayList(tableMetadata.split(System.lineSeparator()));
        return sqls.stream().map(String::trim).filter((str -> !str.startsWith("primary key") && !str.startsWith("key")))
                .collect(Collectors.toList());
    }

    public String getPrimaryKeys(String canonicalTableName) throws SQLException {
        ResultSet rs = stmt.executeQuery("show keys from " + canonicalTableName + " where Key_name='PRIMARY'");
        List<String> keys = new ArrayList<>();
        while (rs.next()) {
            keys.add(canonicalTableName + "." + rs.getString(5).toLowerCase());
        }
        return keys.size() > 0 ? String.join(",", keys) : null;
    }

    public String[] getDataRange(String canonicalTableName, List<String> canonicalColumnNames) throws SQLException, TouchstoneException {
        String sql = "select " + getColumnDistributionSql(canonicalColumnNames) + " from " + canonicalTableName;
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        String[] infos = new String[rs.getMetaData().getColumnCount()];
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            try {
                infos[i - 1] = rs.getString(i).trim().toLowerCase();
            } catch (NullPointerException e) {
                infos[i - 1] = "0";
            }
        }
        return infos;
    }

    public List<String[]> explainQuery(String sql) throws SQLException {
        ResultSet rs = stmt.executeQuery("explain analyze " + sql);
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
        ResultSet rs = stmt.executeQuery("select count(distinct " + columns + ") from " + canonicalTableName);
        rs.next();
        int result = rs.getInt(1);
        multiColNdvMap.put(String.format("%s.%s", canonicalTableName, columns), result);
        return result;
    }

    public Map<String, Integer> getMultiColNdvMap() {
        return this.multiColNdvMap;
    }


    public int getTableSize(String canonicalTableName) throws SQLException {
        ResultSet rs = stmt.executeQuery(String.format("select count(*) as cnt from %s", canonicalTableName));
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
                case DATE:
                case DATETIME:
                case DECIMAL:
                    sql.append(String.format("min(%s),", canonicalColumnName));
                    sql.append(String.format("max(%s),", canonicalColumnName));
                    break;
                case INTEGER:
                    sql.append(String.format("min(%s),", canonicalColumnName));
                    sql.append(String.format("max(%s),", canonicalColumnName));
                    sql.append(String.format("count(distinct %s),", canonicalColumnName));
                    break;
                case VARCHAR:
                    sql.append(String.format("min(length(%s)),", canonicalColumnName));
                    sql.append(String.format("max(length(%s)),", canonicalColumnName));
                    sql.append(String.format("count(distinct %s),", canonicalColumnName));
                    break;
                case BOOL:
                    sql.append(String.format("avg(%s)", canonicalColumnName));
                    break;
                default:
                    throw new TouchstoneException("未匹配到的类型");
            }
            sql.append(String.format("avg(case when %s IS NULL then 1 else 0 end),", canonicalColumnName));
        }
        return sql.substring(0, sql.length() - 1);
    }
}