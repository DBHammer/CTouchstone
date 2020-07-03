package ecnu.db.dbconnector;

import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneToolChainException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wangqingshuai
 * 数据库驱动连接器
 */
public abstract class AbstractDbConnector implements DatabaseConnectorInterface {

    private final static Logger logger = LoggerFactory.getLogger(AbstractDbConnector.class);

    private final HashMap<String, Integer> multiColNdvMap = new HashMap<>();

    public DatabaseMetaData databaseMetaData;
    /**
     * JDBC 驱动名及数据库 URL
     */
    protected Statement stmt;

    AbstractDbConnector(SystemConfig config) throws TouchstoneToolChainException {
        // 数据库的用户名与密码
        String user = config.getDatabaseUser();
        String pass = config.getDatabasePwd();
        try {
            stmt = DriverManager.getConnection(dbUrl(config), user, pass).createStatement();
            databaseMetaData = DriverManager.getConnection(dbUrl(config), user, pass).getMetaData();
        } catch (SQLException e) {
            throw new TouchstoneToolChainException("无法建立数据库连接,连接信息为\n" + dbUrl(config));
        }
    }

    /**
     * 获取数据库连接的URL
     * @param config 配置信息
     * @return 数据库连接的URL
     */
    abstract String dbUrl(SystemConfig config);

    /**
     * 获取在数据库中出现的表名
     *
     * @return 所有表名
     */
    abstract String abstractGetTableNames();

    /**
     * 获取数据库表DDL所需要使用的SQL
     * @param tableName 需要获取的表名
     * @return SQL
     */
    abstract String abstractGetCreateTableSql(String tableName);

    @Override
    public List<String> getTableNames() throws SQLException {
        ResultSet rs = stmt.executeQuery(abstractGetTableNames());
        ArrayList<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString(1).trim().toLowerCase());
        }
        return tables;
    }

    public String getTableDdl(String tableName) throws SQLException {
        ResultSet rs = stmt.executeQuery(abstractGetCreateTableSql(tableName));
        rs.next();
        return rs.getString(2).trim().toLowerCase();
    }

    public String[] getDataRange(String tableName, String columnInfo) throws SQLException {
        String sql = "select " + columnInfo + " from " + tableName;
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

    @Override
    public List<String[]> explainQuery(String queryCanonicalName, String sql, String[] sqlInfoColumns) throws SQLException {
        ResultSet rs = stmt.executeQuery("explain analyze " + sql);
        ArrayList<String[]> result = new ArrayList<>();
        while (rs.next()) {
            String[] infos = new String[sqlInfoColumns.length];
            for (int i = 0; i < sqlInfoColumns.length; i++) {
                infos[i] = rs.getString(sqlInfoColumns[i]);
            }
            result.add(infos);
        }
        return result;
    }

    @Override
    public int getMultiColNdv(String schema, String columns) throws SQLException {
        ResultSet rs = stmt.executeQuery("select count(distinct " + columns + ") from " + schema);
        rs.next();
        int result = rs.getInt(1);
        multiColNdvMap.put(String.format("%s.%s", schema, columns), result);
        return result;
    }

    @Override
    public Map<String, Integer> getMultiColNdvMap() {
        return this.multiColNdvMap;
    }


}