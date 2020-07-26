package ecnu.db.dbconnector;

import ecnu.db.analyzer.statical.QueryTableName;
import ecnu.db.schema.Schema;
import ecnu.db.schema.generation.AbstractSchemaGenerator;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.ReadQuery;
import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneToolChainException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

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
            throw new TouchstoneToolChainException(String.format("无法建立数据库连接,连接信息为: '%s'", dbUrl(config)));
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

    /**
     * @param isCrossMultiDatabase 是否跨多个数据库
     * @param databaseName         数据库名称，若isCrossMultiDatabase为false，则可以填写null
     * @param files                SQL文件
     * @return 表名
     * @throws IOException                  从SQL文件中获取Query失败
     * @throws TouchstoneToolChainException 从Query中获取tableNames失败
     * @throws SQLException                 从数据库中获取tableNames失败
     */
    public List<String> fetchTableNames(boolean isCrossMultiDatabase, String databaseName, List<File> files) throws IOException, TouchstoneToolChainException, SQLException {
        List<String> tableNames;
        if (isCrossMultiDatabase) {
            tableNames = new ArrayList<>();
            for (File sqlFile : files) {
                List<String> queries = ReadQuery.getQueriesFromFile(sqlFile.getPath(), "mysql");
                for (String query : queries) {
                    Set<String> tableNameRefs = QueryTableName.getTableName(sqlFile.getAbsolutePath(), query, "mysql", true);
                    tableNames.addAll(tableNameRefs);
                }
            }
            tableNames = tableNames.stream().distinct().collect(Collectors.toList());
        } else {
            tableNames = getTableNames().stream().map((name) -> CommonUtils.addDBNamePrefix(databaseName, name)).collect(Collectors.toList());
        }
        return tableNames;
    }

    /**
     * 从数据库中提取Schema
     *
     * @param dbSchemaGenerator  Schema生成器
     * @param canonicalTableName 标准表名
     * @return Schema
     * @throws TouchstoneToolChainException 生成Schema失败，设置col分布失败或者设置col的cardinality和average length等信息失败
     * @throws SQLException                 获取表的DDL失败或者获取col分布失败
     * @throws IOException                  获取col的cardinality和average length等信息失败
     */
    public Schema fetchSchema(AbstractSchemaGenerator dbSchemaGenerator, String canonicalTableName) throws TouchstoneToolChainException, SQLException, IOException {
        Schema schema = dbSchemaGenerator.generateSchemaNoKeys(canonicalTableName, getTableDdl(canonicalTableName));
        dbSchemaGenerator.setDataRangeBySqlResult(schema.getColumns().values(), getDataRange(canonicalTableName,
                dbSchemaGenerator.getColumnDistributionSql(schema.getTableName(), schema.getColumns().values())));
        dbSchemaGenerator.setDataRangeUnique(schema, this);
        logger.info(String.format("获取'%s'表结构和表数据分布成功", canonicalTableName));
        return schema;
    }
}