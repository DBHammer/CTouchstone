package ecnu.db.dbconnector;

import ecnu.db.analyzer.statical.QueryReader;
import ecnu.db.analyzer.statical.QueryTableName;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.SchemaGenerator;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.SystemConfig;
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
public class DbConnector implements DatabaseConnectorInterface {

    private final static Logger logger = LoggerFactory.getLogger(DbConnector.class);

    private final HashMap<String, Integer> multiColNdvMap = new HashMap<>();

    public DatabaseMetaData databaseMetaData;

    // 数据库连接
    protected Statement stmt;

    public DbConnector(SystemConfig config, String dbType, String databaseConnectionConfig) throws TouchstoneToolChainException {
        String url;
        if (!config.isCrossMultiDatabase()) {
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
            throw new TouchstoneToolChainException(String.format("无法建立数据库连接,连接信息为: '%s'", url));
        }
    }


    public String getTableMetadata(String canonicalTableName) throws SQLException {
        ResultSet rs = stmt.executeQuery("show create table " + canonicalTableName);
        rs.next();
        return rs.getString(2).trim().toLowerCase();
    }

    public String[] getDataRange(String canonicalTableName, String columnInfo) throws SQLException {
        String sql = "select " + columnInfo + " from " + canonicalTableName;
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
    public int getMultiColNdv(String canonicalTableName, String columns) throws SQLException {
        ResultSet rs = stmt.executeQuery("select count(distinct " + columns + ") from " + canonicalTableName);
        rs.next();
        int result = rs.getInt(1);
        multiColNdvMap.put(String.format("%s.%s", canonicalTableName, columns), result);
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
     * @param dbType               数据库类型
     * @return 表名
     * @throws IOException                  从SQL文件中获取Query失败
     * @throws TouchstoneToolChainException 从Query中获取tableNames失败或不支持的数据库类型
     */
    public List<String> fetchTableNames(boolean isCrossMultiDatabase, String databaseName, List<File> files, String dbType)
            throws IOException, TouchstoneToolChainException {
        List<String> tableNames = new ArrayList<>();
        for (File sqlFile : files) {
            List<String> queries = QueryReader.getQueriesFromFile(sqlFile.getPath(), dbType);
            for (String query : queries) {
                Set<String> tableNameRefs = QueryTableName.getTableName(sqlFile.getAbsolutePath(), query, dbType, isCrossMultiDatabase);
                tableNames.addAll(tableNameRefs);
            }
        }
        tableNames = tableNames.stream().distinct().collect(Collectors.toList());
        if (!isCrossMultiDatabase) {
            tableNames = tableNames.stream().map((name) -> CommonUtils.addDatabaseNamePrefix(databaseName, name)).collect(Collectors.toList());
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
     */
    public Schema fetchSchema(SchemaGenerator dbSchemaGenerator, String canonicalTableName) throws TouchstoneToolChainException, SQLException {
        String tableMetadata = getTableMetadata(canonicalTableName);
        Schema schema = dbSchemaGenerator.generateSchema(canonicalTableName, tableMetadata);
        String distributionSql = dbSchemaGenerator.getColumnDistributionSql(schema.getTableName(), schema.getColumns().values());
        String[] dataRange = getDataRange(canonicalTableName, distributionSql);
        dbSchemaGenerator.setDataRangeBySqlResult(schema.getColumns().values(), dataRange);
        logger.info(String.format("获取'%s'表结构和表数据分布成功", canonicalTableName));
        return schema;
    }

    public int getTableSize(String canonicalTableName) throws SQLException {
        ResultSet rs = stmt.executeQuery(String.format("select count(*) as cnt from %s", canonicalTableName));
        if (rs.next()) {
            return rs.getInt("cnt");
        }
        throw new SQLException(String.format("table'%s'的size为0", canonicalTableName));
    }
}