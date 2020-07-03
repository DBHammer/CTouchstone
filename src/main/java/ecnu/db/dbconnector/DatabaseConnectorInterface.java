package ecnu.db.dbconnector;

import ecnu.db.utils.TouchstoneToolChainException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author lianxuechao
 */
public interface DatabaseConnectorInterface {
    /**
     * 获取数据库的全部表名
     * @return 全部表名
     * @throws SQLException
     */
    List<String> getTableNames() throws SQLException;

    /**
     * explain analyze一个query
     * @param queryCanonicalName 对应query的标准名称
     * @param sql 对应query的sql
     * @param sqlInfoColumns 需要提取的col
     * @return 查询计划
     * @throws SQLException
     * @throws TouchstoneToolChainException
     */
    List<String[]> explainQuery(String queryCanonicalName, String sql, String[] sqlInfoColumns) throws SQLException, TouchstoneToolChainException;

    /**
     * 获取多个col组合的cardinality, 每次查询会被记录到multiColNdvMap
     * @param schema 需要查询的Schema
     * @param columns 需要查询的col组合(','组合)
     * @return 多个col组合的cardinality
     * @throws SQLException
     * @throws TouchstoneToolChainException
     */
    int getMultiColNdv(String schema, String columns) throws SQLException, TouchstoneToolChainException;

    /**
     * 获取多个col组合的cardinality的map，用于后续的dump操作
     * @return 多个col组合的cardinality的map
     */
    Map<String, Integer> getMultiColNdvMap();
}
