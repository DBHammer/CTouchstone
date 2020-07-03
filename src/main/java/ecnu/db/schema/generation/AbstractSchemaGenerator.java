package ecnu.db.schema.generation;

import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.*;
import ecnu.db.utils.ConfigConvert;
import ecnu.db.utils.TouchstoneToolChainException;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangqingshuai
 */
public abstract class AbstractSchemaGenerator {
    /**
     * format sql and return two sqls
     *
     * @param tableDDL 表的DDL
     * @return 1.column info sqls 2. keys info sql, including primary key and foreign keys
     */


    abstract Pair<String[], String> getColumnSqlAndKeySql(String tableDDL);

    /**
     * 获取table DDL结果中的col的名称到类型的map
     * @param columnSqls 需要的col
     * @return col的名称到类型的map
     */
    abstract HashMap<String, String> getColumnInfo(String[] columnSqls);

    public Schema generateSchemaNoKeys(String tableName, String sql) throws TouchstoneToolChainException {
        Pair<String[], String> columnSqlAndKeySql = getColumnSqlAndKeySql(sql);
        HashMap<String, AbstractColumn> columns = getColumns(getColumnInfo(columnSqlAndKeySql.getLeft()));
        return new Schema(tableName, columns);
    }

    private HashMap<String, AbstractColumn> getColumns(HashMap<String, String> columnNameAndTypes) throws TouchstoneToolChainException {
        HashMap<String, AbstractColumn> columns = new HashMap<>(columnNameAndTypes.size());
        for (Map.Entry<String, String> columnNameAndType : columnNameAndTypes.entrySet()) {
            switch (ConfigConvert.getColumnType(columnNameAndType.getValue())) {
                case INTEGER:
                    columns.put(columnNameAndType.getKey(), new IntColumn(columnNameAndType.getKey()));
                    break;
                case BOOL:
                    columns.put(columnNameAndType.getKey(), new BoolColumn(columnNameAndType.getKey()));
                    break;
                case DECIMAL:
                    columns.put(columnNameAndType.getKey(), new DecimalColumn(columnNameAndType.getKey()));
                    break;
                case VARCHAR:
                    columns.put(columnNameAndType.getKey(), new StringColumn(columnNameAndType.getKey()));
                    break;
                case DATETIME:
                    columns.put(columnNameAndType.getKey(), new DateColumn(columnNameAndType.getKey()));
                    break;
                default:
                    throw new TouchstoneToolChainException("没有实现的类型转换");
            }
        }
        return columns;
    }

    /**
     * 获取col分布所需的查询SQL语句
     * @param tableName 需要查询的表名
     * @param columns 需要查询的col
     * @return SQL
     * @throws TouchstoneToolChainException 获取失败
     */
    public abstract String getColumnDistributionSql(String tableName, Collection<AbstractColumn> columns) throws TouchstoneToolChainException;

    /**
     * 提取col的range信息(最大值，最小值)
     * @param columns 需要设置的col
     * @param sqlResult 有关的SQL结果(由AbstractDbConnector.getDataRange返回)
     * @throws TouchstoneToolChainException 设置失败
     */
    public abstract void setDataRangeBySqlResult(Collection<AbstractColumn> columns, String[] sqlResult) throws TouchstoneToolChainException;

    /**
     * 从数据库的统计信息里提取schme里的col的cardinality和average length等信息
     * @param schema 需要设置的schema
     * @param dbConnector 数据库连接
     * @throws IOException
     * @throws SQLException
     */
    public abstract void setDataRangeUnique(Schema schema, AbstractDbConnector dbConnector) throws IOException, SQLException;
}
