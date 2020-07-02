package ecnu.db.schema.generation;

import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.dbconnector.DumpFileConnector;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.*;
import ecnu.db.utils.ConfigConvert;
import ecnu.db.utils.TouchstoneToolChainException;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * @param createTableSql create table sql
     * @return 1.column info sqls 2. keys info sql, including primary key and foreign keys
     */

    private static final Logger logger = LoggerFactory.getLogger(AbstractSchemaGenerator.class);

    abstract Pair<String[], String> getColumnSqlAndKeySql(String createTableSql);

    /**
     * @param columnSqls column info sqls
     * @return a collection of column names and column types, all the types must exist in the utils
     */
    abstract HashMap<String, String> getColumnInfo(String[] columnSqls);

    public Schema generateSchemaNoKeys(String tableName, String sql) throws TouchstoneToolChainException {
        Pair<String[], String> columnSqlAndKeySql = getColumnSqlAndKeySql(sql);
        HashMap<String, AbstractColumn> columns = getColumns(getColumnInfo(columnSqlAndKeySql.getLeft()));
        return new Schema(tableName, columns);
    }

    private HashMap<String, AbstractColumn> getColumns(HashMap<String, String> columnNameAndTypes) throws TouchstoneToolChainException {
        HashMap<String, AbstractColumn> columns = new HashMap<>();
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
                    logger.warn("没有实现的类型转换");
            }
        }
        return columns;
    }


    public abstract String getColumnDistributionSql(String tableName, Collection<AbstractColumn> columns) throws TouchstoneToolChainException;

    public abstract void setDataRangeBySqlResult(Collection<AbstractColumn> columns, String[] sqlResult) throws TouchstoneToolChainException;

    public abstract void setDataRangeUnique(Schema schema, AbstractDbConnector dbConnector) throws IOException, SQLException;
}
