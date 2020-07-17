package ecnu.db.schema.generation;

import com.alibaba.fastjson.JSON;
import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.dbconnector.TidbConnector;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.*;
import ecnu.db.utils.TidbStatsJsonObject;
import ecnu.db.utils.TouchstoneToolChainException;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

/**
 * @author wangqingshuai
 */
public class TidbSchemaGenerator extends AbstractSchemaGenerator {


    @Override
    Pair<String[], String> getColumnSqlAndKeySql(String tableDDL) {
        tableDDL = tableDDL.toLowerCase();
        tableDDL = tableDDL.substring(tableDDL.indexOf(System.lineSeparator()) + 1, tableDDL.lastIndexOf(")"));
        tableDDL = tableDDL.replaceAll("`", "");
        String[] sqls = tableDDL.split(System.lineSeparator());
        int index = sqls.length - 1;
        for (; index >= 0; index--) {
            if (!sqls[index].contains("key ")) {
                break;
            }
        }
        String keysInfo = null;
        return new MutablePair<>(Arrays.copyOfRange(sqls, 0, index + 1), keysInfo);
    }

    @Override
    HashMap<String, String> getColumnInfo(String[] columnSqls) {
        HashMap<String, String> columnInfos = new HashMap<>(columnSqls.length);
        for (String columnSql : columnSqls) {
            String[] attributes = columnSql.trim().split(" ");
            String columnName = attributes[0];
            int indexOfBrackets = attributes[1].indexOf('(');
            columnInfos.put(columnName, (indexOfBrackets > 0) ?
                    attributes[1].substring(0, indexOfBrackets) : attributes[1]);
        }
        return columnInfos;
    }

    @Override
    public String getColumnDistributionSql(String tableName, Collection<AbstractColumn> columns) throws TouchstoneToolChainException {
        StringBuilder sql = new StringBuilder();
        for (AbstractColumn column : columns) {
            switch (column.getColumnType()) {
                case DATETIME:
                case DECIMAL:
                case INTEGER:
                    sql.append("min(").append(tableName).append(".").append(column.getColumnName())
                            .append("),max(").append(tableName).append(".").append(column.getColumnName()).append("),");
                    break;
                case VARCHAR:
                    sql.append("max(length(").append(tableName).append(".").append(column.getColumnName()).append(")),");
                    break;
                case BOOL:
                    break;
                default:
                    throw new TouchstoneToolChainException("未匹配到的类型");
            }
        }
        return sql.substring(0, sql.length() - 1);
    }

    @Override
    public void setDataRangeBySqlResult(Collection<AbstractColumn> columns, String[] sqlResult) throws TouchstoneToolChainException {
        int index = 0;
        for (AbstractColumn column : columns) {
            switch (column.getColumnType()) {
                case INTEGER:
                    ((IntColumn) column).setMin(Integer.parseInt(sqlResult[index++]));
                    ((IntColumn) column).setMax(Integer.parseInt(sqlResult[index++]));
                    break;
                case VARCHAR:
                    ((StringColumn) column).setMaxLength(Integer.parseInt(sqlResult[index++]));
                    break;
                case DECIMAL:
                    ((DecimalColumn) column).setMin(Double.parseDouble(sqlResult[index++]));
                    ((DecimalColumn) column).setMax(Double.parseDouble(sqlResult[index++]));
                    break;
                case DATETIME:
                    ((DateColumn) column).setBegin(sqlResult[index++]);
                    ((DateColumn) column).setEnd(sqlResult[index++]);
                    break;
                case BOOL:
                    break;
                default:
                    throw new TouchstoneToolChainException("未匹配到的类型");
            }
        }
    }

    @Override
    public void setDataRangeUnique(Schema schema, AbstractDbConnector dbConnector) throws IOException {
        TidbStatsJsonObject tidbStatsJsonObject = JSON.parseObject(((TidbConnector) dbConnector).
                tableInfoJson(schema.getTableName()).replace(" ", ""), TidbStatsJsonObject.class);
        schema.setTableSize(tidbStatsJsonObject.getCount());
        for (AbstractColumn column : schema.getColumns().values()) {
            column.setNullPercentage(tidbStatsJsonObject.getNullProbability(column.getColumnName()));
            if (column.getColumnType() == ColumnType.INTEGER) {
                ((IntColumn) column).setNdv(tidbStatsJsonObject.getNdv(column.getColumnName()));
            } else if (column.getColumnType() == ColumnType.VARCHAR) {
                ((StringColumn) column).setNdv(tidbStatsJsonObject.getNdv(column.getColumnName()));
                ((StringColumn) column).setAvgLength(tidbStatsJsonObject.getAvgLength(column.getColumnName()));
            }
        }
    }
}
