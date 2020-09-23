package ecnu.db.schema;

import com.google.common.collect.Lists;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.column.*;
import ecnu.db.utils.ColumnConvert;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author wangqingshuai
 * todo 实现非特异性的数据采集器
 */
public class SchemaGenerator {
    /**
     * format sql and return two sqls
     *
     * @param tableMetadata 表的DDL
     * @return 1.column info sqls 2. keys info sql, including primary key and foreign keys
     */
    protected List<String> getColumnSql(String tableMetadata) {
        tableMetadata = tableMetadata.toLowerCase();
        tableMetadata = tableMetadata.substring(tableMetadata.indexOf(System.lineSeparator()) + 1, tableMetadata.lastIndexOf(")"));
        tableMetadata = tableMetadata.replaceAll("`", "");
        List<String> sqls = Lists.newArrayList(tableMetadata.split(System.lineSeparator()));
        sqls = sqls.stream().map(String::trim)
                .filter((str -> !str.startsWith("primary key") && !str.startsWith("key")))
                .collect(Collectors.toList());

        return sqls;
    }

    public Schema generateSchema(String tableName, String sql) throws TouchstoneToolChainException {
        List<String> columnSqls = getColumnSql(sql);
        Map<String, AbstractColumn> columns = new HashMap<>(columnSqls.size());
        for (String columnSql : columnSqls) {
            String[] attributes = columnSql.trim().split(" ");
            String columnName = attributes[0];
            int indexOfBrackets = attributes[1].indexOf('(');
            String dataType = (indexOfBrackets > 0) ? attributes[1].substring(0, indexOfBrackets) : attributes[1];
            switch (ColumnConvert.getColumnType(dataType)) {
                case INTEGER:
                    columns.put(columnName, new IntColumn(columnName));
                    break;
                case BOOL:
                    columns.put(columnName, new BoolColumn(columnName));
                    break;
                case DECIMAL:
                    columns.put(columnName, new DecimalColumn(columnName));
                    break;
                case VARCHAR:
                    columns.put(columnName, new StringColumn(columnName));
                    break;
                case DATE:
                    columns.put(columnName, new DateColumn(columnName));
                case DATETIME:
                    DateTimeColumn column = new DateTimeColumn(columnName);
                    if (indexOfBrackets > 0) {
                        column.setPrecision(Integer.parseInt(attributes[1].substring(indexOfBrackets + 1, attributes[1].length() - 1)));
                    } else {
                        column.setPrecision(0);
                    }
                    columns.put(columnName, column);
                    break;
                default:
                    throw new TouchstoneToolChainException("没有实现的类型转换");
            }
        }
        return new Schema(tableName, columns);
    }

    /**
     * 获取col分布所需的查询SQL语句
     *
     * @param tableName 需要查询的表名
     * @param columns   需要查询的col
     * @return SQL
     * @throws TouchstoneToolChainException 获取失败
     */
    public String getColumnDistributionSql(String tableName, Collection<AbstractColumn> columns) throws TouchstoneToolChainException {
        StringBuilder sql = new StringBuilder();
        for (AbstractColumn column : columns) {
            switch (column.getColumnType()) {
                case DATE:
                case DATETIME:
                case DECIMAL:
                    sql.append(String.format("min(%s.%s),", tableName, column.getColumnName()));
                    sql.append(String.format("max(%s.%s),", tableName, column.getColumnName()));
                    break;
                case INTEGER:
                    sql.append(String.format("min(%s.%s),", tableName, column.getColumnName()));
                    sql.append(String.format("max(%s.%s),", tableName, column.getColumnName()));
                    sql.append(String.format("count(distinct %s.%s),", tableName, column.getColumnName()));
                    break;
                case VARCHAR:
                    sql.append(String.format("max(length(%s.%s)),", tableName, column.getColumnName()));
                    sql.append(String.format("min(length(%s.%s)),", tableName, column.getColumnName()));
                    sql.append(String.format("count(distinct %s.%s),", tableName, column.getColumnName()));
                    break;
                case BOOL:
                    break;
                default:
                    throw new TouchstoneToolChainException("未匹配到的类型");
            }
        }
        return sql.substring(0, sql.length() - 1);
    }

    /**
     * 提取col的range信息(最大值，最小值)
     *
     * @param columns   需要设置的col
     * @param sqlResult 有关的SQL结果(由AbstractDbConnector.getDataRange返回)
     * @throws TouchstoneToolChainException 设置失败
     */
    public void setDataRangeBySqlResult(Collection<AbstractColumn> columns, String[] sqlResult) throws TouchstoneToolChainException {
        int index = 0;
        for (AbstractColumn column : columns) {
            switch (column.getColumnType()) {
                case INTEGER:
                    ((IntColumn) column).setMin(Integer.parseInt(sqlResult[index++]));
                    ((IntColumn) column).setMax(Integer.parseInt(sqlResult[index++]));
                    ((IntColumn) column).setNdv(Integer.parseInt(sqlResult[index++]));
                    break;
                case VARCHAR:
                    ((StringColumn) column).setMaxLength(Integer.parseInt(sqlResult[index++]));
                    ((StringColumn) column).setMinLength(Integer.parseInt(sqlResult[index++]));
                    ((StringColumn) column).setNdv(Integer.parseInt(sqlResult[index++]));
                    break;
                case DECIMAL:
                    ((DecimalColumn) column).setMin(Double.parseDouble(sqlResult[index++]));
                    ((DecimalColumn) column).setMax(Double.parseDouble(sqlResult[index++]));
                    break;
                case DATETIME:
                    ((DateTimeColumn) column).setBegin(LocalDateTime.parse(sqlResult[index++], DateTimeColumn.FMT));
                    ((DateTimeColumn) column).setEnd(LocalDateTime.parse(sqlResult[index++], DateTimeColumn.FMT));
                    break;
                case DATE:
                    ((DateColumn) column).setBegin(LocalDate.parse(sqlResult[index++], DateColumn.FMT));
                    ((DateColumn) column).setEnd(LocalDate.parse(sqlResult[index++], DateColumn.FMT));
                    break;
                case BOOL:
                    break;
                default:
                    throw new TouchstoneToolChainException("未匹配到的类型");
            }
        }
    }

}
