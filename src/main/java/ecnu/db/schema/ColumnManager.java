package ecnu.db.schema;

import ecnu.db.exception.TouchstoneException;
import ecnu.db.exception.schema.CannotFindColumnException;
import ecnu.db.schema.column.*;
import ecnu.db.utils.CommonUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ColumnManager {
    private static final LinkedHashMap<String, AbstractColumn> columns = new LinkedHashMap<>();

    public static void addColumn(String columnName, AbstractColumn column) throws TouchstoneException {
        if (columnName.split("\\.").length != 3) {
            throw new TouchstoneException("非canonicalColumnName格式");
        }
        columns.put(columnName, column);
    }

    public static void storeColumnResult() throws IOException {
        String content = CommonUtils.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(columns);
        FileUtils.writeStringToFile(new File(CommonUtils.getResultDir() + "distribution.json"), content, UTF_8);
    }

    public static void prepareGenerationAll(int size) {
        columns.values().stream().parallel().forEach(column -> column.prepareGeneration(size));
    }

    public static AbstractColumn getColumn(String columnName) throws CannotFindColumnException {
        AbstractColumn column = columns.get(columnName);
        if (column == null) {
            throw new CannotFindColumnException(columnName);
        }
        return column;
    }

    /**
     * 提取col的range信息(最大值，最小值)
     *
     * @param canonicalColumnNames 需要设置的col
     * @param sqlResult            有关的SQL结果(由AbstractDbConnector.getDataRange返回)
     * @throws TouchstoneException 设置失败
     */
    public static void setDataRangeBySqlResult(List<String> canonicalColumnNames, String[] sqlResult) throws TouchstoneException {
        int index = 0;
        for (String canonicalColumnName : canonicalColumnNames) {
            AbstractColumn column = ColumnManager.getColumn(canonicalColumnName);
            switch (column.getColumnType()) {
                case INTEGER:
                    ((IntColumn) column).setMin(Integer.parseInt(sqlResult[index++]));
                    ((IntColumn) column).setMax(Integer.parseInt(sqlResult[index++]));
                    ((IntColumn) column).setNdv(Integer.parseInt(sqlResult[index++]));
                    break;
                case VARCHAR:
                    ((StringColumn) column).setMinLength(Integer.parseInt(sqlResult[index++]));
                    ((StringColumn) column).setMaxLength(Integer.parseInt(sqlResult[index++]));
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
                    throw new TouchstoneException("未匹配到的类型");
            }
            column.setNullPercentage(Float.parseFloat(sqlResult[index++]));
        }
    }
}
