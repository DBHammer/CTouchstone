package ecnu.db.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static ecnu.db.utils.CommonUtils.CANONICAL_NAME_SPLIT_REGEX;

public class ColumnManager {
    private static final ColumnManager INSTANCE = new ColumnManager();
    private LinkedHashMap<String, Column> columns = new LinkedHashMap<>();
    private File distributionInfoPath;
    public static final String COLUMN_MANAGE_INFO = "/distribution.json";
    private static final DateTimeFormatter FMT = new DateTimeFormatterBuilder()
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            .appendOptional(new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd")
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter())
            .appendOptional(DateTimeFormatter.ISO_LOCAL_DATE)
            .toFormatter();

    // Private constructor suppresses
    // default public constructor
    private ColumnManager() {
    }

    public static ColumnManager getInstance() {
        return INSTANCE;
    }

    public void setData(String columnName, long[] data) {
        getColumn(columnName).setColumnData(data);
    }

    public void setSpecialValue(String columnName, int specialValue) {
        getColumn(columnName).setSpecialValue(specialValue);
    }

    public void insertUniVarProbability(String columnName, BigDecimal probability, CompareOperator operator, List<Parameter> parameters) {
        getColumn(columnName).insertUniVarProbability(probability, operator, parameters);
    }

    public void setResultDir(String resultDir) {
        this.distributionInfoPath = new File(resultDir + COLUMN_MANAGE_INFO);
    }

    public Column getColumn(String columnName) {
        return columns.get(columnName);
    }

    public boolean[] evaluate(String columnName, CompareOperator operator, List<Parameter> parameters) {
        return columns.get(columnName).evaluate(operator, parameters);
    }

    public float getNullPercentage(String columnName) {
        return columns.get(columnName).getNullPercentage();
    }

    public double[] calculate(String columnName) {
        return getColumn(columnName).calculate();
    }

    public ColumnType getColumnType(String columnName) {
        return columns.get(columnName).getColumnType();
    }

    public int getNdv(String columnName) {
        return (int) getColumn(columnName).getRange();
    }

    public void addColumn(String columnName, Column column) throws TouchstoneException {
        if (columnName.split(CANONICAL_NAME_SPLIT_REGEX).length != 3) {
            throw new TouchstoneException("非canonicalColumnName格式");
        }
        columns.put(columnName, column);
    }

    public void storeColumnDistribution() throws IOException {
        String content = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(columns);
        CommonUtils.writeFile(distributionInfoPath.getPath(), content);
    }

    public void loadColumnDistribution() throws IOException {
        String fileContent = CommonUtils.readFile(distributionInfoPath.getPath());
        columns = CommonUtils.MAPPER.readValue(fileContent, new TypeReference<>() {
        });
    }

    public void initAllEqParameter() {
        columns.values().parallelStream().forEach(Column::initEqParameter);
    }

    /**
     * 提取col的range信息(最大值，最小值)
     *
     * @param canonicalColumnNames 需要设置的col
     * @param sqlResult            有关的SQL结果(由AbstractDbConnector.getDataRange返回)
     * @throws TouchstoneException 设置失败
     */
    public void setDataRangeBySqlResult(List<String> canonicalColumnNames, String[] sqlResult) throws TouchstoneException {
        int index = 0;
        for (String canonicalColumnName : canonicalColumnNames) {
            Column column = columns.get(canonicalColumnName);
            long min;
            long range;
            long specialValue;
            switch (column.getColumnType()) {
                case INTEGER -> {
                    min = Long.parseLong(sqlResult[index++]);
                    long maxBound = Long.parseLong(sqlResult[index++]);
                    range = Long.parseLong(sqlResult[index++]);
                    specialValue = (int) ((maxBound - min + 1) / range);
                }
                case VARCHAR -> {
                    StringTemplate stringTemplate = new StringTemplate();
                    stringTemplate.minLength = Integer.parseInt(sqlResult[index++]);
                    stringTemplate.rangeLength = Integer.parseInt(sqlResult[index++]) - stringTemplate.minLength;
                    column.setStringTemplate(stringTemplate);
                    min = 0;
                    range = Integer.parseInt(sqlResult[index++]);
                    specialValue = ThreadLocalRandom.current().nextInt();
                }
                case DECIMAL -> {
                    int precision = CommonUtils.SAMPLE_DOUBLE_PRECISION;
                    min = (long) (Double.parseDouble(sqlResult[index++]) * precision);
                    range = (long) (Double.parseDouble(sqlResult[index++]) * precision) - min;
                    specialValue = precision;
                }
                case DATE, DATETIME -> {
                    min = LocalDateTime.parse(sqlResult[index++], FMT).toEpochSecond(ZoneOffset.UTC) * 1000;
                    range = LocalDateTime.parse(sqlResult[index++], FMT).toEpochSecond(ZoneOffset.UTC) * 1000 - min;
                    specialValue = 0;
                }
                default -> throw new TouchstoneException("未匹配到的类型");
            }
            column.setMin(min);
            column.setRange(range);
            column.setSpecialValue(specialValue);
            column.initBucketBound2FreeSpace();
            column.setNullPercentage(Float.parseFloat(sqlResult[index++]));
        }
    }

    public void prepareGeneration(Collection<String> columnNames, int size) {
        columnNames.stream().parallel().forEach(columnName -> getColumn(columnName).prepareTupleData(size));
    }

    public List<String> getData(String columnName) {
        return getColumn(columnName).output();
    }

    public void insertBetweenProbability(String columnName, BigDecimal probability,
                                         CompareOperator lessOperator, List<Parameter> lessParameters,
                                         CompareOperator greaterOperator, List<Parameter> greaterParameters) {
        columns.get(columnName).insertBetweenProbability(probability, lessOperator, lessParameters, greaterOperator, greaterParameters);
    }


}
