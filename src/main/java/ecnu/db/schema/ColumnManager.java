package ecnu.db.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;

import java.io.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static ecnu.db.utils.CommonUtils.CANONICAL_NAME_SPLIT_REGEX;
import static ecnu.db.utils.CommonUtils.CSV_MAPPER;

public class ColumnManager {
    public static final String COLUMN_STRING_INFO = "/stringTemplate.json";
    public static final String COLUMN_DISTRIBUTION_INFO = "/distribution.json";
    public static final String COLUMN_EQDISTRIBUTION_INFO = "/eq_distribution.json";
    public static final String COLUMN_BOUNDPARA_INFO = "/boundPara.json";
    public static final String COLUMN_METADATA_INFO = "/column.csv";
    private static final ColumnManager INSTANCE = new ColumnManager();
    private static final CsvSchema columnSchema = CSV_MAPPER.schemaFor(Column.class);
    private static final DateTimeFormatter FMT = new DateTimeFormatterBuilder()
            .appendOptional(new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss")
                    .appendFraction(ChronoField.MILLI_OF_SECOND, 2, 3, true) // min 2 max 3
                    .toFormatter())
            .appendOptional(new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter())
            .appendOptional(new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd")
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter())
            .appendOptional(DateTimeFormatter.ISO_LOCAL_DATE)
            .toFormatter();
    private final LinkedHashMap<String, Column> columns = new LinkedHashMap<>();
    private File distributionInfoPath;

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
        this.distributionInfoPath = new File(resultDir);
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

    public boolean isDateColumn(String columnName) {
        return columns.containsKey(columnName) && columns.get(columnName).getColumnType() == ColumnType.DATE;
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

    public void storeColumnMetaData() throws IOException {
        try (StringWriter writer = new StringWriter()) {
            writer.write("ColumnName");
            for (int i = 0; i < columnSchema.size(); i++) {
                writer.write("," + columnSchema.columnName(i));
            }
            writer.write("\n");
            SequenceWriter seqW = CSV_MAPPER.writerFor(Column.class).with(columnSchema).writeValues(writer);
            for (var column : columns.entrySet()) {
                writer.write(column.getKey() + ",");
                seqW.write(column.getValue());
            }
            CommonUtils.writeFile(distributionInfoPath.getPath() + COLUMN_METADATA_INFO, writer.toString());
        }
    }

    public void storeColumnDistribution() throws IOException {
        File distribution = new File(distributionInfoPath + "/distribution");
        if(!distribution.exists()){
            distribution.mkdir();
        }
        Map<String, Map<Long, boolean[]>> columName2StringTemplate = new HashMap<>();
        for (Map.Entry<String, Column> column : columns.entrySet()) {
            if (column.getValue().getColumnType() == ColumnType.VARCHAR &&
                    column.getValue().getStringTemplate().getLikeIndex2Status() != null) {
                columName2StringTemplate.put(column.getKey(), column.getValue().getStringTemplate().getLikeIndex2Status());
            }
        }
        String content = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(columName2StringTemplate);
        CommonUtils.writeFile(distribution.getPath() + COLUMN_STRING_INFO, content);
        Map<String, List<Map.Entry<Long, BigDecimal>>> bucket2Probabilities = new HashMap<>();
        Map<String, Map<Long, BigDecimal>> eq2Probabilities = new HashMap<>();
        Map<String, List<Parameter>> boundParas = new HashMap<>();
        for (Map.Entry<String, Column> column : columns.entrySet()) {
            if (column.getValue().getBucketBound2FreeSpace().size() > 1 ||
                    column.getValue().getBucketBound2FreeSpace().get(0).getValue().compareTo(BigDecimal.ONE) < 0) {
                bucket2Probabilities.put(column.getKey(), column.getValue().getBucketBound2FreeSpace());
            }
            if (column.getValue().getEqConstraint2Probability().size() > 0) {
                eq2Probabilities.put(column.getKey(), column.getValue().getEqConstraint2Probability());
            }
            if (!column.getValue().getBoundPara().isEmpty()) {
                boundParas.put(column.getKey(), column.getValue().getBoundPara());
            }
        }
        content = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(bucket2Probabilities);
        CommonUtils.writeFile(distribution.getPath() + COLUMN_DISTRIBUTION_INFO, content);
        content = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(eq2Probabilities);
        CommonUtils.writeFile(distribution.getPath() + COLUMN_EQDISTRIBUTION_INFO, content);
        content = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(boundParas);
        CommonUtils.writeFile(distribution.getPath() + COLUMN_BOUNDPARA_INFO, content);
    }

    public void loadColumnMetaData() throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(distributionInfoPath.getPath() + COLUMN_METADATA_INFO))) {
            String line = bufferedReader.readLine();
            while ((line = bufferedReader.readLine()) != null) {
                int commaIndex = line.indexOf(",");
                String columnData = line.substring(commaIndex + 1);
                Column column = CSV_MAPPER.readerFor(Column.class).with(columnSchema).readValue(columnData);
                if (column.getColumnType() == ColumnType.VARCHAR) {
                    column.initStringTemplate();
                }
                column.initBucketBound2FreeSpace();
                columns.put(line.substring(0, commaIndex), column);
            }
        }
    }

    public void loadColumnDistribution() throws IOException {
        File distribution = new File(distributionInfoPath + "/distribution");
        String content = CommonUtils.readFile(distribution.getPath() + COLUMN_STRING_INFO);
        Map<String, Map<Long, boolean[]>> columName2StringTemplate = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        for (Map.Entry<String, Map<Long, boolean[]>> template : columName2StringTemplate.entrySet()) {
            columns.get(template.getKey()).getStringTemplate().setLikeIndex2Status(template.getValue());
        }
        content = CommonUtils.readFile(distribution.getPath() + COLUMN_DISTRIBUTION_INFO);
        Map<String, List<Map.Entry<Long, BigDecimal>>> bucket2Probabilities = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        for (Map.Entry<String, List<Map.Entry<Long, BigDecimal>>> bucket : bucket2Probabilities.entrySet()) {
            columns.get(bucket.getKey()).setBucketBound2FreeSpace(bucket.getValue());
        }
        content = CommonUtils.readFile(distribution.getPath() + COLUMN_EQDISTRIBUTION_INFO);
        Map<String, Map<Long, BigDecimal>> eq2Probabilities = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        for (Map.Entry<String, Map<Long, BigDecimal>> eq2Probability : eq2Probabilities.entrySet()) {
            columns.get(eq2Probability.getKey()).setEqConstraint2Probability(eq2Probability.getValue());
        }
        content = CommonUtils.readFile(distribution.getPath() + COLUMN_BOUNDPARA_INFO);
        Map<String, List<Parameter>> boundParas = CommonUtils.MAPPER.readValue(content, new TypeReference<>() {
        });
        for (Map.Entry<String, List<Parameter>> boundPara : boundParas.entrySet()) {
            columns.get(boundPara.getKey()).setBoundPara(boundPara.getValue());
        }
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
            String minResult = sqlResult[index++];
            String maxResult = sqlResult[index++];
            long min;
            long range;
            long specialValue;
            if (minResult == null) {
                min = -1;
                range = -1;
                specialValue = 0;
                if (column.getColumnType().isHasCardinalityConstraint()) {
                    index++;
                }
            } else {
                switch (column.getColumnType()) {
                    case INTEGER -> {
                        min = Long.parseLong(minResult);
                        long maxBound = Long.parseLong(maxResult);
                        range = Long.parseLong(sqlResult[index++]);
                        specialValue = (int) ((maxBound - min + 1) / range);
                    }
                    case VARCHAR -> {
                        column.setMinLength(Integer.parseInt(minResult));
                        //todo avg length
                        column.setRangeLength(Integer.parseInt(maxResult) - column.getMinLength());
                        min = 0;
                        range = Integer.parseInt(sqlResult[index++]);
                        specialValue = ThreadLocalRandom.current().nextInt();
                    }
                    case DECIMAL -> {
                        specialValue = column.getSpecialValue();
                        min = (long) (Double.parseDouble(minResult) * specialValue);
                        range = (long) (Double.parseDouble(maxResult) * specialValue) - min + 1;
                    }
                    case DATE -> {
                        min = LocalDateTime.parse(minResult, FMT).toEpochSecond(ZoneOffset.UTC) / (24 * 60 * 60);
                        range = LocalDateTime.parse(maxResult, FMT).toEpochSecond(ZoneOffset.UTC) / (24 * 60 * 60) - min + 1;
                        specialValue = 0;
                    }
                    case DATETIME -> {
                        min = LocalDateTime.parse(minResult, FMT).toEpochSecond(ZoneOffset.UTC);
                        range = LocalDateTime.parse(maxResult, FMT).toEpochSecond(ZoneOffset.UTC) - min + 1;
                        specialValue = 0;
                    }
                    default -> throw new TouchstoneException("未匹配到的类型");
                }
            }
            column.setMin(min);
            column.setRange(range);
            column.setSpecialValue(specialValue);
            if (column.getColumnType() == ColumnType.VARCHAR) {
                column.initStringTemplate();
            }
            column.initBucketBound2FreeSpace();
            column.setNullPercentage(Float.parseFloat(sqlResult[index++]));
        }
    }

    public void prepareParameterInit(Collection<String> columnNames, int size) {
        columnNames.stream().parallel().forEach(columnName -> getColumn(columnName).prepareTupleData(size));
    }

    public void prepareGeneration(Collection<String> columnNames, int size) {
        columnNames.stream().parallel().forEach(columnName -> getColumn(columnName).prepareTupleData(size));
        List<Integer> rowIndex = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            rowIndex.add(i);
        }
        Collections.shuffle(rowIndex);
        columnNames.stream().parallel().forEach(columnName -> getColumn(columnName).shuffleRows(rowIndex));
    }

    public void insertBetweenProbability(String columnName, BigDecimal probability,
                                         CompareOperator lessOperator, List<Parameter> lessParameters,
                                         CompareOperator greaterOperator, List<Parameter> greaterParameters) {
        columns.get(columnName).insertBetweenProbability(probability, lessOperator, lessParameters, greaterOperator, greaterParameters);
    }


}
