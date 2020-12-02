package ecnu.db.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.operation.CompareOperator;
import ecnu.db.schema.column.*;
import ecnu.db.schema.column.bucket.EqBucket;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.utils.exception.compute.InstantiateParameterException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;

import static ecnu.db.schema.column.ColumnType.DECIMAL;
import static ecnu.db.schema.column.ColumnType.INTEGER;
import static ecnu.db.utils.CommonUtils.CANONICAL_NAME_SPLIT_REGEX;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ColumnManager {
    private static final ColumnManager INSTANCE = new ColumnManager();
    private LinkedHashMap<String, AbstractColumn> columns = new LinkedHashMap<>();

    private File distributionInfoPath;

    // Private constructor suppresses
    // default public constructor
    private ColumnManager() {
    }

    public static ColumnManager getInstance() {
        return INSTANCE;
    }

    public void setResultDir(String resultDir) {
        if (distributionInfoPath == null) {
            this.distributionInfoPath = new File(resultDir + CommonUtils.COLUMN_MANAGE_INFO);
        }
    }

    public AbstractColumn getColumn(String columnName) {
        return columns.get(columnName);
    }

    public boolean[] evaluate(String columnName, CompareOperator operator, List<Parameter> parameters, boolean hasNot) {
        return columns.get(columnName).evaluate(operator, parameters, hasNot);
    }

    public boolean[] getIsnullEvaluations(String columnName) {
        return columns.get(columnName).getIsnullEvaluations();
    }

    public float getNullPercentage(String columnName) {
        return columns.get(columnName).getNullPercentage();
    }

    public double[] calculate(String columnName) {
        AbstractColumn column = columns.get(columnName);
        double[] ret;
        if (column.getColumnType() == INTEGER) {
            ret = ((IntColumn) column).calculate();
        } else if (column.getColumnType() == DECIMAL) {
            ret = ((DecimalColumn) column).calculate();
        } else {
            throw new UnsupportedOperationException();
        }
        return ret;
    }

    public float getMin(String columnName) throws InstantiateParameterException {
        AbstractColumn column = columns.get(columnName);
        if (column instanceof IntColumn) {
            return (float) ((IntColumn) column).getMin();
        } else if (column instanceof DecimalColumn) {
            return (float) ((DecimalColumn) column).getMin();
        } else {
            throw new InstantiateParameterException(String.format("计算节点出现非法的column'%s'", column));
        }
    }

    public List<EqBucket> getEqBuckets(String columnName) {
        return columns.get(columnName).getEqBuckets();
    }

    public float getMax(String columnName) throws InstantiateParameterException {
        AbstractColumn column = columns.get(columnName);
        if (column instanceof IntColumn) {
            return (float) ((IntColumn) column).getMax();
        } else if (column instanceof DecimalColumn) {
            return (float) ((DecimalColumn) column).getMax();
        } else {
            throw new InstantiateParameterException(String.format("计算节点出现非法的column'%s'", column));
        }
    }

    public ColumnType getColumnType(String columnName) {
        return columns.get(columnName).getColumnType();
    }

    public int getNdv(String columnName) {
        return columns.get(columnName).getNdv();
    }

    public void addColumn(String columnName, AbstractColumn column) throws TouchstoneException {
        if (columnName.split(CANONICAL_NAME_SPLIT_REGEX).length != 3) {
            throw new TouchstoneException("非canonicalColumnName格式");
        }
        columns.put(columnName, column);
    }

    public void storeColumnDistribution() throws IOException {
        String content = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(columns);
        FileUtils.writeStringToFile(distributionInfoPath, content, UTF_8);
    }


    public void loadColumnDistribution(String columnDistributionPath) throws IOException {
        columns = CommonUtils.MAPPER.readValue(FileUtils.readFileToString(new File(columnDistributionPath), UTF_8),
                new TypeReference<LinkedHashMap<String, AbstractColumn>>() {
                });
    }

    public void prepareGenerationAll(int size) {
        columns.values().stream().parallel().forEach(column -> column.prepareGeneration(size));
    }


    public void initAllEqProbabilityBucket() {
        columns.values().parallelStream().forEach(AbstractColumn::initEqProbabilityBucket);
    }

    public void initAllEqParameter() {
        columns.values().parallelStream().forEach(AbstractColumn::initEqParameter);
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
            AbstractColumn column = columns.get(canonicalColumnName);
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
