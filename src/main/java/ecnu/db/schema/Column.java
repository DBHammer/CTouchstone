package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

/**
 * @author wangqingshuai
 */
@JsonPropertyOrder({"columnType", "nullPercentage", "specialValue", "min", "range", "minLength", "rangeLength", "originalType"})
public class Column {
    private ColumnType columnType;
    private long min;
    private String originalType;
    private long range;
    private long specialValue;
    private BigDecimal nullPercentage;
    private int minLength;
    private int rangeLength;
    @JsonIgnore
    private BigDecimal decimalPre;
    @JsonIgnore
    private StringTemplate stringTemplate;
    @JsonIgnore
    private long[] columnData;
    @JsonIgnore
    private Distribution distribution;

    public Distribution getDistribution() {
        return distribution;
    }

    public Column() {
    }

    public Column(ColumnType columnType) {
        this.columnType = columnType;
    }

    public String getOriginalType() {
        return originalType;
    }

    public void setOriginalType(String originalType) {
        this.originalType = originalType;
    }


    public void init() {
        distribution = new Distribution(nullPercentage, range);
        if (columnType == ColumnType.VARCHAR) {
            stringTemplate = new StringTemplate(minLength, rangeLength, specialValue);
        }
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public long getRange() {
        return range;
    }

    public void setRange(long range) {
        this.range = range;
    }

    public BigDecimal getNullPercentage() {
        return nullPercentage;
    }

    public void setNullPercentage(BigDecimal nullPercentage) {
        this.nullPercentage = nullPercentage;
    }


    public void prepareTupleData(int size) {
        columnData = distribution.prepareTupleData(size);
    }


    public void shuffleRows(List<Integer> rowIndexes) {
        long[] tempIndex = new long[rowIndexes.size()];
        for (int i = 0; i < tempIndex.length; i++) {
            tempIndex[i] = columnData[rowIndexes.get(i)];
        }
        columnData = tempIndex;
    }

    /**
     * 无运算比较，针对传入的参数，对于单操作符进行比较
     *
     * @param operator   运算操作符
     * @param parameters 待比较的参数
     * @return 运算结果
     */
    public boolean[] evaluate(CompareOperator operator, List<Parameter> parameters) {
        long value;
        if (operator == CompareOperator.ISNULL) {
            value = Long.MIN_VALUE;
        } else {
            value = parameters.get(0).getData();
        }
        boolean[] ret = new boolean[columnData.length];
        IntStream indexStream = IntStream.range(0, columnData.length);
        switch (operator) {
            case EQ, LIKE, ISNULL -> indexStream.forEach(i -> ret[i] = columnData[i] == value);
            case NE, NOT_LIKE, IS_NOT_NULL -> indexStream.forEach(i -> ret[i] = columnData[i] != value);
            case LT -> indexStream.forEach(i -> ret[i] = columnData[i] < value);
            case LE -> indexStream.forEach(i -> ret[i] = columnData[i] <= value);
            case GT -> indexStream.forEach(i -> ret[i] = columnData[i] > value);
            case GE -> indexStream.forEach(i -> ret[i] = columnData[i] >= value);
            case IN -> {
                HashSet<Long> parameterData = new HashSet<>();
                for (Parameter parameter : parameters) {
                    parameterData.add(parameter.getData());
                }
                indexStream.forEach(i -> ret[i] = parameterData.contains(columnData[i]));
            }
            case NOT_IN -> {
                HashSet<Long> parameterData = new HashSet<>();
                for (Parameter parameter : parameters) {
                    parameterData.add(parameter.getData());
                }
                indexStream.forEach(i -> ret[i] = !parameterData.contains(columnData[i]));
            }
            default -> throw new UnsupportedOperationException();
        }
        return ret;
    }

    /**
     * @return 返回用于multi-var计算的一个double数组
     */
    public double[] calculate() {
        //lazy生成computeData
        return switch (columnType) {
            case DATE, DATETIME ->
                    Arrays.stream(columnData).parallel().mapToDouble(data -> (double) data + min).toArray();
            case DECIMAL ->
                    Arrays.stream(columnData).parallel().mapToDouble(data -> (double) (data + min) / specialValue).toArray();
            case INTEGER ->
                    Arrays.stream(columnData).parallel().mapToDouble(data -> (double) (specialValue * data) + min).toArray();
            default -> throw new IllegalStateException("Unexpected value: " + columnType);
        };
    }

    public int getMinLength() {
        return minLength;
    }

    public void setMinLength(int minLength) {
        this.minLength = minLength;
    }

    public int getRangeLength() {
        return rangeLength;
    }

    public void setRangeLength(int rangeLength) {
        this.rangeLength = rangeLength;
    }

    public String transferDataToValue(long data) {
        return switch (columnType) {
            case INTEGER -> Long.toString((specialValue * data) + min);
            case DECIMAL -> BigDecimal.valueOf(data + min).multiply(decimalPre).toString();
            case VARCHAR -> stringTemplate.transferColumnData2Value(data, range);
            case DATE -> CommonUtils.dateFormatter.format(Instant.ofEpochSecond((data + min) * 24 * 60 * 60));
            case DATETIME -> CommonUtils.dateTimeFormatter.format(Instant.ofEpochSecond(data + min));
            default -> throw new UnsupportedOperationException();
        };
    }

    public void setColumnData(long[] columnData) {
        this.columnData = columnData;
    }

    public String output(int index) {
        return transferDataToValue(columnData[index]);
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public long getSpecialValue() {
        return specialValue;
    }

    public void setSpecialValue(long specialValue) {
        if (columnType == ColumnType.DECIMAL) {
            decimalPre = BigDecimal.ONE.divide(BigDecimal.valueOf(specialValue), CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);
        }
        this.specialValue = specialValue;
    }


    public StringTemplate getStringTemplate() {
        return stringTemplate;
    }

}
