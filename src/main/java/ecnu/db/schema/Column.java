package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;

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
    private int avgLength;
    private int maxLength;
    @JsonIgnore
    private BigDecimal decimalPre;
    @JsonIgnore
    private StringTemplate stringTemplate;
    @JsonIgnore
    private long[] columnData;
    @JsonIgnore
    private Distribution distribution;

    public Column() {
    }

    public Column(ColumnType columnType) {
        this.columnType = columnType;
    }

    public Distribution getDistribution() {
        return distribution;
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
            stringTemplate = new StringTemplate(avgLength, maxLength, specialValue, range);
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

    private boolean[] computeEqualComparatorResult(long value) {
        boolean[] ret = new boolean[columnData.length];
        for (int i = 0; i < columnData.length; i++) {
            ret[i] = columnData[i] != Long.MIN_VALUE && columnData[i] == value;
        }
        return ret;
    }

    private boolean[] computeNotEqualComparatorResult(long value) {
        boolean[] ret = new boolean[columnData.length];
        for (int i = 0; i < columnData.length; i++) {
            ret[i] = columnData[i] != Long.MIN_VALUE && columnData[i] != value;
        }
        return ret;
    }

    private boolean[] computeGreaterComparatorResult(long value) {
        boolean[] ret = new boolean[columnData.length];
        for (int i = 0; i < columnData.length; i++) {
            ret[i] = columnData[i] != Long.MIN_VALUE && columnData[i] > value;
        }
        return ret;
    }

    private boolean[] computeGreaterOrEqualComparatorResult(long value) {
        boolean[] ret = new boolean[columnData.length];
        for (int i = 0; i < columnData.length; i++) {
            ret[i] = columnData[i] != Long.MIN_VALUE && columnData[i] >= value;
        }
        return ret;
    }

    private boolean[] computeLessComparatorResult(long value) {
        boolean[] ret = new boolean[columnData.length];
        for (int i = 0; i < columnData.length; i++) {
            ret[i] = columnData[i] != Long.MIN_VALUE && columnData[i] < value;
        }
        return ret;
    }

    private boolean[] computeLessOrEqualComparatorResult(long value) {
        boolean[] ret = new boolean[columnData.length];
        for (int i = 0; i < columnData.length; i++) {
            ret[i] = columnData[i] != Long.MIN_VALUE && columnData[i] <= value;
        }
        return ret;
    }


    private boolean[] computeInComparator(List<Parameter> parameters) {
        boolean[] ret = new boolean[columnData.length];
        HashSet<Long> parameterData = new HashSet<>();
        for (Parameter parameter : parameters) {
            parameterData.add(parameter.getData());
        }
        for (int i = 0; i < columnData.length; i++) {
            ret[i] = columnData[i] != Long.MIN_VALUE && parameterData.contains(columnData[i]);
        }
        return ret;
    }

    private boolean[] computeNotInComparator(List<Parameter> parameters) {
        boolean[] ret = new boolean[columnData.length];
        HashSet<Long> parameterData = new HashSet<>();
        for (Parameter parameter : parameters) {
            parameterData.add(parameter.getData());
        }
        for (int i = 0; i < columnData.length; i++) {
            ret[i] = columnData[i] != Long.MIN_VALUE && !parameterData.contains(columnData[i]);
        }
        return ret;
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
        return switch (operator) {
            case EQ, LIKE, ISNULL -> computeEqualComparatorResult(value);
            case NE, NOT_LIKE, IS_NOT_NULL -> computeNotEqualComparatorResult(value);
            case LT -> computeLessComparatorResult(value);
            case LE -> computeLessOrEqualComparatorResult(value);
            case GT -> computeGreaterComparatorResult(value);
            case GE -> computeGreaterOrEqualComparatorResult(value);
            case IN -> computeInComparator(parameters);
            case NOT_IN -> computeNotInComparator(parameters);
            default -> throw new UnsupportedOperationException();
        };
    }

    /**
     * @return 返回用于multi-var计算的一个double数组
     */
    public double[] calculate() {
        //lazy生成computeData
        double[] ret = new double[columnData.length];
        switch (columnType) {
            case DATE, DATETIME -> {
                for (int i = 0; i < columnData.length; i++) {
                    ret[i] = (double) columnData[i] + min;
                }
            }
            case DECIMAL -> {
                for (int i = 0; i < columnData.length; i++) {
                    ret[i] = ((double) (columnData[i] + min)) / specialValue;
                }
            }
            case INTEGER -> {
                for (int i = 0; i < columnData.length; i++) {
                    ret[i] = (double) (specialValue * columnData[i]) + min;
                }
            }
            default -> throw new IllegalStateException("Unexpected value: " + columnType);
        }
        return ret;
    }

    public int getAvgLength() {
        return avgLength;
    }

    public void setAvgLength(int avgLength) {
        this.avgLength = avgLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    public String transferDataToValue(long data) {
        if (data == Long.MIN_VALUE) {
            return "\\N";
        }
        return switch (columnType) {
            case INTEGER -> Long.toString((specialValue * data) + min);
            case DECIMAL -> BigDecimal.valueOf(data + min).multiply(decimalPre).toString();
            case VARCHAR -> stringTemplate.getParameterValue(data);
            case DATE -> CommonUtils.dateFormatter.format(Instant.ofEpochSecond((data + min) * 24 * 60 * 60));
            case DATETIME -> CommonUtils.dateTimeFormatter.format(Instant.ofEpochSecond(data + min));
            default -> throw new UnsupportedOperationException();
        };
    }

    public void addSubStringIndex(long dataId) {
        stringTemplate.addSubStringIndex(dataId);
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
