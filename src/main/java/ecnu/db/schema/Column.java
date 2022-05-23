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
    @JsonIgnore
    private final TreeMap<BigDecimal, List<Parameter>> eqRequest2ParameterIds = new TreeMap<>();
    @JsonIgnore
    private final HashSet<Integer> likeParameterId = new HashSet<>();
    private ColumnType columnType;
    private long min;
    private String originalType;
    private long range;
    private long specialValue;
    private float nullPercentage;
    private int minLength;
    private int rangeLength;
    @JsonIgnore
    private BigDecimal decimalPre;

    @JsonIgnore
    private List<Parameter> boundPara = new ArrayList<>();
    @JsonIgnore
    private StringTemplate stringTemplate;
    @JsonIgnore
    private long[] columnData;
    @JsonIgnore
    private boolean columnData2ComputeData = false;
    @JsonIgnore
    private double[] computeData;
    @JsonIgnore
    public TreeMap<BigDecimal, List<Parameter>> pvAndPbList = new TreeMap<>();

    @JsonIgnore
    public SortedMap<Long, BigDecimal> paraData2Probability = new TreeMap<>();

    public Column() {
    }

    public void initPvAndPbList() {
        pvAndPbList.clear();
        Parameter parameterEnd = new Parameter();
        parameterEnd.setId(-1);
        parameterEnd.setData(range);
        parameterEnd.setDataValue(transferDataToValue(range));
        pvAndPbList.put(BigDecimal.ONE, new ArrayList<>(List.of(parameterEnd)));
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

    public void initStringTemplate() {
        stringTemplate = new StringTemplate(minLength, rangeLength, specialValue);
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

    public float getNullPercentage() {
        return nullPercentage;
    }

    public void setNullPercentage(float nullPercentage) {
        this.nullPercentage = nullPercentage;
    }

    private void dealZeroPb(List<Parameter> parameters) {
        for (Parameter parameter : parameters) {
            parameter.setData(min - 1);
            if (likeParameterId.contains(parameter.getId())) {
                parameter.setDataValue(stringTemplate.getLikeValue(min - 1, parameter.getDataValue()));
            } else {
                parameter.setDataValue(transferDataToValue(min - 1));
            }
        }
    }

    /**
     * 插入非等值约束概率
     *
     * @param probability 约束概率
     * @param operator    操作符
     * @param parameters  参数
     */
    private void insertNonEqProbability(BigDecimal probability, CompareOperator operator, List<Parameter> parameters) {
        if (operator == CompareOperator.GE || operator == CompareOperator.GT) {
            probability = BigDecimal.ONE.subtract(probability);
        }
        if (probability.compareTo(BigDecimal.ONE) < 0 && probability.compareTo(BigDecimal.ZERO) > 0) {
            pvAndPbList.putIfAbsent(probability, new ArrayList<>());
            pvAndPbList.get(probability).addAll(parameters);
        } else {
            long dataIndex = probability.compareTo(BigDecimal.ZERO) <= 0 ? -1 : range + 1;
            for (Parameter parameter : parameters) {
                parameter.setData(dataIndex);
                parameter.setDataValue(transferDataToValue(dataIndex));
            }
        }
    }

    private void insertEqualProbability(BigDecimal probability, List<Parameter> parameters) throws TouchstoneException {
        if (probability.compareTo(BigDecimal.ZERO) == 0) {
            dealZeroPb(parameters);
        } else {
            // 标记该参数为等值的参数
            parameters.forEach(parameter -> parameter.setEqualPredicate(true));
            BigDecimal minRange = BigDecimal.TEN;
            BigDecimal minCDF = BigDecimal.ZERO;
            for (BigDecimal currentCDF : pvAndPbList.keySet()) {
                BigDecimal currentRange = getRange(currentCDF);
                // 当前空间可以被完全利用
                if (currentRange.compareTo(probability) == 0) {
                    pvAndPbList.get(currentCDF).addAll(parameters);
                    return;
                } else {
                    // 找到可以放置的最小空间
                    boolean enoughCapacity = currentRange.compareTo(probability) > 0;
                    boolean isMinRange = minRange.compareTo(currentRange) > 0;
                    // 如果cdf中包含等值的参数 则该range为一个等值的range，不可分割
                    boolean isNonEqualRange = isNonEqualRange(pvAndPbList.get(currentCDF));
                    if (enoughCapacity && isMinRange && isNonEqualRange) {
                        minRange = currentRange;
                        minCDF = currentCDF;
                    }
                }
            }
            // 如果没有找到
            if (minCDF.compareTo(BigDecimal.ZERO) == 0) {
                throw new TouchstoneException("不存在可以放置的range");
            } else {
                BigDecimal remainCapacity = minRange.subtract(probability);
                BigDecimal eqCDf = minCDF.subtract(remainCapacity);
                pvAndPbList.put(eqCDf, parameters);
            }
        }
    }

    private boolean isNonEqualRange(List<Parameter> parameters) {
        return parameters.stream().noneMatch(Parameter::isEqualPredicate);
    }

    private BigDecimal getRange(BigDecimal currentCDF) {
        BigDecimal lastCDf = pvAndPbList.lowerKey(currentCDF);
        if (lastCDf == null) {
            lastCDf = BigDecimal.ZERO;
        }
        return currentCDF.subtract(lastCDf);
    }

    public void initAllParameters() {
        long remainCardinality = range - pvAndPbList.size();
        BigDecimal remainRange = pvAndPbList.entrySet().stream()
                // 找到所有的非等值的range右边界
                .filter(e -> isNonEqualRange(e.getValue()))
                // 确定其range大小
                .map(e -> getRange(e.getKey()))
                // 计算range的和
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal cardinalityPercentage = BigDecimal.valueOf(remainCardinality).divide(remainRange, CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);
        long dataIndex = 0;
        for (var CDF2Parameters : pvAndPbList.entrySet()) {
            dataIndex++;
            BigDecimal range = getRange(CDF2Parameters.getKey());
            if (isNonEqualRange(CDF2Parameters.getValue())) {
                dataIndex += cardinalityPercentage.multiply(range).intValue();
            }
            paraData2Probability.put(dataIndex, range);
            for (Parameter parameter : CDF2Parameters.getValue()) {
                parameter.setData(dataIndex);
                parameter.setDataValue(transferDataToValue(dataIndex));
            }
        }

    }

    public void applyUniVarConstraint(BigDecimal probability, CompareOperator operator, List<Parameter> parameters) throws TouchstoneException {
        switch (operator) {
            case NE, NOT_LIKE, NOT_IN -> eqRequest2ParameterIds.computeIfAbsent(BigDecimal.ONE.subtract(probability), i -> new LinkedList<>()).add(parameters.get(0));
            case EQ, LIKE, IN -> insertEqualProbability(probability, parameters);
            case GT, LT, GE, LE -> insertNonEqProbability(probability, operator, parameters);
            default -> throw new UnsupportedOperationException();
        }
        likeParameterId.addAll(parameters.stream()
                .filter(parameter -> parameter.getType() == Parameter.ParameterType.LIKE ||
                        parameter.getType() == Parameter.ParameterType.SUBSTRING)
                .mapToInt(Parameter::getId).boxed().toList());
    }

    /**
     * 插入between的概率
     * 目前如果列上存在between，则不会实例化等值约束
     *
     * @param probability       between的概率
     * @param lessParameters    代表between的小于条件的参数
     * @param greaterParameters 代表between的大于条件的参数
     */
    public void insertBetweenProbability(BigDecimal probability,
                                         CompareOperator lessOperator, List<Parameter> lessParameters,
                                         CompareOperator greaterOperator, List<Parameter> greaterParameters) {
        long betweenRange = probability.multiply(BigDecimal.valueOf(range)).longValue();
        long start = ThreadLocalRandom.current().nextLong(range - betweenRange);
        long end = start + betweenRange;
        long parameterStart = start - (greaterOperator == CompareOperator.GE ? 0 : 1);
        long parameterEnd = end + (lessOperator == CompareOperator.LE ? 0 : 1);
        lessParameters.forEach(parameter -> {
            parameter.setData(parameterStart);
            parameter.setDataValue(transferDataToValue(parameterStart));
        });
        greaterParameters.forEach(parameter -> {
            parameter.setData(parameterEnd);
            parameter.setDataValue(transferDataToValue(parameterEnd));
        });
    }


    /**
     * 在column中维护数据
     *
     * @param size column内部需要维护的数据大小
     */
    public void prepareTupleData(int size) {
        columnData = new long[size];
        int nullSize = (int) (size * nullPercentage);
        int sizeWithoutNull = size - nullSize;
        //使用Long.MIN_VALUE来代指null;
        Arrays.fill(columnData, 0, nullSize, Long.MIN_VALUE);
        int currentIndex = nullSize;
        List<Long> boundParaData = new ArrayList<>();
        for (Parameter parameter : boundPara) {
            long paraData = parameter.getData();
            if (paraData2Probability.containsKey(paraData)) {
                boundParaData.add(paraData);
                BigDecimal paraProbability = paraData2Probability.get(paraData);
                int eqSize = paraProbability.multiply(BigDecimal.valueOf(size)).setScale(0, RoundingMode.HALF_UP).intValue();
                Arrays.fill(columnData, currentIndex, currentIndex += eqSize, paraData);
            }
        }
        long lastParaData = 0;
        for (Map.Entry<Long, BigDecimal> data2Probability : paraData2Probability.entrySet()) {
            long currentParaData = data2Probability.getKey();
            if (!boundParaData.contains(currentParaData)) {
                BigDecimal currentPb = data2Probability.getValue();
                int randomSize = BigDecimal.valueOf(size).multiply(currentPb).setScale(0, RoundingMode.HALF_UP).intValue();
                if (currentParaData - lastParaData == 1) {
                    Arrays.fill(columnData, currentIndex, currentIndex += randomSize, currentParaData);
                } else {
                    long[] randomData;
                    try {
                        randomData = ThreadLocalRandom.current().longs(randomSize, lastParaData + 1, currentParaData).toArray();
                        System.arraycopy(randomData, 0, columnData, currentIndex, randomSize);
                        currentIndex += randomSize;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            lastParaData = currentParaData;
        }
        columnData2ComputeData = false;
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
        if (!columnData2ComputeData) {
            computeData = switch (columnType) {
                case DATE, DATETIME -> Arrays.stream(columnData).parallel().mapToDouble(data -> (double) data + min).toArray();
                case DECIMAL -> Arrays.stream(columnData).parallel().mapToDouble(data -> (double) (data + min) / specialValue).toArray();
                case INTEGER -> Arrays.stream(columnData).parallel().mapToDouble(data -> (double) (specialValue * data) + min).toArray();
                default -> throw new IllegalStateException("Unexpected value: " + columnType);
            };
            columnData2ComputeData = true;
        }
        return computeData;
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


    public List<Parameter> getBoundPara() {
        return boundPara;
    }

    public void setBoundPara(List<Parameter> boundPara) {
        this.boundPara = boundPara;
    }

    public void addBoundPara(List<Parameter> parameter) {
        this.boundPara.addAll(parameter);
    }

    public SortedMap<Long, BigDecimal> getParaData2Probability() {
        return paraData2Probability;
    }

    public void setParaData2Probability(SortedMap<Long, BigDecimal> paraData2Probability) {
        this.paraData2Probability = paraData2Probability;
    }

}
