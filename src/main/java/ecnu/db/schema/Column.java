package ecnu.db.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * @author wangqingshuai
 */
@JsonPropertyOrder({"columnType", "nullPercentage", "specialValue", "min", "range", "minLength", "rangeLength"})
public class Column {
    @JsonIgnore
    private final TreeMap<BigDecimal, List<Parameter>> eqRequest2ParameterIds = new TreeMap<>();
    @JsonIgnore
    private final HashSet<Integer> likeParameterId = new HashSet<>();
    private ColumnType columnType;
    private long min;

    public String getOriginalType() {
        return originalType;
    }

    public void setOriginalType(String originalType) {
        this.originalType = originalType;
    }

    private String originalType;
    private long range;
    private long specialValue;
    private float nullPercentage;
    private int minLength;
    private int rangeLength;
    @JsonIgnore
    private BigDecimal decimalPre;
    @JsonIgnore
    private List<Map.Entry<Long, BigDecimal>> bucketBound2FreeSpace = new LinkedList<>();
    @JsonIgnore
    private Map<Long, BigDecimal> eqConstraint2Probability = new HashMap<>();
    @JsonIgnore
    private List<Parameter> boundPara = new ArrayList<>();
    @JsonIgnore
    private StringTemplate stringTemplate;
    @JsonIgnore
    private boolean hasBetweenConstraint = false;
    @JsonIgnore
    private long[] columnData;
    @JsonIgnore
    private boolean columnData2ComputeData = false;
    @JsonIgnore
    private double[] computeData;

    public Column() {
    }

    public Column(ColumnType columnType) {
        this.columnType = columnType;
    }

    public void initStringTemplate() {
        stringTemplate = new StringTemplate(minLength, rangeLength, specialValue);
    }

    public void initBucketBound2FreeSpace() {
        bucketBound2FreeSpace.add(new AbstractMap.SimpleEntry<>(range, BigDecimal.ONE));
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
        long bound;
        if (operator == CompareOperator.GE || operator == CompareOperator.GT) {
            probability = BigDecimal.ONE.subtract(probability);
        }
        bound = switch (operator) {
            case LT, GE -> probability.multiply(BigDecimal.valueOf(range)).setScale(0, RoundingMode.HALF_UP).longValue();
            case GT, LE -> probability.multiply(BigDecimal.valueOf(range)).longValue();
            default -> throw new UnsupportedOperationException();
        };
        if (probability.compareTo(BigDecimal.ONE) < 0 && probability.compareTo(BigDecimal.ZERO) > 0) {
            long finalBound = bound;
            if (bucketBound2FreeSpace.stream().noneMatch(bucket -> bucket.getKey().equals(finalBound))) {
                this.bucketBound2FreeSpace.add(new AbstractMap.SimpleEntry<>(bound, probability));
            } else {
                do {
                    long finalBound1 = bound;
                    var range = bucketBound2FreeSpace.stream().filter(bucket -> bucket.getKey().equals(finalBound1)).findFirst();
                    if (range.isPresent()) {
                        var realRange = range.get();
                        if (realRange.getValue().compareTo(probability) == 0) {
                            break;
                        } else {
                            bound--;
                        }
                    } else {
                        this.bucketBound2FreeSpace.add(new AbstractMap.SimpleEntry<>(bound, probability));
                        break;
                    }
                } while (true);
            }
        }
        long finalBound = bound;
        parameters.parallelStream().forEach(parameter -> {
            long value = finalBound;
            if (operator == CompareOperator.GT || operator == CompareOperator.LE) {
                value--;
            }
            parameter.setData(value);
            parameter.setDataValue(transferDataToValue(value));
        });

    }

    private void insertEqualProbability(BigDecimal probability, List<Parameter> parameters) {
        if (probability.compareTo(BigDecimal.ZERO) == 0) {
            dealZeroPb(parameters);
        } else {
            BigDecimal tempProbability = new BigDecimal(probability.toString()).divide(BigDecimal.valueOf(parameters.size()), CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);
            eqRequest2ParameterIds.computeIfAbsent(tempProbability, i -> new LinkedList<>()).addAll(parameters);
        }
    }

    public void insertUniVarProbability(BigDecimal probability, CompareOperator operator, List<Parameter> parameters) {
        switch (operator) {
            case NE, NOT_LIKE, NOT_IN -> eqRequest2ParameterIds.computeIfAbsent(BigDecimal.ONE.subtract(probability), i -> new LinkedList<>()).add(parameters.get(0));
            case EQ, LIKE -> eqRequest2ParameterIds.computeIfAbsent(probability, i -> new LinkedList<>()).add(parameters.get(0));
            case IN -> insertEqualProbability(probability, parameters);
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
        hasBetweenConstraint = true;
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

    public void initEqParameter() {
        if (eqRequest2ParameterIds.isEmpty()) {
            return;
        }
        if (hasBetweenConstraint) {
            throw new UnsupportedOperationException("当前不支持为存在between约束的列分配等值");
        }
        //初始化每个bucket的剩余容量和等值容量
        Map<Long, AtomicInteger> bucketId2EqNum = new HashMap<>();
        bucketBound2FreeSpace.sort(Map.Entry.comparingByKey());
        BigDecimal lastBound = BigDecimal.ZERO;
        for (Map.Entry<Long, BigDecimal> currentBound : bucketBound2FreeSpace) {
            currentBound.setValue(currentBound.getValue().subtract(lastBound));
            bucketId2EqNum.put(currentBound.getKey(), new AtomicInteger(0));
        }
        // 将等值的概率请求从大到小排序，首先为请求最大的安排空间，每次选择能放下的bucket中最小的。
        // 填充到bucket之后，重新调整剩余容量的记录treemap
        // 针对等值约束的赋值，采用逆序赋值法，即从bound开始，按照当前在bucket中的位置，分配对应的值，赋值最小从lowBound-1开始
        for (BigDecimal eqProbability : eqRequest2ParameterIds.descendingKeySet()) {
            while (!eqRequest2ParameterIds.get(eqProbability).isEmpty()) {
                bucketBound2FreeSpace.sort(Map.Entry.comparingByValue());
                Optional<Map.Entry<Long, BigDecimal>> freeSpace2BucketIdOptional = bucketBound2FreeSpace.stream().
                        filter(bucket -> bucket.getValue().compareTo(eqProbability) > -1).findFirst();
                if (freeSpace2BucketIdOptional.isPresent()) {
                    //重新调整freeSpace
                    Map.Entry<Long, BigDecimal> freeSpace2BucketId = freeSpace2BucketIdOptional.get();
                    Long bucketBound = freeSpace2BucketId.getKey();
                    bucketBound2FreeSpace.remove(freeSpace2BucketId);
                    bucketBound2FreeSpace.add(new AbstractMap.SimpleEntry<>(bucketBound, freeSpace2BucketId.getValue().subtract(eqProbability)));
                    //顺序赋值
                    long eqParameterData = bucketBound - bucketId2EqNum.get(bucketBound).incrementAndGet();
                    eqConstraint2Probability.put(eqParameterData, eqProbability);
                    Parameter parameter = eqRequest2ParameterIds.get(eqProbability).remove(0);
                    String tempValue = parameter.getDataValue();
                    parameter.setData(eqParameterData);
                    String newValue;
                    if (likeParameterId.contains(parameter.getId())) {
                        newValue = stringTemplate.getLikeValue(eqParameterData, parameter.getDataValue());
                    } else {
                        newValue = transferDataToValue(eqParameterData);
                    }
                    parameter.setDataValue(newValue);
                    Iterator<Parameter> parameterIterator = eqRequest2ParameterIds.get(eqProbability).iterator();
                    while (parameterIterator.hasNext()) {
                        Parameter parameter1 = parameterIterator.next();
                        if (parameter1.getDataValue().equals(tempValue)) {
                            parameter1.setData(eqParameterData);
                            parameter1.setDataValue(newValue);
                            parameterIterator.remove();
                        }
                    }
                } else {
                    throw new UnsupportedOperationException("等值约束冲突，无法实例化");
                }
            }
        }

        var bucketId2CardinalityList = bucketId2EqNum.entrySet().stream().filter(e -> e.getValue().get() > 0).toList();
        List<Map.Entry<Long, BigDecimal>> newbucketBound2FreeSpace = new ArrayList<>();
        for (var bucketId2Cardinality : bucketId2CardinalityList) {
            var spaceIterator = bucketBound2FreeSpace.iterator();
            while (spaceIterator.hasNext()) {
                var space = spaceIterator.next();
                if (space.getKey().equals(bucketId2Cardinality.getKey())) {
                    spaceIterator.remove();
                    long newSpaceBound = space.getKey() - bucketId2Cardinality.getValue().get();
                    newbucketBound2FreeSpace.add(new AbstractMap.SimpleEntry<>(newSpaceBound, space.getValue()));
                }
            }
        }
        bucketBound2FreeSpace.addAll(newbucketBound2FreeSpace);
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
        // 使用Long.MIN_VALUE来代指null
        Arrays.fill(columnData, 0, nullSize, Long.MIN_VALUE);
        int currentIndex = nullSize;
        Map<Long, BigDecimal> newEqConstraint2Probability = eqConstraint2Probability;
        if (!boundPara.isEmpty()) {
            newEqConstraint2Probability = new HashMap<>(eqConstraint2Probability);
            for (Parameter parameter : boundPara) {
                long paraData = parameter.getData();
                BigDecimal paraProbability = newEqConstraint2Probability.get(paraData);
                int eqSize = paraProbability.multiply(BigDecimal.valueOf(sizeWithoutNull)).setScale(0, RoundingMode.HALF_UP).intValue();
                Arrays.fill(columnData, currentIndex, currentIndex += eqSize, paraData);
                newEqConstraint2Probability.remove(paraData);
            }
        }
        if (newEqConstraint2Probability.size() > 0) {
            for (Map.Entry<Long, BigDecimal> entry : newEqConstraint2Probability.entrySet()) {
                int eqSize = entry.getValue().multiply(BigDecimal.valueOf(sizeWithoutNull)).setScale(0, RoundingMode.HALF_UP).intValue();
                Arrays.fill(columnData, currentIndex, currentIndex += eqSize, entry.getKey());
                nullSize += eqSize;
            }
        }
        //赋值随机数据
        int i = 0;
        long lastBound = 0;
        while (eqConstraint2Probability.containsKey(lastBound)){
            lastBound++;
        }
        bucketBound2FreeSpace.sort(Map.Entry.comparingByKey());
        for (Map.Entry<Long, BigDecimal> bucket2Probability : bucketBound2FreeSpace) {
            int randomSize;
            if (++i == bucketBound2FreeSpace.size()) {
                randomSize = size - currentIndex;
            } else {
                //todo 确定左边界
                randomSize = BigDecimal.valueOf(sizeWithoutNull).multiply(bucket2Probability.getValue()).setScale(0, RoundingMode.HALF_UP).intValue() - (currentIndex - nullSize);
            }
            try {
                //todo
                long[] randomData;
                long bound = bucket2Probability.getKey();
                if (bound == lastBound) {
                    throw new UnsupportedOperationException();
                } else {
                    randomData = ThreadLocalRandom.current().longs(randomSize, lastBound, bound).toArray();
                }
                lastBound = bound;
                while (eqConstraint2Probability.containsKey(lastBound)){
                    lastBound++;
                }
                System.arraycopy(randomData, 0, columnData, currentIndex, randomSize);
            } catch (Exception e) {
                e.printStackTrace();
            }
            currentIndex += randomSize;
        }

//        // shuffle数组
//        long temp;
//        int swapIndex;
//        Random rnd = ThreadLocalRandom.current();
//
//        for (int index = columnData.length - 1; index > boundIndex; index--) {
//            swapIndex = rnd.nextInt(index - boundIndex) + boundIndex;
//            temp = columnData[swapIndex];
//            columnData[swapIndex] = columnData[index];
//            columnData[index] = temp;
//        }
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
            case VARCHAR -> stringTemplate.transferColumnData2Value(data, bucketBound2FreeSpace.size() > 1, range);
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

    public List<Map.Entry<Long, BigDecimal>> getBucketBound2FreeSpace() {
        return bucketBound2FreeSpace;
    }

    public void setBucketBound2FreeSpace(List<Map.Entry<Long, BigDecimal>> bucketBound2FreeSpace) {
        this.bucketBound2FreeSpace = bucketBound2FreeSpace;
    }

    public Map<Long, BigDecimal> getEqConstraint2Probability() {
        return eqConstraint2Probability;
    }

    public void setEqConstraint2Probability(Map<Long, BigDecimal> eqConstraint2Probability) {
        this.eqConstraint2Probability = eqConstraint2Probability;
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
}
