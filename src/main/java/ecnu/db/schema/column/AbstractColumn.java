package ecnu.db.schema.column;


import ch.obermuhlner.math.big.BigDecimalMath;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.operation.CompareOperator;
import ecnu.db.exception.InstantiateParameterException;
import ecnu.db.schema.column.bucket.EqBucket;
import ecnu.db.schema.column.bucket.NonEqBucket;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;

import static ecnu.db.constraintchain.filter.operation.CompareOperator.EQ;
import static ecnu.db.constraintchain.filter.operation.CompareOperator.LT;
import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.GREATER;
import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.LESS;
import static ecnu.db.schema.column.ColumnType.*;
import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;


/**
 * @author qingshuai.wang
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AbstractColumn {
    private final ColumnType columnType;
    protected float nullPercentage;
    protected String columnName;
    // 非等值约束
    protected NonEqBucket bucket;
    // 已经处理过的约束
    protected Multimap<String, Parameter> metConditions = ArrayListMultimap.create();
    // 已经处理过的等值参数对应的概率, data->probability
    protected Map<String, BigDecimal> eqParamData2Probability = new HashMap<>();
    // 所有的非等值约束划分而成的等值约束的区间
    @JsonIgnore
    protected List<EqBucket> eqBuckets = new ArrayList<>();
    // 生成的等于约束参数数据
    protected Set<String> eqCandidates = new HashSet<>();

    public AbstractColumn(String columnName, ColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
        bucket = new NonEqBucket();
    }

    /**
     * 获取该列非重复值的个数
     *
     * @return 非重复值的个数
     */
    public abstract int getNdv();

    @JsonGetter
    @SuppressWarnings("unused")
    public float getNullPercentage() {
        return nullPercentage;
    }

    @JsonSetter
    public void setNullPercentage(float nullPercentage) {
        this.nullPercentage = nullPercentage;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public List<EqBucket> getEqBuckets() {
        return eqBuckets;
    }

    /**
     * 插入非等值约束概率，按照lt来计算
     *
     * @param probability 非等值约束概率
     * @param operator    操作符
     * @param parameter   参数
     */
    public void insertNonEqProbability(BigDecimal probability, CompareOperator operator, Parameter parameter) {
        // 非等值比较概率无需记录重复的parameter
        if (metConditions.containsKey(operator + parameter.getData())) {
            return;
        }
        metConditions.put(operator + parameter.getData(), parameter);
        if (operator.getType() != LESS && operator.getType() != GREATER) {
            throw new UnsupportedOperationException();
        }
        NonEqBucket bucket = this.bucket.search(probability);
        bucket.value = parameter.getData();
        bucket.probability = probability;
        bucket.leftBucket = new NonEqBucket();
        bucket.rightBucket = new NonEqBucket();
        if (bucket.child != null) {
            BigDecimal toFillCapacity = probability.subtract(bucket.child.leftBorder);
            BigDecimal occupiedCapacity = bucket.child.rightBorder.subtract(bucket.child.leftBorder).subtract(bucket.child.capacity);
            if (bucket.child.capacity.compareTo(toFillCapacity) < 0 && occupiedCapacity.compareTo(toFillCapacity) > 0) {
                throw new UnsupportedOperationException();
            }
            if (bucket.child.capacity.compareTo(toFillCapacity) >= 0) {
                bucket.leftBucket.child = new EqBucket(bucket, toFillCapacity, bucket.child.leftBorder, probability);
                bucket.rightBucket.child = bucket.child;
                bucket.rightBucket.child.capacity = bucket.rightBucket.child.capacity.subtract(toFillCapacity);
                bucket.rightBucket.child.leftBorder = probability;
                bucket.child = null;
                eqBuckets.add(bucket.leftBucket.child);
            } else if (occupiedCapacity.compareTo(toFillCapacity) <= 0) {
                bucket.rightBucket.child = new EqBucket(bucket, bucket.child.capacity.subtract(toFillCapacity.subtract(occupiedCapacity)), bucket.child.leftBorder, probability);
                bucket.leftBucket.child = bucket.child;
                bucket.leftBucket.child.capacity = bucket.child.capacity.subtract(toFillCapacity.subtract(occupiedCapacity));
                bucket.leftBucket.child.rightBorder = probability;
                bucket.child = null;
                eqBuckets.add(bucket.rightBucket.child);
            }
        }
    }

    public void insertInProbability(BigDecimal probability, List<Parameter> parameters) throws InstantiateParameterException {
        List<Parameter> notMetParams = new LinkedList<>();
        for (Parameter parameter : parameters) {
            if (metConditions.containsKey(EQ + parameter.getData())) {
                // 等值的比较需要记录parameter
                metConditions.put(EQ + parameter.getData(), parameter);
                BigDecimal eqProbability = eqParamData2Probability.get(EQ + parameter.getData());
                if (probability.compareTo(eqProbability) < 0) {
                    throw new InstantiateParameterException(String.format("'%s'的in条件与其他eq或in条件存在矛盾", parameters));
                } else {
                    probability = probability.subtract(eqProbability);
                }
            } else {
                notMetParams.add(parameter);
            }
        }
        if (notMetParams.size() == 0) {
            throw new InstantiateParameterException(String.format("'%s'的in条件与其他eq或in条件存在矛盾", parameters));
        }
        BigDecimal size = BigDecimal.valueOf(notMetParams.size());
        probability = BigDecimalMath.pow(probability, BigDecimal.ONE.divide(size, BIG_DECIMAL_DEFAULT_PRECISION), BIG_DECIMAL_DEFAULT_PRECISION);
        for (Parameter parameter : notMetParams) {
            insertEqualProbability(probability, parameter);
        }
    }

    public void insertEqualProbability(BigDecimal probability, Parameter parameter) throws InstantiateParameterException {
        if (metConditions.containsKey(EQ + parameter.getData())) {
            // 等值的比较需要记录parameter
            metConditions.put(EQ + parameter.getData(), parameter);
            if (eqParamData2Probability.get(EQ + parameter.getData()).compareTo(probability) != 0) {
                throw new InstantiateParameterException(String.format("'%s'的eq条件存在矛盾, '%s'不等于'%s'",
                        parameter, eqParamData2Probability.get(EQ + parameter.getData()), probability));
            }
            return;
        }
        metConditions.put(EQ + parameter.getData(), parameter);
        eqParamData2Probability.put(EQ + parameter.getData(), probability);
        EqBucket eqBucket = fitProbability(probability);
        if (eqBucket.capacity.compareTo(probability) >= 0) {
            eqBucket.eqConditions.put(probability, parameter);
            eqBucket.capacity = eqBucket.capacity.subtract(probability);
        } else {
            eqBucket.eqConditions.put(eqBucket.capacity, parameter);
            eqBucket.capacity = BigDecimal.ZERO;
        }
        eqBuckets.sort(Comparator.comparing(o -> o.capacity));
    }

    /**
     * 从non-eq bucket划分的区间里生成eq-bucket
     */
    public void initEqProbabilityBucket() {
        Deque<Pair<Pair<BigDecimal, BigDecimal>, NonEqBucket>> buckets = new ArrayDeque<>();
        buckets.push(Pair.of(Pair.of(BigDecimal.ZERO, BigDecimal.ONE), bucket));
        while (!buckets.isEmpty()) {
            Pair<Pair<BigDecimal, BigDecimal>, NonEqBucket> pair = buckets.pop();
            NonEqBucket bucket = pair.getRight();
            BigDecimal min = pair.getLeft().getLeft(), max = pair.getLeft().getRight();
            if (bucket.leftBucket == null && bucket.rightBucket == null) {
                EqBucket eqBucket = new EqBucket();
                eqBucket.parent = bucket;
                eqBucket.capacity = max.subtract(min);
                eqBucket.leftBorder = min;
                eqBucket.rightBorder = max;
                bucket.child = eqBucket;
                eqBuckets.add(eqBucket);
            }
            if (bucket.leftBucket != null) {
                buckets.push(Pair.of(Pair.of(min, bucket.probability), bucket.leftBucket));
            }
            if (bucket.rightBucket != null) {
                buckets.push(Pair.of(Pair.of(bucket.probability, max), bucket.rightBucket));
            }
        }
        eqBuckets.sort(Comparator.comparing(b -> b.capacity));
    }

    public void initEqParameter() {
        eqBuckets.sort(Comparator.comparing(b -> b.leftBorder));
        for (EqBucket eqBucket : eqBuckets) {
            eqBucket.eqConditions.forEach((b, param) -> {
                String data = generateEqData(eqBucket.leftBorder, eqBucket.rightBorder);
                metConditions.get(EQ + param.getData()).forEach((p) -> p.setData(data));
            });
        }
    }

    private EqBucket fitProbability(BigDecimal probability) {
        int low = 0, high = eqBuckets.size() - 1;
        while (low < high) {
            EqBucket bucket = eqBuckets.get((low + high) / 2);
            if (bucket.capacity.compareTo(probability) > 0) {
                high = (low + high) / 2;
            } else if (bucket.capacity.compareTo(probability) < 0) {
                low = (low + 1 + high) / 2;
            } else {
                return eqBuckets.get((low + high) / 2);
            }
        }
        return eqBuckets.get(low);
    }

    /**
     * 生成等值参数的数据
     *
     * @param minProbability 等值参数可以出现的概率区间的左边界
     * @param maxProbability 等值参数可以出现的概率区间的右边界
     * @return 生成的数据
     */
    protected abstract String generateEqData(BigDecimal minProbability, BigDecimal maxProbability);

    public boolean hasNotMetCondition(String condition) {
        return !metConditions.containsKey(condition);
    }

    /**
     * 插入between的概率
     * 目前的实现方式:
     * 仅在eq bucket里还没有被使用的capacity里插入，即假设插入between a and b，那么结果应该如
     * *****************************************************
     * *       a        between         b                  *
     * *       __________________________                  *
     * *      |                         |                  *
     * *   [capacity|eq1|eq2|...]...[capacity|eq1|eq2|...] *
     * *****************************************************
     * 也就是说，我们目前不会考虑a处在eq1和eq2之间这样的情况，或者处在capacity与eq边界的情况。
     * 同时我们假设capacity在between插入前是一体的，不存在形如[capacity1|eq1|capacity2|...]的情况
     *
     * @param probability       between的概率
     * @param lessParameters    代表between的小于条件的参数
     * @param greaterParameters 代表between的大于条件的参数
     */
    public void insertBetweenProbability(BigDecimal probability, CompareOperator lessOperator, List<Parameter> lessParameters, CompareOperator greaterOperator, List<Parameter> greaterParameters) {
        BigDecimal minDeviation = BigDecimal.ONE, deviation;
        eqBuckets.sort(Comparator.comparing(b -> b.leftBorder));
        int i, minIndex = 0;
        for (i = 0; i < eqBuckets.size(); i++) {
            EqBucket eqBucket = eqBuckets.get(i);
            BigDecimal rightBorder = eqBucket.leftBorder.add(probability);
            EqBucket rightBucket = fitProbability(rightBorder);
            // 判断右边的eq bucket是否有足够空间容下probability
            if (rightBucket.hasSpaceFor(probability)) {
                minIndex = i;
                break;
            } else {
                deviation = probability.subtract(rightBucket.leftBorder.add(rightBucket.capacity));
                if (deviation.compareTo(minDeviation) < 0) {
                    minDeviation = deviation;
                    minIndex = i;
                }
            }
        }
        EqBucket leftBucket = eqBuckets.get(minIndex);
        BigDecimal rightBorder = leftBucket.leftBorder.add(probability);
        EqBucket rightBucket = fitProbability(rightBorder);
        BigDecimal leftProbability, rightProbability;
        if (rightBucket.hasSpaceFor(probability)) {
            double leftBorderFreedom;
            // 如果左右是一个bucket，那么左边界的自由度就是这个bucket的capacity减去probability得到
            if (rightBucket == leftBucket) {
                leftBorderFreedom = leftBucket.capacity.subtract(probability).doubleValue();
            }
            // 正常情况下，取左右边界所在的bucket的capacity的较小的那一个
            else {
                leftBorderFreedom = Double.min(leftBucket.capacity.doubleValue(), rightBucket.capacity.doubleValue());
            }
            leftProbability = BigDecimal.valueOf((1 - Math.random()) * leftBorderFreedom);
            rightProbability = leftProbability.add(probability);
        } else {
            // 对于空间不足的情况，左右是否为一个bucket，没有区别
            leftProbability = leftBucket.leftBorder;
            rightProbability = rightBucket.leftBorder.add(rightBucket.capacity);
        }
        String leftData = genData(leftProbability), rightData = genData(rightProbability);
        lessParameters.forEach((p) -> p.setData(leftData));
        greaterParameters.forEach((p) -> p.setData(rightData));
        // todo 当前仅使用LT
        insertNonEqProbability(leftProbability, LT, lessParameters.get(0));
        insertNonEqProbability(rightProbability, LT, greaterParameters.get(0));
    }

    /**
     * 根据概率生成非等值filter的参数数据
     *
     * @param probability 分割概率
     * @return 生成的数据
     */
    public String genData(BigDecimal probability) {
        String ret;
        if (getColumnType() == INTEGER) {
            IntColumn column = (IntColumn) this;
            int value = column.generateData(probability);
            ret = Integer.toString(value);
        } else if (getColumnType() == DECIMAL) {
            DecimalColumn column = (DecimalColumn) this;
            BigDecimal value = column.generateData(probability);
            ret = value.toString();
        } else if (getColumnType() == DATETIME) {
            DateTimeColumn column = (DateTimeColumn) this;
            LocalDateTime newDateTime = column.generateData(probability);
            DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss");
            if (column.getPrecision() > 0) {
                builder.appendFraction(ChronoField.MICRO_OF_SECOND, 0, column.getPrecision(), true);
            }
            DateTimeFormatter formatter = builder.toFormatter();
            return formatter.format(newDateTime);
        } else if (getColumnType() == DATE) {
            DateColumn column = (DateColumn) this;
            LocalDate newDate = column.generateData(probability);
            DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter();
            return formatter.format(newDate);
        } else {
            throw new UnsupportedOperationException();
        }

        return ret;
    }
}
