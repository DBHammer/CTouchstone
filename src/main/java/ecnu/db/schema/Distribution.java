package ecnu.db.schema;

import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class Distribution {

    private final Logger logger = LoggerFactory.getLogger(Distribution.class);
    private final HashSet<Integer> likeParameterId = new HashSet<>();

    // bound PV的偏移位置
    private SortedMap<BigDecimal, Long> offset2Pv = new TreeMap<>();


    private final NavigableMap<BigDecimal, List<Parameter>> pvAndPbList = new TreeMap<>();

    private SortedMap<Long, BigDecimal> paraData2Probability = new TreeMap<>();

    private final long range;

    public Distribution(BigDecimal nullPercentage, long range) {
        this.range = range;
        // 初始化pvAndPblist 插入pve
        if (nullPercentage.compareTo(BigDecimal.ONE) < 0) {
            Parameter pve = new Parameter();
            pve.setData(range);
            pvAndPbList.put(BigDecimal.ONE.subtract(nullPercentage), new ArrayList<>(List.of(pve)));
            paraData2Probability.put(range, BigDecimal.ONE.subtract(nullPercentage));
        }
    }

    private void dealZeroPb(List<Parameter> parameters) {
        for (Parameter parameter : parameters) {
            parameter.setData(-1);
        }
    }

    public BigDecimal getOffset(long dataIndex) {
        if (offset2Pv.isEmpty()) {
            return BigDecimal.ZERO;
        }
        for (Map.Entry<BigDecimal, Long> off2Pv : offset2Pv.entrySet()) {
            if (off2Pv.getValue() == dataIndex) {
                return off2Pv.getKey();
            }
        }
        Long lastPv = offset2Pv.get(offset2Pv.lastKey());
        BigDecimal lastRange = paraData2Probability.get(lastPv);
        return offset2Pv.lastKey().add(lastRange);
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
            adjustNullPercentage(probability, parameters);
            pvAndPbList.putIfAbsent(probability, new ArrayList<>());
            pvAndPbList.get(probability).addAll(parameters);
        } else {
            long dataIndex = probability.compareTo(BigDecimal.ZERO) <= 0 ? -1 : range + 1;
            for (Parameter parameter : parameters) {
                parameter.setData(dataIndex);
            }
        }
    }

    public BigDecimal reusePb(List<Parameter> tempParameterList, List<BigDecimal> hasUsedEqRange, BigDecimal probability) {
        TreeMap<BigDecimal, BigDecimal> rangePb2CDFBound = new TreeMap<>();
        for (var currentCDF2Pb : pvAndPbList.entrySet()) {
            if (!isNonEqualRange(currentCDF2Pb.getValue())) {
                rangePb2CDFBound.put(getRange(currentCDF2Pb.getKey()), currentCDF2Pb.getKey());
            }
        }
        // todo 使用CPModel求解
        while (rangePb2CDFBound.size() > 0) {
            BigDecimal assignRange = rangePb2CDFBound.firstKey();
            BigDecimal cdfBound = rangePb2CDFBound.remove(assignRange);
            // 如果只剩下最后一个无法复用的range，则退出
            boolean canNotAssign = probability.compareTo(assignRange) < 0;
            boolean withoutRemain = tempParameterList.size() == 1 && probability.compareTo(assignRange) > 0;
            if (canNotAssign || withoutRemain) {
                break;
            }
            probability = probability.subtract(assignRange);
            pvAndPbList.get(cdfBound).add(tempParameterList.remove(0));
            hasUsedEqRange.add(cdfBound);
        }
        return probability;
    }

    /**
     * 如果合法请求概率超过了能提供的有效概率，则降低null的概率
     */
    private void adjustNullPercentage(BigDecimal probability, List<Parameter> parameters) {
        BigDecimal validCDFRange = pvAndPbList.lastKey();
        if (probability.compareTo(validCDFRange) > 0) {
            pvAndPbList.get(validCDFRange).removeIf(parameter -> parameter.getId() == -1);
            if (pvAndPbList.get(validCDFRange).isEmpty()) {
                pvAndPbList.remove(validCDFRange);
            }
            BigDecimal error = probability.subtract(validCDFRange);
            pvAndPbList.put(probability, new ArrayList<>(List.of(new Parameter(-1, null, null))));
            logger.error("参数{}请求range超过有效的CDF空间, 增加非Null概率，幅度为{}",
                    parameters.stream().mapToInt(Parameter::getId).boxed().toList(), error);
        }
    }

    private void insertEqualProbability(BigDecimal probability, List<Parameter> parameters) throws TouchstoneException {
        adjustNullPercentage(probability, parameters);
        List<Parameter> tempParameterList = new LinkedList<>(parameters);
        List<BigDecimal> hasUsedEqRange = new LinkedList<>();
        probability = reusePb(tempParameterList, hasUsedEqRange, probability);
        if (probability.compareTo(BigDecimal.ZERO) == 0) {
            dealZeroPb(tempParameterList);
            return;
        }
        // 标记该参数为等值的参数
        tempParameterList.forEach(parameter -> parameter.setEqualPredicate(true));
        BigDecimal minRange = BigDecimal.TEN;
        BigDecimal minCDF = BigDecimal.ZERO;
        for (var currentCDF : pvAndPbList.entrySet()) {
            BigDecimal currentRange = getRange(currentCDF.getKey());
            // 当前空间可以被完全利用
            if (currentRange.compareTo(probability) == 0 && !hasUsedEqRange.contains(currentCDF.getKey())) {
                currentCDF.getValue().addAll(tempParameterList);
                return;
            } else {
                // 找到可以放置的最小空间
                boolean enoughCapacity = currentRange.compareTo(probability) > 0;
                boolean isMinRange = minRange.compareTo(currentRange) > 0;
                // 如果cdf中包含等值的参数 则该range为一个等值的range，不可分割
                boolean isNonEqualRange = isNonEqualRange(currentCDF.getValue());
                if (enoughCapacity && isMinRange && isNonEqualRange) {
                    minRange = currentRange;
                    minCDF = currentCDF.getKey();
                }
            }
        }
        // 如果没有找到
        if (minCDF.compareTo(BigDecimal.ZERO) == 0) {
            throw new TouchstoneException("不存在可以放置的range");
        } else {
            BigDecimal remainCapacity = minRange.subtract(probability);
            BigDecimal eqCDf = minCDF.subtract(remainCapacity);
            pvAndPbList.put(eqCDf, new LinkedList<>(tempParameterList));
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

    /**
     * @return 增加的基数
     */
    public long initAllParameters() {
        long addCardinality = 0;
        if (range <= 0) {
            return addCardinality;
        }
        long remainCardinality = range - pvAndPbList.size();
        if (remainCardinality < 0) {
            addCardinality = -remainCardinality;
            remainCardinality = 0;
        }
        BigDecimal remainRange = pvAndPbList.entrySet().stream()
                // 找到所有的非等值的range右边界
                .filter(e -> isNonEqualRange(e.getValue()))
                // 确定其range大小
                .map(e -> getRange(e.getKey()))
                // 计算range的和
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal cardinalityPercentage;
        if (remainRange.compareTo(BigDecimal.ZERO) > 0) {
            cardinalityPercentage = BigDecimal.valueOf(remainCardinality).divide(remainRange, CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);
        } else {
            cardinalityPercentage = BigDecimal.ZERO;
        }
        long dataIndex = 0;
        paraData2Probability.clear();
        for (var CDF2Parameters : pvAndPbList.entrySet()) {
            dataIndex++;
            BigDecimal rangeSize = getRange(CDF2Parameters.getKey());
            if (isNonEqualRange(CDF2Parameters.getValue())) {
                dataIndex += cardinalityPercentage.multiply(rangeSize).setScale(0, RoundingMode.HALF_UP).intValue();
            }
            paraData2Probability.put(dataIndex, rangeSize);
            for (Parameter parameter : CDF2Parameters.getValue()) {
                parameter.setData(dataIndex);
            }
        }
        return addCardinality;
    }

    public void applyUniVarConstraint(BigDecimal probability, CompareOperator operator, List<Parameter> parameters) throws TouchstoneException {
        switch (operator) {
            case NE, NOT_LIKE, NOT_IN -> insertEqualProbability(BigDecimal.ONE.subtract(probability), parameters);
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
     * 在column中维护数据
     *
     * @param size column内部需要维护的数据大小
     */
    public long[] prepareTupleData(int size) {
        // 存储符合CDF的属性值
        List<Long> attributeData = new ArrayList<>(size);
        BigDecimal bSize = BigDecimal.valueOf(size);
        // 生成为左开右闭，因此lastParaData始终比上一右边界大
        long lastParaData = 1;
        List<Long> allBoundPvs = offset2Pv.values().stream().toList();
        for (Map.Entry<Long, BigDecimal> data2Probability : paraData2Probability.entrySet()) {
            long currentParaData = data2Probability.getKey();
            if (!allBoundPvs.contains(currentParaData)) {
                BigDecimal currentPb = data2Probability.getValue();
                int generateSize = bSize.multiply(currentPb).setScale(0, RoundingMode.HALF_UP).intValue();
                List<Long> rangeData;
                if (currentParaData == lastParaData) {
                    rangeData = Collections.nCopies(generateSize, currentParaData);
                } else {
                    rangeData = ThreadLocalRandom.current().longs(generateSize, lastParaData, currentParaData + 1).boxed().toList();
                }
                attributeData.addAll(rangeData);
            }
            // 移动到右边界+1
            lastParaData = currentParaData + 1;
        }
        // 将属性值与bound值组合
        int currentIndex = 0;
        int attributeIndex = 0;
        long[] columnData = new long[size];
        for (Map.Entry<BigDecimal, Long> pv2Offset : offset2Pv.entrySet()) {
            // 确定bound的开始offset
            int offset = bSize.multiply(pv2Offset.getKey()).setScale(0, RoundingMode.HALF_UP).intValue();
            while (currentIndex < currentIndex + offset) {
                columnData[currentIndex++] = attributeData.get(attributeIndex++);
            }
            // 确定bound的大小
            BigDecimal rangeProbability = paraData2Probability.get(pv2Offset.getValue());
            int generateSize = bSize.multiply(rangeProbability).setScale(0, RoundingMode.HALF_UP).intValue();
            Arrays.fill(columnData, currentIndex, currentIndex + generateSize, pv2Offset.getValue());
            currentIndex += generateSize;
        }
        // 概率误差可能会导致attributeData略微大于size
        while (attributeData.size() > size) {
            attributeData.remove(attributeData.size() - 1);
        }
        // 复制最后部分
        for (int i = attributeIndex; i < attributeData.size(); i++) {
            columnData[currentIndex++] = attributeData.get(i);
        }
        // 使用Long.MIN_VALUE标记结尾的null值
        if (currentIndex < size - 1) {
            Arrays.fill(columnData, currentIndex, size - 1, Long.MIN_VALUE);
        }
        return columnData;
    }

    public SortedMap<BigDecimal, Long> getOffset2Pv() {
        return offset2Pv;
    }

    public void setOffset2Pv(SortedMap<BigDecimal, Long> offset2Pv) {
        this.offset2Pv = offset2Pv;
    }

    public SortedMap<Long, BigDecimal> getParaData2Probability() {
        return paraData2Probability;
    }

    public void setParaData2Probability(SortedMap<Long, BigDecimal> paraData2Probability) {
        this.paraData2Probability = paraData2Probability;
    }
}
