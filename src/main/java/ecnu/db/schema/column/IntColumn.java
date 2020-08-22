package ecnu.db.schema.column;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.operation.CompareOperator;
import ecnu.db.schema.column.bucket.EqBucket;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author qingshuai.wang
 */
public class IntColumn extends AbstractColumn {
    private int min;
    private int max;
    private int ndv;
    private int[] tupleData;
    private double[] doubleCopyOfTupleData;

    public IntColumn() {
        super(null, ColumnType.INTEGER);
    }

    public IntColumn(String columnName) {
        super(columnName, ColumnType.INTEGER);
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    @Override
    public int getNdv() {
        return this.ndv;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }

    @Override
    protected String generateEqParamData(BigDecimal minProbability, BigDecimal maxProbability) {
        String data;
        double minP = minProbability.doubleValue(), maxP = maxProbability.doubleValue();
        do {
            // minPData = minP * (max - min) + min
            // maxPData = maxP * (max - min) + min
            // data = random() * (maxPData - minPData) + minPData = random() * (maxP - minP) * (max - min) + minP * (max - min) + min
            // 加入double转为int的修正, data = floor(random() * (maxP - minP) * (max - min + 1) + minP * (max - min) + min)
            data = Integer.toString((int) Math.floor(Math.random() * (maxP - minP) * (max - min + 1) + minP * (max - min) + min));
        } while (eqCandidates.contains(data));
        eqCandidates.add(data);
        return data;
    }

    @Override
    public String generateNonEqParamData(BigDecimal probability) {
        int value = BigDecimal.valueOf(getMax() - getMin()).multiply(probability).intValue() + getMin();
        return Integer.toString(value);
    }

    @Override
    public void prepareTupleData(int size) {
        eqBuckets.sort(Comparator.comparing(o -> o.leftBorder));
        if (tupleData == null || this.tupleData.length != size) {
            tupleData = new int[size];
        }
        int bound = max - min + 1;
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            tupleData[i] = (int) Math.floor((1 - rand.nextDouble()) * bound + min);
        }
        if (eqCandidates.size() > 0) {
            Arrays.sort(tupleData);
        }
        for (EqBucket eqBucket : eqBuckets) {
            int j = (int) (eqBucket.leftBorder.doubleValue() * size);
            for (Map.Entry<BigDecimal, Parameter> entry : eqBucket.eqConditions.entries()) {
                // new_start = ori_start + probability * size
                int newJ = (int) (j + entry.getKey().doubleValue() * size);
                int eqValue = Integer.parseInt(entry.getValue().getData());
                for (; j < newJ && j < size; j++) {
                    tupleData[j] = eqValue;
                }
                j = newJ;
            }
        }
        if (eqCandidates.size() > 0) {
            CommonUtils.shuffle(size, rand, tupleData);
        }
        if (doubleCopyOfTupleData == null || this.doubleCopyOfTupleData.length != size) {
            doubleCopyOfTupleData = new double[size];
        }
        for (int i = 0; i < size; i++) {
            doubleCopyOfTupleData[i] = tupleData[i];
        }
    }

    /**
     * for columnNode
     * @return 返回用于multi-var计算的一个double数组
     */
    public double[] calculate() {
        return Arrays.copyOf(doubleCopyOfTupleData, doubleCopyOfTupleData.length);
    }

    @Override
    public boolean[] evaluate(CompareOperator operator, List<Parameter> parameters, boolean hasNot) {
        boolean[] ret = new boolean[tupleData.length];
        switch (operator) {
            case EQ:
                int param = Integer.parseInt(parameters.get(0).getData());
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (tupleData[i] == param);
                }
                break;
            case NE:
                param = Integer.parseInt(parameters.get(0).getData());
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (tupleData[i] != param);
                }
                break;
            case LT:
                param = Integer.parseInt(parameters.get(0).getData());
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (tupleData[i] < param);
                }
                break;
            case LE:
                param = Integer.parseInt(parameters.get(0).getData());
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (tupleData[i] <= param);
                }
                break;
            case GT:
                param = Integer.parseInt(parameters.get(0).getData());
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (tupleData[i] > param);
                }
                break;
            case GE:
                param = Integer.parseInt(parameters.get(0).getData());
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (tupleData[i] >= param);
                }
                break;
            case IN:
                int[] params = new int[parameters.size()];
                for (int i = 0; i < parameters.size(); i++) {
                    params[i] = Integer.parseInt(parameters.get(0).getData());
                }
                if (hasNot) {
                    for (int i = 0; i < tupleData.length; i++) {
                        ret[i] = false;
                        for (int paramDatum : params) {
                            ret[i] = (ret[i] | (tupleData[i] == paramDatum));
                            ret[i] = !ret[i];
                        }
                    }
                } else {
                    for (int i = 0; i < tupleData.length; i++) {
                        ret[i] = false;
                        for (int paramDatum : params) {
                            ret[i] = (ret[i] | (tupleData[i] == paramDatum));
                        }
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return ret;
    }

    @JsonIgnore
    public int[] getTupleData() {
        return tupleData;
    }
}
