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
public class DecimalColumn extends AbstractColumn {
    double min;
    double max;
    double[] tupleData;

    public DecimalColumn() {
        super(null, ColumnType.DECIMAL);
    }

    public DecimalColumn(String columnName) {
        super(columnName, ColumnType.DECIMAL);
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public int getNdv() {
        return -1;
    }

    @Override
    protected String generateEqParamData(BigDecimal minProbability, BigDecimal maxProbability) {
        String data;
        double minP = minProbability.doubleValue(), maxP = maxProbability.doubleValue();
        do {
            data = Double.toString(BigDecimal.valueOf(Math.random() * (maxP - minP) + minP).doubleValue() * (max - min) + min);
        } while (eqCandidates.contains(data));
        eqCandidates.add(data);
        return data;
    }

    @Override
    public String generateNonEqParamData(BigDecimal probability) {
        BigDecimal value = BigDecimal.valueOf(getMax() - getMin()).multiply(probability).add(BigDecimal.valueOf(getMin()));
        return value.toString();
    }

    @Override
    public void prepareTupleData(int size) {
        eqBuckets.sort(Comparator.comparing(o -> o.leftBorder));
        BigDecimal cumBorder = BigDecimal.ZERO, sizeVal = BigDecimal.valueOf(size);
        if (tupleData == null || this.tupleData.length != size) {
            tupleData = new double[size];
        }
        for (EqBucket eqBucket : eqBuckets) {
            for (Map.Entry<BigDecimal, Parameter> entry : eqBucket.eqConditions.entries()) {
                BigDecimal newCum = cumBorder.add(entry.getKey().multiply(sizeVal));
                double eqValue = Double.parseDouble(entry.getValue().getData());
                for (int j = cumBorder.intValue(); j < newCum.intValue() && j < size; j++) {
                    tupleData[j] = eqValue;
                }
                cumBorder = newCum;
            }
        }
        double bound = max - min;
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for (int i = cumBorder.intValue(); i < size; i++) {
            tupleData[i] = (1 - rand.nextFloat()) * bound + min;
        }
        if (cumBorder.compareTo(BigDecimal.ZERO) > 0) {
            // shuffle
            CommonUtils.shuffle(size, rand, tupleData);
        }
    }

    /**
     * for columnNode
     * @return 返回用于multi-var计算的一个double数组
     */
    public double[] calculate() {
        return Arrays.copyOf(tupleData, tupleData.length);
    }

    @Override
    public boolean[] evaluate(CompareOperator operator, List<Parameter> parameters, boolean hasNot) {
        boolean[] ret = new boolean[tupleData.length];
        switch (operator) {
            case EQ:
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (!hasNot & (tupleData[i] == Double.parseDouble(parameters.get(0).getData())));
                }
                break;
            case NE:
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (!hasNot & (tupleData[i] != Double.parseDouble(parameters.get(0).getData())));
                }
                break;
            case LT:
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (!hasNot & (tupleData[i] < Double.parseDouble(parameters.get(0).getData())));
                }
                break;
            case LE:
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (!hasNot & (tupleData[i] <= Double.parseDouble(parameters.get(0).getData())));
                }
                break;
            case GT:
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (!hasNot & (tupleData[i] > Double.parseDouble(parameters.get(0).getData())));
                }
                break;
            case GE:
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = (!hasNot & (tupleData[i] >= Double.parseDouble(parameters.get(0).getData())));
                }
                break;
            case IN:
                double[] paramData = new double[parameters.size()];
                for (int i = 0; i < parameters.size(); i++) {
                    paramData[i] = Double.parseDouble(parameters.get(i).getData());
                }
                for (int i = 0; i < tupleData.length; i++) {
                    ret[i] = false;
                    for (double paramDatum : paramData) {
                        ret[i] = (ret[i] | (!hasNot & (tupleData[i] == paramDatum)));
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return ret;
    }

    @JsonIgnore
    public double[] getTupleData() {
        return tupleData;
    }
}
