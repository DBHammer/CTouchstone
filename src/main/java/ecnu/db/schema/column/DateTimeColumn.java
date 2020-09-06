package ecnu.db.schema.column;


import com.fasterxml.jackson.annotation.JsonFormat;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.operation.CompareOperator;
import ecnu.db.schema.column.bucket.EqBucket;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author qingshuai.wang
 */
public class DateTimeColumn extends AbstractColumn {
    public static final DateTimeFormatter FMT = new DateTimeFormatterBuilder()
            .appendOptional(new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
                    .toFormatter())
            .appendOptional(
                    new DateTimeFormatterBuilder()
                            .appendPattern("yyyy-MM-dd")
                            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                            .toFormatter())
            .toFormatter();
    private LocalDateTime begin;
    private LocalDateTime end;
    private int precision; // fraction precision for datetime
    private long[] longCopyOfTupleData;
    private LocalDateTime[] tupleData;

    public DateTimeColumn() {
        super(null, ColumnType.DATETIME);
    }

    public DateTimeColumn(String columnName) {
        super(columnName, ColumnType.DATETIME);
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS")
    public LocalDateTime getBegin() {
        return begin;
    }

    public void setBegin(LocalDateTime begin) {
        this.begin = begin;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS")
    public LocalDateTime getEnd() {
        return end;
    }

    public void setEnd(LocalDateTime end) {
        this.end = end;
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
            Duration duration = Duration.between(getBegin(), getEnd());
            BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
            BigDecimal nano = BigDecimal.valueOf(duration.getNano());
            BigDecimal probability = BigDecimal.valueOf(Math.random() * (maxP - minP) + minP);
            duration = Duration.ofSeconds(seconds.multiply(probability).longValue(), nano.multiply(probability).intValue());
            data = FMT.format(begin.plus(duration));
        } while (eqCandidates.contains(data));
        eqCandidates.add(data);
        return data;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    @Override
    public String generateNonEqParamData(BigDecimal probability) {
        Duration duration = Duration.between(getBegin(), getEnd());
        BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
        BigDecimal nano = BigDecimal.valueOf(duration.getNano());
        duration = Duration.ofSeconds(seconds.multiply(probability).longValue(), nano.multiply(probability).intValue());
        LocalDateTime newDateTime = begin.plus(duration);
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss");
        if (precision > 0) {
            builder.appendFraction(ChronoField.MICRO_OF_SECOND, 0, precision, true);
        }
        DateTimeFormatter formatter = builder.toFormatter();

        return formatter.format(newDateTime);
    }

    // todo 暂时不考虑超过毫秒精度的情况
    @Override
    public void prepareTupleData(int size) {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        eqBuckets.sort(Comparator.comparing(o -> o.leftBorder));
        BigDecimal cumBorder = BigDecimal.ZERO, sizeVal = BigDecimal.valueOf(size);
        if (longCopyOfTupleData == null || longCopyOfTupleData.length != size) {
            longCopyOfTupleData = new long[size];
        }
        long endTimeStamp = end.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(), beginTimeStamp = begin.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(), bound = endTimeStamp - beginTimeStamp + 1;
        for (int i = cumBorder.intValue(); i < size; i++) {
            longCopyOfTupleData[i] = (long) ((1 - rand.nextDouble()) * bound + beginTimeStamp);
        }
        if (eqCandidates.size() > 0) {
            Arrays.sort(longCopyOfTupleData);
        }
        for (EqBucket eqBucket : eqBuckets) {
            int j = (int) (eqBucket.leftBorder.doubleValue() * size);
            for (Map.Entry<BigDecimal, Parameter> entry : eqBucket.eqConditions.entries()) {
                // new_start = ori_start + probability * size
                int newJ = (int) (j + entry.getKey().doubleValue() * size);
                LocalDateTime time = LocalDateTime.parse(entry.getValue().getData(), FMT);
                for (; j < newJ && j < size; j++) {
                    longCopyOfTupleData[j] = time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                }
                j = newJ;
            }
        }
        if (eqCandidates.size() > 0) {
            CommonUtils.shuffle(size, rand, longCopyOfTupleData);
        }
        if (tupleData == null || tupleData.length != size) {
            tupleData = new LocalDateTime[size];
        }
        for (int i = 0; i < size; i++) {
            tupleData[i] = Instant.ofEpochMilli(longCopyOfTupleData[i]).atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
    }

    @Override
    public boolean[] evaluate(CompareOperator operator, List<Parameter> parameters, boolean hasNot) {
        boolean[] ret = new boolean[longCopyOfTupleData.length];
        long value;
        switch (operator) {
            case EQ:
                value = LocalDateTime.parse(parameters.get(0).getData(), FMT).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (longCopyOfTupleData[i] == value);
                }
                break;
            case NE:
                value = LocalDateTime.parse(parameters.get(0).getData(), FMT).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (longCopyOfTupleData[i] != value);
                }
                break;
            case IN:
                long[] parameterData = new long[parameters.size()];
                for (int i = 0; i < parameterData.length; i++) {
                    parameterData[i] = LocalDateTime.parse(parameters.get(i).getData(), FMT).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                }
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = false;
                    for (double paramDatum : parameterData) {
                        ret[i] = (ret[i] | (longCopyOfTupleData[i] == paramDatum));
                        ret[i] = (hasNot ^ ret[i]);
                    }
                }
                break;
            case LT:
                value = LocalDateTime.parse(parameters.get(0).getData(), FMT).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (longCopyOfTupleData[i] < value);
                }
                break;
            case LE:
                value = LocalDateTime.parse(parameters.get(0).getData(), FMT).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (longCopyOfTupleData[i] <= value);
                }
                break;
            case GT:
                value = LocalDateTime.parse(parameters.get(0).getData(), FMT).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (longCopyOfTupleData[i] > value);
                }
                break;
            case GE:
                value = LocalDateTime.parse(parameters.get(0).getData(), FMT).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (longCopyOfTupleData[i] >= value);
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return ret;
    }

    @Override
    public void setTupleByRefColumn(AbstractColumn column, int i, int j) {
        DateTimeColumn dateTimeColumn = (DateTimeColumn) column;
        tupleData[i] = dateTimeColumn.tupleData[j];
    }

    public LocalDateTime[] getTupleData() {
        return tupleData;
    }
}
