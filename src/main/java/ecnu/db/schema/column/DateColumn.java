package ecnu.db.schema.column;

import com.fasterxml.jackson.annotation.JsonFormat;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.operation.CompareOperator;
import ecnu.db.schema.column.bucket.EqBucket;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author alan
 */
public class DateColumn extends AbstractColumn {
    public static final DateTimeFormatter FMT = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter();
    private LocalDate begin;
    private LocalDate end;
    private long[] longCopyOfTupleData;
    private LocalDate[] tupleData;

    public DateColumn() {
        super(null, ColumnType.DATETIME);
    }

    public DateColumn(String columnName) {
        super(columnName, ColumnType.DATETIME);
    }

    public LocalDate getBegin() {
        return begin;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    public void setBegin(LocalDate begin) {
        this.begin = begin;
    }

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    public LocalDate getEnd() {
        return end;
    }

    public void setEnd(LocalDate end) {
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
            Duration duration = Duration.between(begin, end);
            BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
            BigDecimal probability = BigDecimal.valueOf(Math.random() * (maxP - minP) + minP);
            duration = Duration.ofSeconds(seconds.multiply(probability).longValue());
            data = FMT.format(begin.plus(duration));
        } while (eqCandidates.contains(data));
        eqCandidates.add(data);
        return data;
    }

    @Override
    public String generateNonEqParamData(BigDecimal probability) {
        Duration duration = Duration.between(begin, end);
        BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
        duration = Duration.ofSeconds(seconds.multiply(probability).longValue());
        LocalDate newDate = begin.plus(duration);
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter();
        return formatter.format(newDate);
    }

    @Override
    public void prepareTupleData(int size) {
        eqBuckets.sort(Comparator.comparing(o -> o.leftBorder));
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        BigDecimal cumBorder = BigDecimal.ZERO, sizeVal = BigDecimal.valueOf(size);
        if (longCopyOfTupleData == null || longCopyOfTupleData.length != size) {
            longCopyOfTupleData = new long[size];
        }
        for (EqBucket eqBucket : eqBuckets) {
            for (Map.Entry<BigDecimal, Parameter> entry : eqBucket.eqConditions.entries()) {
                BigDecimal newCum = cumBorder.add(entry.getKey().multiply(sizeVal));
                LocalDate date = LocalDate.parse(entry.getValue().getData(), FMT);
                for (int j = cumBorder.intValue(); j < newCum.intValue() && j < size; j++) {
                    longCopyOfTupleData[j] = date.atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                }
                cumBorder = newCum;
            }
        }
        long endTimeStamp = end.atStartOfDay(ZoneId.systemDefault()).toEpochSecond(), beginTimeStamp = begin.atStartOfDay(ZoneId.systemDefault()).toEpochSecond(), bound = endTimeStamp - beginTimeStamp + 1;
        for (int i = cumBorder.intValue(); i < size; i++) {
            longCopyOfTupleData[i] = (long) ((1 - rand.nextDouble()) * bound + beginTimeStamp);
        }
        if (cumBorder.compareTo(BigDecimal.ZERO) > 0) {
            CommonUtils.shuffle(size, rand, longCopyOfTupleData);
        }
        if (tupleData == null || tupleData.length != size) {
            tupleData = new LocalDate[size];
        }
        for (int i = 0; i < size; i++) {
            tupleData[i] = Instant.ofEpochSecond(longCopyOfTupleData[i]).atZone(ZoneId.systemDefault()).toLocalDate();
        }
    }

    @Override
    public boolean[] evaluate(CompareOperator operator, List<Parameter> parameters, boolean hasNot) {
        boolean[] ret = new boolean[longCopyOfTupleData.length];
        long value;
        switch (operator) {
            case EQ:
                value = LocalDate.parse(parameters.get(0).getData(), FMT).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (!hasNot & (longCopyOfTupleData[i] == value));
                }
                break;
            case NE:
                value = LocalDate.parse(parameters.get(0).getData(), FMT).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (!hasNot & (longCopyOfTupleData[i] != value));
                }
                break;
            case IN:
                long[] parameterData = new long[parameters.size()];
                for (int i = 0; i < parameterData.length; i++) {
                    parameterData[i] = LocalDate.parse(parameters.get(i).getData(), FMT).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                }
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = false;
                    for (double paramDatum : parameterData) {
                        ret[i] = (ret[i] | (!hasNot & (longCopyOfTupleData[i] == paramDatum)));
                    }
                }
                break;
            case LT:
                value = LocalDate.parse(parameters.get(0).getData(), FMT).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (!hasNot & (longCopyOfTupleData[i] < value));
                }
                break;
            case LE:
                value = LocalDate.parse(parameters.get(0).getData(), FMT).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (!hasNot & (longCopyOfTupleData[i] <= value));
                }
                break;
            case GT:
                value = LocalDate.parse(parameters.get(0).getData(), FMT).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (!hasNot & (longCopyOfTupleData[i] > value));
                }
                break;
            case GE:
                value = LocalDate.parse(parameters.get(0).getData(), FMT).atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
                for (int i = 0; i < longCopyOfTupleData.length; i++) {
                    ret[i] = (!hasNot & (longCopyOfTupleData[i] >= value));
                }
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return ret;
    }

    public LocalDate[] getTupleData() {
        return tupleData;
    }
}
