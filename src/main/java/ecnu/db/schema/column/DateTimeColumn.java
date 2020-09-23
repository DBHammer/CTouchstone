package ecnu.db.schema.column;


import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

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
    protected String generateEqData(BigDecimal minProbability, BigDecimal maxProbability) {
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

    public LocalDateTime generateData(BigDecimal probability) {
        Duration duration = Duration.between(getBegin(), getEnd());
        BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
        BigDecimal nano = BigDecimal.valueOf(duration.getNano());
        duration = Duration.ofSeconds(seconds.multiply(probability).longValue(), nano.multiply(probability).intValue());
        return begin.plus(duration);
    }
}
