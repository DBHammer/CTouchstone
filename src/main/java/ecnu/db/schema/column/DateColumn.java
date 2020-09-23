package ecnu.db.schema.column;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

/**
 * @author alan
 */
public class DateColumn extends AbstractColumn {
    public static final DateTimeFormatter FMT = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd").toFormatter();
    private LocalDate begin;
    private LocalDate end;

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
    protected String generateEqData(BigDecimal minProbability, BigDecimal maxProbability) {
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

    public LocalDate generateData(BigDecimal probability) {
        Duration duration = Duration.between(begin, end);
        BigDecimal seconds = BigDecimal.valueOf(duration.getSeconds());
        duration = Duration.ofSeconds(seconds.multiply(probability).longValue());
        return begin.plus(duration);
    }
}
