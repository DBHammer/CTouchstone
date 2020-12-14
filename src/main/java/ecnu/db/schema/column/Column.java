package ecnu.db.schema.column;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wangqingshuai
 */
public class Column {
    private long min;
    private long max;
    private List<Histogram> histograms;

    public Column(long min, long max) {
        this.min = min;
        this.max = max;
        histograms = Collections.singletonList(new Histogram(min, 1));
    }

    private static class Histogram {
        private long start;
        private double range;
        Map<Long, Double> eqConstraints = new HashMap<>();

        public double getFreeRange() {
            return range - eqConstraints.values().stream().mapToDouble(Double::doubleValue).sum();
        }

        public double getRange() {
            return range;
        }

        public long getStart() {
            return start;
        }

        public void setRange(double range) {
            this.range = range;
        }

        public Histogram(long start) {
            this.start = start;
        }

        public Histogram(long start, double range) {
            this.start = start;
            this.range = range;
        }
    }
}
