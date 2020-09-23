package ecnu.db.tidb.stats;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;

@JsonIgnoreProperties(ignoreUnknown = true)
class Distribution {
    int nullCount;
    Histogram histogram;
    int totalColSize;

    public int getTotalColSize() {
        return totalColSize;
    }

    @JsonSetter("tot_col_size")
    public void setTotalColSize(int totalColSize) {
        this.totalColSize = totalColSize;
    }

    public Histogram getHistogram() {
        return histogram;
    }

    public void setHistogram(Histogram histogram) {
        this.histogram = histogram;
    }

    @JsonSetter("null_count")
    public int getNullCount() {
        return nullCount;
    }

    public void setNullCount(int nullCount) {
        this.nullCount = nullCount;
    }
}