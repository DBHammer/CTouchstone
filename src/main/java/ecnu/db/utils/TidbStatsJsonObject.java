package ecnu.db.utils;

import com.alibaba.fastjson.annotation.JSONField;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;

/**
 * @author qingshuai.wang
 */
public class TidbStatsJsonObject {
    int count;
    HashMap<String, Distribution> columns;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public HashMap<String, Distribution> getColumns() {
        return columns;
    }

    public void setColumns(HashMap<String, Distribution> columns) {
        this.columns = columns;
    }

    public float getNullProbability(String name) {
        return (float) columns.get(name).nullCount / count;
    }

    public BigDecimal getAvgLength(String name) {
        if (count == 0) {
            return new BigDecimal(0);
        }
        BigDecimal totalSize = BigDecimal.valueOf(columns.get(name).totalColSize);
        BigDecimal tableSize = BigDecimal.valueOf(count);
        return totalSize.divide(tableSize, 4, RoundingMode.HALF_UP).subtract(BigDecimal.valueOf(2));
    }


    public int getNdv(String name) {
        return columns.get(name).getHistogram().getNdv();
    }
}

class Distribution {
    @JSONField(name = "null_count")
    int nullCount;
    Histogram histogram;
    @JSONField(name = "tot_col_size")
    int totalColSize;

    public int getTotalColSize() {
        return totalColSize;
    }

    public void setTotalColSize(int totalColSize) {
        this.totalColSize = totalColSize;
    }

    public Histogram getHistogram() {
        return histogram;
    }

    public void setHistogram(Histogram histogram) {
        this.histogram = histogram;
    }

    public int getNullCount() {
        return nullCount;
    }

    public void setNullCount(int nullCount) {
        this.nullCount = nullCount;
    }
}

class Histogram {
    int ndv;

    public int getNdv() {
        return ndv;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }
}


