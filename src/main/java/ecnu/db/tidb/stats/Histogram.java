package ecnu.db.tidb.stats;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
class Histogram {
    int ndv;

    public int getNdv() {
        return ndv;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }
}