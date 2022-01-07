package ecnu.db.generator.fkgenerate;

import org.apache.commons.math3.distribution.ZipfDistribution;

public class RandomFkGenerate implements FkGenerate {

    long minIndex;
    long maxIndex;
    long currentIndex;
    private ZipfDistribution zipfDistribution;

    public RandomFkGenerate(long minIndex, long maxIndex, long rangeSize) {
        this.minIndex = minIndex;
        this.maxIndex = maxIndex;
        this.currentIndex = minIndex;
        if (maxIndex > minIndex) {
            zipfDistribution = new ZipfDistribution((int) (maxIndex - minIndex), 0.0001);
        }
    }

    @Override
    public long getValue() {
        if (currentIndex < maxIndex) {
            return currentIndex++;
        } else {
            if (maxIndex == minIndex) {
                return maxIndex;
            } else {
                return zipfDistribution.sample() + minIndex - 1;
            }
        }
    }

    @Override
    public boolean isValid() {
        return maxIndex >= 0;
    }
}
