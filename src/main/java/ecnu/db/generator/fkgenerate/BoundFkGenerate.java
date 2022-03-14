package ecnu.db.generator.fkgenerate;

import ecnu.db.utils.CommonUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

public class BoundFkGenerate implements FkGenerate {
    int currentIndex;
    long currentCardinality;
    long stepSize;
    long stepIndex;
    long rangeSize;
    long maxIndex;
    long maxCardinality;
    ArrayList<Long> allCardinality = new ArrayList<>();

    public BoundFkGenerate(long minIndex, long maxIndex, long rangeSize) {
        this.currentIndex = 0;
        this.currentCardinality = maxIndex - minIndex - 1;
        for (long i = minIndex; i < maxIndex; i++) {
            allCardinality.add(i);
        }
        Collections.shuffle(allCardinality);
        this.stepIndex = 0;
        this.maxIndex = maxIndex;
        this.rangeSize = rangeSize;
        this.stepSize = (long) Math.floor((double) rangeSize / currentCardinality);
        this.maxCardinality = (long) (CommonUtils.CardinalityScale * stepSize);
    }

    @Override
    public long getValue() {
        if (currentCardinality == -1) {
            return allCardinality.get(currentIndex - 1);
        } else {
            long fkStatusIndex = allCardinality.get(currentIndex);
            moveIndex();
            return fkStatusIndex;
        }
    }

    @Override
    public boolean isValid() {
        return maxIndex >= 0;
    }

    public void moveIndex() {
        stepIndex++;
        rangeSize--;
        if (stepIndex % stepSize == 0) {
            currentIndex++;
            currentCardinality--;
            if (currentCardinality > 0) {
                stepSize = (long) ((double) rangeSize / currentCardinality);
                if (stepSize > 2) {
                    long temp = ThreadLocalRandom.current().nextLong(stepSize - 2, stepSize + 3);
                    if (currentCardinality > 1 && (double) (rangeSize - stepSize) / (currentCardinality - 1) <= maxCardinality) {
                        stepSize = temp;
                    }
                }
            } else {
                stepSize = rangeSize;
            }
            stepIndex = 0;
        }
    }

    @Override
    public String toString() {
        return "CardinalityConstraint{" +
                "currentIndex=" + currentIndex +
                ", cardinality=" + currentCardinality +
                '}';
    }
}
