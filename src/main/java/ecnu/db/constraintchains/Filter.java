package ecnu.db.constraintchains;

import java.util.Arrays;
import java.util.Map;

public class Filter {

    // The logical relation between multiple basic filter operations
    // -1: default value (there is only one basic filter operations), 0: and, 1: or
    private final int logicalRelation;
    // the 'probability' is the combined probability of multiple basic filter operations, and the probability
    // of each basic filter operation can be calculated based on it
    private final float probability;
    // there may be multiple basic filter operations
    private final FilterOperation[] filterOperations;

    public Filter(FilterOperation[] filterOperations, int logicalRelation, float probability) {
        super();
        this.filterOperations = filterOperations;
        this.logicalRelation = logicalRelation;
        this.probability = probability;
    }

    public Filter(Filter filter) {
        super();
        this.filterOperations = new FilterOperation[filter.filterOperations.length];
        for (int i = 0; i < filter.filterOperations.length; i++) {
            this.filterOperations[i] = new FilterOperation(filter.filterOperations[i]);
        }
        this.logicalRelation = filter.logicalRelation;
        this.probability = filter.probability;
    }

    public boolean isSatisfied(Map<String, String> attributeValueMap) {
        boolean res = false;
        if (logicalRelation == -1) {
            res = filterOperations[0].isSatisfied(attributeValueMap);
        } else if (logicalRelation == 0) {
            res = true;
            for (FilterOperation filterOperation : filterOperations) {
                if (!filterOperation.isSatisfied(attributeValueMap)) {
                    res = false;
                    break;
                }
            }
        } else if (logicalRelation == 1) {
            for (FilterOperation filterOperation : filterOperations) {
                if (filterOperation.isSatisfied(attributeValueMap)) {
                    res = true;
                    break;
                }
            }
        }
        return res;
    }

    public FilterOperation[] getFilterOperations() {
        return filterOperations;
    }

    public int getLogicalRelation() {
        return logicalRelation;
    }

    public float getProbability() {
        return probability;
    }

    @Override
    public String toString() {
        return "\n\tFilter [filterOperations=" + Arrays.toString(filterOperations) + ", \n\t\tlogicalRelation="
                + logicalRelation + ", probability=" + probability + "]";
    }
}
