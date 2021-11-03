package ecnu.db.generator.constraintchain.chain;

import java.util.List;

public class ConstraintChainAggregateNode extends ConstraintChainNode {
    List<String> groupKey;
    String filter="";
    int outPutRows;
    int rowsAfterFilter=0;
    public ConstraintChainAggregateNode(List<String> groupKeys, String Filter, int outPutRows, int rowsAfterFilter) {
        super(ConstraintChainNodeType.AGGREGATE);
        groupKey = groupKeys;
        filter = Filter;
        this.outPutRows = outPutRows;
        this.rowsAfterFilter = rowsAfterFilter;
    }

    @Override
    public String toString() {
        return String.format("{GroupKey:%s, Filter:%s, outPutRows:%d, rowsAfterFilter:%d}", groupKey, filter, outPutRows, rowsAfterFilter);
    }

    public List<String> getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(List<String> groupKey) {
        this.groupKey = groupKey;
    }
}
