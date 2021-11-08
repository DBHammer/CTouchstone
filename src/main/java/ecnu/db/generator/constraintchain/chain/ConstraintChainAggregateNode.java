package ecnu.db.generator.constraintchain.chain;

import ecnu.db.generator.constraintchain.filter.LogicNode;

import java.math.BigDecimal;
import java.util.List;

public class ConstraintChainAggregateNode extends ConstraintChainNode {
    private List<String> groupKey;
    private final LogicNode root;
    private final BigDecimal aggProbability;
    private final BigDecimal filterProbability;

    public ConstraintChainAggregateNode(List<String> groupKeys, LogicNode root, BigDecimal aggProbability, BigDecimal filterProbability) {
        super(ConstraintChainNodeType.AGGREGATE);
        this.groupKey = groupKeys;
        this.root = root;
        this.aggProbability = aggProbability;
        this.filterProbability = filterProbability;
    }

    @Override
    public String toString() {
        return String.format("{GroupKey:%s, Filter:%s, aggProbability:%s, filterProbability:%s}",
                groupKey, root, aggProbability, filterProbability);
    }

    public List<String> getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(List<String> groupKey) {
        this.groupKey = groupKey;
    }
}
