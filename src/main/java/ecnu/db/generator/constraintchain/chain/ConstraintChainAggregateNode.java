package ecnu.db.generator.constraintchain.chain;

import ecnu.db.generator.constraintchain.filter.LogicNode;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.schema.TableManager;

import java.math.BigDecimal;
import java.util.List;

public class ConstraintChainAggregateNode extends ConstraintChainNode {
    private List<String> groupKey;
    private BigDecimal aggProbability;
    ConstraintChainFilterNode aggFilter;

    public BigDecimal getAggProbability() {
        return aggProbability;
    }

    public void setAggProbability(BigDecimal aggProbability) {
        this.aggProbability = aggProbability;
    }

    public ConstraintChainAggregateNode(List<String> groupKeys, BigDecimal aggProbability) {
        super(ConstraintChainNodeType.AGGREGATE);
        this.groupKey = groupKeys;
        this.aggProbability = aggProbability;
    }

    public boolean removeAgg() {
        if (aggProbability.equals(BigDecimal.ONE) && aggFilter == null) {
            return true;
        }
        if (groupKey == null) {
            if (aggFilter == null) {
                return true;
            } else {
                return aggFilter.getParameters().stream().allMatch(Parameter::isActual);
            }
        }
        //todo deal with fk and attributes
        return groupKey.stream().anyMatch(key -> !TableManager.getInstance().isPrimaryKeyOrForeignKey(key));
    }

    public ConstraintChainFilterNode getAggFilter() {
        return aggFilter;
    }

    public void setAggFilter(ConstraintChainFilterNode aggFilter) {
        this.aggFilter = aggFilter;
    }

    @Override
    public String toString() {
        return String.format("{GroupKey:%s, aggProbability:%s}", groupKey, aggProbability);
    }

    public List<String> getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(List<String> groupKey) {
        this.groupKey = groupKey;
    }
}
