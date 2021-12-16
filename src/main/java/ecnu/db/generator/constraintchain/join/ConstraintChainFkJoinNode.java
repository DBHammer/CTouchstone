package ecnu.db.generator.constraintchain.join;

import ecnu.db.generator.constraintchain.ConstraintChainNode;
import ecnu.db.generator.constraintchain.ConstraintChainNodeType;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFkJoinNode extends ConstraintChainNode {
    private String refCols;
    private String localCols;
    private long pkTag;
    private BigDecimal probability;
    private BigDecimal probabilityWithFailFilter;
    private BigDecimal pkDistinctProbability;
    private ConstraintNodeJoinType type = ConstraintNodeJoinType.INNER_JOIN;

    public ConstraintChainFkJoinNode() {
        super(ConstraintChainNodeType.FK_JOIN);
    }

    public ConstraintChainFkJoinNode(String localCols, String refCols, long pkTag, BigDecimal probability) {
        super(ConstraintChainNodeType.FK_JOIN);
        this.refCols = refCols;
        this.pkTag = pkTag;
        this.localCols = localCols;
        this.probability = probability;
    }

    public ConstraintNodeJoinType getType() {
        return type;
    }

    public void setType(ConstraintNodeJoinType type) {
        this.type = type;
    }

    public BigDecimal getPkDistinctProbability() {
        return pkDistinctProbability;
    }

    public void setPkDistinctProbability(BigDecimal pkDistinctProbability) {
        this.pkDistinctProbability = pkDistinctProbability;
    }

    @Override
    public String toString() {
        return String.format("{pkTag:%d,refCols:%s,probability:%s,pkDistinctProbability:%f,probabilityWithFailFilter:%s}", pkTag, refCols, probability, pkDistinctProbability, probabilityWithFailFilter);
    }

    public long getPkTag() {
        return pkTag;
    }

    public void setPkTag(long pkTag) {
        this.pkTag = pkTag;
    }

    public BigDecimal getProbability() {
        return probability;
    }

    public void setProbability(BigDecimal probability) {
        this.probability = probability.stripTrailingZeros();
    }

    public String getLocalCols() {
        return localCols;
    }

    public void setLocalCols(String localCols) {
        this.localCols = localCols;
    }

    public String getRefCols() {
        return refCols;
    }

    public void setRefCols(String refCols) {
        this.refCols = refCols;
    }

    public BigDecimal getProbabilityWithFailFilter() {
        return probabilityWithFailFilter;
    }

    public void setProbabilityWithFailFilter(BigDecimal probabilityWithFailFilter) {
        this.probabilityWithFailFilter = probabilityWithFailFilter.stripTrailingZeros();
    }
}
