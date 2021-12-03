package ecnu.db.generator.constraintchain.chain;

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
    private boolean antiOrNot = false;

    public double getPkDistinctProbability() {
        return pkDistinctProbability;
    }

    public void setPkDistinctProbability(double pkDistinctProbability) {
        this.pkDistinctProbability = pkDistinctProbability;
    }

    private double pkDistinctProbability;

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
        this.probability = probability;
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

    public void setAntiJoin() {
        this.antiOrNot = true;
    }

    public void setAntiJoin(boolean antiOrNot) {
        this.antiOrNot = antiOrNot;
    }

    public boolean getAntiJoin() {
        return this.antiOrNot;
    }

    public void setProbabilityWithFailFilter(BigDecimal probabilityWithFailFilter) {
        this.probabilityWithFailFilter = probabilityWithFailFilter;
    }

    public BigDecimal getProbabilityWithFailFilter() {
        return probabilityWithFailFilter;
    }
}
