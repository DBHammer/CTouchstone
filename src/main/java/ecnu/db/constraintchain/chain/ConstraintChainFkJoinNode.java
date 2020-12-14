package ecnu.db.constraintchain.chain;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFkJoinNode extends ConstraintChainNode {
    private String refCols;
    private String localCols;
    private int pkTag;
    private BigDecimal probability;


    public ConstraintChainFkJoinNode() {
        super(ConstraintChainNodeType.FK_JOIN);
    }

    public ConstraintChainFkJoinNode(String localCols, String refCols, int pkTag, BigDecimal probability) {
        super(ConstraintChainNodeType.FK_JOIN);
        this.refCols = refCols;
        this.pkTag = pkTag;
        this.localCols = localCols;
        this.probability = probability;
    }

    public ConstraintChainFkJoinNode(String constraintChainInfo) {
        super(ConstraintChainNodeType.FK_JOIN);
        //todo 解析constraintChainInfo
    }

    @Override
    public String toString() {
        return String.format("{pkTag:%d,refCols:%s,probability:%s}", pkTag, refCols , probability);
    }

    public int getPkTag() {
        return pkTag;
    }

    public BigDecimal getProbability() {
        return probability;
    }

    public String getLocalCols() {
        return localCols;
    }

    public void setRefCols(String refCols) {
        this.refCols = refCols;
    }

    public void setPkTag(int pkTag) {
        this.pkTag = pkTag;
    }

    public void setProbability(BigDecimal probability) {
        this.probability = probability;
    }

    public void setLocalCols(String localCols) {
        this.localCols = localCols;
    }

    public String getRefCols() {
        return refCols;
    }
}
