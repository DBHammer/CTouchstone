package ecnu.db.constraintchain.chain;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFkJoinNode extends ConstraintChainNode {
    private String refTable;
    private String refCols;
    private int pkTag;
    private BigDecimal probability;
    private String localCols;

    public ConstraintChainFkJoinNode() {
        super(null, ConstraintChainNodeType.FK_JOIN);
    }

    public ConstraintChainFkJoinNode(String localTable, String localCols, String refTable, String refCols, int pkTag, BigDecimal probability) {
        super(localTable, ConstraintChainNodeType.FK_JOIN);
        this.refTable = refTable;
        this.refCols = refCols;
        this.pkTag = pkTag;
        this.localCols = localCols;
        this.probability = probability;
    }

    public ConstraintChainFkJoinNode(String tableName, String constraintChainInfo) {
        super(tableName, ConstraintChainNodeType.FK_JOIN);
        //todo 解析constraintChainInfo
    }

    @Override
    public String toString() {
        return String.format("{pkTag:%d,refTable:%s,probability:%s}", pkTag, refTable, probability);
    }

    public String getRefTable() {
        return refTable;
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

    public String getRefCols() {
        return refCols;
    }

    public String getJoinInfoName() {
        return localCols + ":" + refTable + "." + refCols + pkTag;
    }
}
