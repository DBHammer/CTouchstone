package ecnu.db.constraintchain.chain;

import java.math.BigDecimal;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFkJoinNode extends ConstraintChainNode {
    private String refTable;
    private String refCol;
    private int pkTag;
    private BigDecimal probability;
    private String fkCol;

    public ConstraintChainFkJoinNode() {
        super(null, ConstraintChainNodeType.FK_JOIN);
    }

    public ConstraintChainFkJoinNode(String tableName, String refTable, int pkTag, String refCol, String fkCol, BigDecimal probability) {
        super(tableName, ConstraintChainNodeType.FK_JOIN);
        this.refTable = refTable;
        this.refCol = refCol;
        this.pkTag = pkTag;
        this.fkCol = fkCol;
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

    public String getFkCol() {
        return fkCol;
    }

    public String getRefCol() {
        return refCol;
    }

    public String getJoinInfoName() {
        return fkCol + ":" + refTable + "." + refCol + pkTag;
    }
}
