package ecnu.db.constraintchain.chain;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFkJoinNode extends ConstraintChainNode {
    /**
     * 本地表和参照表的映射关系，用点来分割映射关系
     * localColumnName -> refColumnName
     */
    private Map<String, String> foreignKeys;
    private String refTable;
    private int pkTag;
    private BigDecimal probability;

    public ConstraintChainFkJoinNode() {
        super(null, ConstraintChainNodeType.FK_JOIN);
    }

    public ConstraintChainFkJoinNode(String tableName, String refTable, int pkTag, Map<String, String> foreignKeys, BigDecimal probability) {
        super(tableName, ConstraintChainNodeType.FK_JOIN);
        this.refTable = refTable;
        this.pkTag = pkTag;
        this.foreignKeys = foreignKeys;
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
}
