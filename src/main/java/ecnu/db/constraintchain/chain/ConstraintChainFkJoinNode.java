package ecnu.db.constraintchain.chain;

import java.util.HashMap;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFkJoinNode extends ConstraintChainNode {
    /**
     * 本地表和参照表的映射关系，用点来分割映射关系
     * localColumnName -> refColumnName
     */
    private HashMap<String, String> foreignKeys;
    private String refTable;
    private int pkTag;

    public ConstraintChainFkJoinNode(String tableName, String constraintChainInfo) {
        super(tableName, ConstraintChainNodeType.FkJoin);
        //todo 解析constraintChainInfo
    }
}
