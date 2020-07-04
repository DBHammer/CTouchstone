package ecnu.db.constraintchain.chain;

/**
 * @author wangqingshuai
 */
public class ConstraintChainPkJoinNode extends ConstraintChainNode {
    private String[] pkColumnNames;
    private int pkTag;

    /**
     * 构建ConstraintChainPkJoinNode对象
     *
     * @param constraintChainInfo 获取到的约束链信息
     */
    public ConstraintChainPkJoinNode(String tableName, String constraintChainInfo) {
        super(tableName, ConstraintChainNodeType.PkJoin);
        //todo 解析constraintChainInfo
    }
}
