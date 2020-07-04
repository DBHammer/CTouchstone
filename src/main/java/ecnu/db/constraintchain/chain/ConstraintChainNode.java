package ecnu.db.constraintchain.chain;

/**
 * @author wangqingshuai
 */
public class ConstraintChainNode {
    protected String tableName;
    protected ConstraintChainNodeType constraintChainNodeType;

    public ConstraintChainNode(String tableName, ConstraintChainNodeType constraintChainNodeType) {
        this.tableName = tableName;
        this.constraintChainNodeType = constraintChainNodeType;
    }
}
