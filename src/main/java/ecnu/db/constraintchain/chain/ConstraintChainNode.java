package ecnu.db.constraintchain.chain;

/**
 * @author wangqingshuai
 */
public abstract class ConstraintChainNode {
    protected String tableName;
    protected ConstraintChainNodeType constraintChainNodeType;

    public ConstraintChainNode(String tableName, ConstraintChainNodeType constraintChainNodeType) {
        this.tableName = tableName;
        this.constraintChainNodeType = constraintChainNodeType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ConstraintChainNodeType getConstraintChainNodeType() {
        return constraintChainNodeType;
    }

    public void setConstraintChainNodeType(ConstraintChainNodeType constraintChainNodeType) {
        this.constraintChainNodeType = constraintChainNodeType;
    }
}
