package ecnu.db.constraintchain.chain;

/**
 * @author wangqingshuai
 */
public abstract class ConstraintChainNode {
    protected ConstraintChainNodeType constraintChainNodeType;

    public ConstraintChainNode(ConstraintChainNodeType constraintChainNodeType) {
        this.constraintChainNodeType = constraintChainNodeType;
    }

    public ConstraintChainNodeType getConstraintChainNodeType() {
        return constraintChainNodeType;
    }

    public void setConstraintChainNodeType(ConstraintChainNodeType constraintChainNodeType) {
        this.constraintChainNodeType = constraintChainNodeType;
    }
}
