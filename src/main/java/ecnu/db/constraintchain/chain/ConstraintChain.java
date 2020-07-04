package ecnu.db.constraintchain.chain;

import java.util.List;

/**
 * @author wangqingshuai
 */
public class ConstraintChain {

    private final String tableName;
    private final List<ConstraintChainNode> nodes;

    public ConstraintChain(String tableName, List<ConstraintChainNode> nodes) {
        super();
        this.tableName = tableName;
        this.nodes = nodes;
    }

    public void addNode(ConstraintChainNode node) {
        nodes.add(node);
    }

    public String getTableName() {
        return tableName;
    }

    public List<ConstraintChainNode> getNodes() {
        return nodes;
    }

    @Override
    public String toString() {
        return "\nConstraintChain [tableName=" + tableName + ", nodes=" + nodes + "]";
    }
}
