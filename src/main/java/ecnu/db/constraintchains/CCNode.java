package ecnu.db.constraintchains;

public class CCNode {

    private final int type;

    private Object node = null;

    public CCNode(int type, Object node) {
        super();
        this.type = type;
        this.node = node;
    }

    public CCNode(CCNode ccNode) {
        super();
        this.type = ccNode.type;
        if (this.type == 0) {
            this.node = new Filter((Filter) ccNode.node);
        }
    }

    public int getType() {
        return type;
    }

    public Object getNode() {
        return node;
    }

    @Override
    public String toString() {
        return node.toString();
    }
}
