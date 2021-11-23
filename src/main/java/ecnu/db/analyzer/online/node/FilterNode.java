package ecnu.db.analyzer.online.node;

public class FilterNode extends ExecutionNode{
    /**
     * 是否为新生成的节点
     */
    public boolean isAdd = false;

    public boolean isAdd() {
        return isAdd;
    }

    public void setAdd() {
        isAdd = true;
    }

    public FilterNode(String id, int outputRows, String info) {
        super(id, ExecutionNodeType.filter, outputRows, info);
    }
}
