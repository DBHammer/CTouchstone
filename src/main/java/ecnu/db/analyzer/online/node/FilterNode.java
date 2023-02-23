package ecnu.db.analyzer.online.node;

public class FilterNode extends ExecutionNode {
    /**
     * 是否为新生成的节点
     */
    private boolean isAdd = false;
    private boolean isIndexScan = false;

    public FilterNode(String id, long outputRows, String info) {
        super(id, ExecutionNodeType.FILTER, outputRows, info);
    }

    public boolean isAdd() {
        return isAdd;
    }

    public void setAdd() {
        isAdd = true;
    }

    public boolean isIndexScan() {
        return isIndexScan;
    }

    public void setIndexScan(boolean indexScan) {
        isIndexScan = indexScan;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        FilterNode that = (FilterNode) o;

        if (isAdd != that.isAdd) return false;
        return isIndexScan == that.isIndexScan;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (isAdd ? 1 : 0);
        result = 31 * result + (isIndexScan ? 1 : 0);
        return result;
    }
}
