package ecnu.db.analyzer.online.node;

public class AggNode extends ExecutionNode {
    /**
     * 如果aggregate中含有filter，则记录经过filter之后的行数
     */
    private FilterNode aggFilter;

    public AggNode(String id, int outputRows, String info) {
        super(id, ExecutionNodeType.AGGREGATE, outputRows, info);
    }

    public FilterNode getAggFilter() {
        return aggFilter;
    }

    public void setAggFilter(FilterNode aggFilter) {
        this.aggFilter = aggFilter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AggNode aggNode = (AggNode) o;

        return aggFilter.equals(aggNode.aggFilter);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (aggFilter != null) {
            result = 31 * result + aggFilter.hashCode();
        }
        return result;
    }
}
