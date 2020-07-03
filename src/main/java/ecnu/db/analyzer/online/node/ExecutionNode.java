package ecnu.db.analyzer.online.node;


/**
 * @author wangqingshuai
 */
public class ExecutionNode {
    /**
     * 节点类型
     */
    private final ExecutionNodeType type;
    /**
     * 节点额外信息
     */
    private final String info;
    /**
     * 指向右节点
     */
    public ExecutionNode rightNode;
    /**
     * 指向左节点
     */
    public ExecutionNode leftNode;
    /**
     * 对应explain analyze的query plan树的节点名称
     */
    private String id;
    /**
     * 节点输出的数据量
     */
    private int outputRows;
    /**
     * 节点是否已经被访问过
     */
    private boolean visited;
    /**
     * 记录主键的join tag，第一次访问该节点后设置join tag，后续的访问可以找到之前对应的join tag
     */
    private int joinTag = -1;

    public ExecutionNode(String id, ExecutionNodeType type, int outputRows, String info) {
        this.type = type;
        this.info = info;
        this.id = id;
        this.outputRows = outputRows;
    }

    public ExecutionNode(ExecutionNodeType type, int outputRows, String info) {
        this.type = type;
        this.outputRows = outputRows;
        this.info = info;
    }

    public ExecutionNode(String id, ExecutionNodeType type, String info) {
        this.id = id;
        this.type = type;
        this.info = info;
    }

    public ExecutionNode(ExecutionNodeType type, String info) {
        this.type = type;
        this.info = info;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return 当前表最新的join tag
     */
    public int getJoinTag() {
        return joinTag;
    }

    public void setJoinTag(int joinTag) {
        this.joinTag = joinTag;
    }

    public String getInfo() {
        return info;
    }

    public ExecutionNode getRightNode() {
        return rightNode;
    }

    public void setRightNode(ExecutionNode rightNode) {
        this.rightNode = rightNode;
    }

    public ExecutionNode getLeftNode() {
        return leftNode;
    }

    public void setLeftNode(ExecutionNode leftNode) {
        this.leftNode = leftNode;
    }

    public int getOutputRows() {
        return outputRows;
    }

    public ExecutionNodeType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "ExecutionNode{" +
                "id='" + id + '}';
    }

    public enum ExecutionNodeType {
        /**
         * scan 节点，全表遍历，没有任何的过滤条件，只能作为叶子节点
         */
        scan,
        /**
         * filter节点，过滤节点，只能作为叶子节点
         */
        filter,
        /**
         * join 节点，同时具有左右子节点，只能作为非叶子节点
         */
        join
    }
}
