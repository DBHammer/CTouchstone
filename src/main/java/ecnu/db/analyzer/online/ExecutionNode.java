package ecnu.db.analyzer.online;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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
     * 节点输出的数据量
     */
    private final int outputRows;
    /**
     * 指向右节点
     */
    public ExecutionNode rightNode;
    /**
     * 指向左节点
     */
    public ExecutionNode leftNode;
    /**
     * 是否为新生成的节点
     */
    public boolean isAdd = false;
    /**
     * groupKey
     */
    private List<String> GroupKey = new ArrayList<>();

    public int getRowsAfterFilter() {
        return rowsAfterFilter;
    }

    public void setRowsAfterFilter(int rowsAfterFilter) {
        this.rowsAfterFilter = rowsAfterFilter;
    }

    /**
     * 如果aggregate中含有filter，则记录经过filter之后的行数
     */
    private int rowsAfterFilter;

    private CountDownLatch waitSetJoinTag = new CountDownLatch(1);

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 表名
     */
    private String tableName;
    /**
     * 对应explain analyze的query plan树的节点名称
     */
    private String id;
    /**
     * 记录主键的join tag，第一次访问该节点后设置join tag，后续的访问可以找到之前对应的join tag
     */
    private long joinTag = -1;

    public ExecutionNode(String id, ExecutionNodeType type, int outputRows, String info) {
        this.type = type;
        this.info = info;
        this.id = id;
        this.outputRows = outputRows;
    }

    public ExecutionNode(String id, ExecutionNodeType type, int outputRows, int rowsAfterFilter, String info, List<String> groupKey) {
        this.type = type;
        this.info = info;
        this.id = id;
        this.outputRows = outputRows;
        this.GroupKey = groupKey;
        this.rowsAfterFilter = rowsAfterFilter;
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
    public long getJoinTag() {
        try {
            waitSetJoinTag.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return joinTag;
    }

    public void setJoinTag(long joinTag) {
        waitSetJoinTag.countDown();
        this.joinTag = joinTag;
    }

    public String getInfo() {
        return info;
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

    public List<String> getGroupKey() {
        return GroupKey;
    }

    public void setGroupKey(List<String> groupKey) {
        GroupKey = groupKey;
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
        join,
        /**
         * outerjoin 节点，同时具有左右子节点，只能作为非叶子节点
         */
        outerJoin,
        /**
         * antijoin 节点，同时具有左右子节点，只能作为非叶子节点
         */
        antiJoin,
        /**
         * aggregate 节点，有子节点，只能作为非叶子节点
         */
        aggregate
    }
}
