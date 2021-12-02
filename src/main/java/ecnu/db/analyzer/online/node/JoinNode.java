package ecnu.db.analyzer.online.node;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

public class JoinNode extends ExecutionNode {
    /**
     * 记录主键的join tag，第一次访问该节点后设置join tag，后续的访问可以找到之前对应的join tag
     */
    private long joinTag = Long.MIN_VALUE;

    private final boolean antiJoin;

    private double pkDistinctProbability;

    private long rowsRemoveByFilterAfterJoin;

    private String indexJoinFilter;

    private final CountDownLatch waitSetJoinTag = new CountDownLatch(1);

    public JoinNode(String id, int outputRows, String info, boolean antiJoin, double pkDistinctSize) {
        super(id, ExecutionNodeType.join, outputRows, info);
        this.antiJoin = antiJoin;
        this.pkDistinctProbability = pkDistinctSize;
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
        this.joinTag = joinTag;
        waitSetJoinTag.countDown();
    }

    public boolean isAntiJoin() {
        return antiJoin;
    }

    public double getPkDistinctSize() {
        return pkDistinctProbability;
    }

    public void setPkDistinctSize(int pkDistinctSize) {
        this.pkDistinctProbability = pkDistinctSize;
    }

    public void setIndexJoinFilter(String indexJoinFilter) {
        this.indexJoinFilter = indexJoinFilter;
    }

    public String getIndexJoinFilter() {
        return indexJoinFilter;
    }

    public void setRowsRemoveByFilterAfterJoin(long rowsRemoveByFilterAfterJoin) {
        this.rowsRemoveByFilterAfterJoin = rowsRemoveByFilterAfterJoin;
    }

    public long getRowsRemoveByFilterAfterJoin() {
        return rowsRemoveByFilterAfterJoin;
    }
}
