package ecnu.db.analyzer.online.node;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

public class JoinNode extends ExecutionNode {
    private final boolean antiJoin;
    private final boolean semiJoin;
    private final CountDownLatch waitSetJoinTag = new CountDownLatch(1);
    /**
     * 记录主键的join tag，第一次访问该节点后设置join tag，后续的访问可以找到之前对应的join tag
     */
    private int joinTag = Integer.MIN_VALUE;
    private final BigDecimal pkDistinctProbability;
    private long rowsRemoveByFilterAfterJoin;
    private String indexJoinFilter;

    public JoinNode(String id, long outputRows, String info, boolean antiJoin, boolean semiJoin, BigDecimal pkDistinctProbability) {
        super(id, ExecutionNodeType.join, outputRows, info);
        this.antiJoin = antiJoin;
        this.semiJoin = semiJoin;
        this.pkDistinctProbability = pkDistinctProbability;
    }

    /**
     * @return 当前表最新的join tag
     */
    public int getJoinTag() {
        try {
            waitSetJoinTag.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        return joinTag;
    }

    public void setJoinTag(int joinTag) {
        this.joinTag = joinTag;
        waitSetJoinTag.countDown();
    }

    public boolean isAntiJoin() {
        return antiJoin;
    }

    public BigDecimal getPkDistinctSize() {
        return pkDistinctProbability;
    }

    public String getIndexJoinFilter() {
        return indexJoinFilter;
    }

    public void setIndexJoinFilter(String indexJoinFilter) {
        this.indexJoinFilter = indexJoinFilter;
    }

    public boolean isSemiJoin() {
        return semiJoin;
    }

    public long getRowsRemoveByFilterAfterJoin() {
        return rowsRemoveByFilterAfterJoin;
    }

    public void setRowsRemoveByFilterAfterJoin(long rowsRemoveByFilterAfterJoin) {
        this.rowsRemoveByFilterAfterJoin = rowsRemoveByFilterAfterJoin;
    }
}
