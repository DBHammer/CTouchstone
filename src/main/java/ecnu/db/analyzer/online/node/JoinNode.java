package ecnu.db.analyzer.online.node;

import java.util.concurrent.CountDownLatch;

public class JoinNode extends ExecutionNode{
    /**
     * 记录主键的join tag，第一次访问该节点后设置join tag，后续的访问可以找到之前对应的join tag
     */
    private long joinTag = Long.MIN_VALUE;

    private final boolean antiJoin;

    private int pkDistinctSize;

    private final CountDownLatch waitSetJoinTag = new CountDownLatch(1);

    public JoinNode(String id, int outputRows, String info, boolean antiJoin) {
        super(id, ExecutionNodeType.join, outputRows, info);
        this.antiJoin = antiJoin;
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

    public int getPkDistinctSize() {
        return pkDistinctSize;
    }

    public void setPkDistinctSize(int pkDistinctSize) {
        this.pkDistinctSize = pkDistinctSize;
    }
}
