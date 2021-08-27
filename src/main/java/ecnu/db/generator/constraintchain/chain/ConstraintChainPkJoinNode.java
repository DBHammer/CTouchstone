package ecnu.db.generator.constraintchain.chain;

import java.util.Arrays;

/**
 * @author wangqingshuai
 */
public class ConstraintChainPkJoinNode extends ConstraintChainNode {
    private String[] pkColumns;
    private long pkTag;

    public ConstraintChainPkJoinNode() {
        super(ConstraintChainNodeType.PK_JOIN);
    }

    public ConstraintChainPkJoinNode(long pkTag, String[] pkColumns) {
        super(ConstraintChainNodeType.PK_JOIN);
        this.pkTag = pkTag;
        this.pkColumns = pkColumns;
    }

    /**
     * 构建ConstraintChainPkJoinNode对象
     *
     * @param constraintChainInfo 获取到的约束链信息
     */
    public ConstraintChainPkJoinNode(String constraintChainInfo) {
        super(ConstraintChainNodeType.PK_JOIN);
        //todo 解析constraintChainInfo
    }

    @Override
    public String toString() {
        return String.format("{pkTag:%d,pkColumns:%s}", pkTag, Arrays.toString(pkColumns));
    }

    public String[] getPkColumns() {
        return pkColumns;
    }

    public void setPkTag(long pkTag) {
        this.pkTag = pkTag;
    }

    public long getPkTag() {
        return pkTag;
    }
}
