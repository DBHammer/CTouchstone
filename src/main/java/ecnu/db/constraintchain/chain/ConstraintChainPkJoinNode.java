package ecnu.db.constraintchain.chain;

import java.util.Arrays;

/**
 * @author wangqingshuai
 */
public class ConstraintChainPkJoinNode extends ConstraintChainNode {
    private String[] pkColumns;
    private int pkTag;

    public ConstraintChainPkJoinNode() {
        super(null, ConstraintChainNodeType.PK_JOIN);
    }

    public ConstraintChainPkJoinNode(String tableName, int pkTag, String[] pkColumns) {
        super(tableName, ConstraintChainNodeType.PK_JOIN);
        this.pkTag = pkTag;
        this.pkColumns = pkColumns;
    }

    /**
     * 构建ConstraintChainPkJoinNode对象
     *
     * @param constraintChainInfo 获取到的约束链信息
     */
    public ConstraintChainPkJoinNode(String tableName, String constraintChainInfo) {
        super(tableName, ConstraintChainNodeType.PK_JOIN);
        //todo 解析constraintChainInfo
    }

    @Override
    public String toString() {
        return String.format("{pkTag:%d,pkColumns:%s}", pkTag, Arrays.toString(pkColumns));
    }

    public String[] getPkColumns() {
        return pkColumns;
    }

    public int getPkTag() {
        return pkTag;
    }
}
