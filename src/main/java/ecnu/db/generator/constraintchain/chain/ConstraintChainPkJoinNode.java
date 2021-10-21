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
    
    @Override
    public String toString() {
        return String.format("{pkTag:%d,pkColumns:%s}", pkTag, Arrays.toString(pkColumns));
    }

    public String[] getPkColumns() {
        return pkColumns;
    }

    public long getPkTag() {
        return pkTag;
    }

    public void setPkTag(long pkTag) {
        this.pkTag = pkTag;
    }
}
