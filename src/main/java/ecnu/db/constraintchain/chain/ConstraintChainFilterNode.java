package ecnu.db.constraintchain.chain;

import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFilterNode extends ConstraintChainNode {
    /**
     * 静态变量，用于标记filterOperation的序号，通过序号可以填充query模版
     */
    private static final int maxFilterId = 0;
    private AbstractFilterOperation[] abstractFilterOperations;

    public ConstraintChainFilterNode(String tableName, String constraintChainInfo) {
        super(tableName, ConstraintChainNodeType.Filter);
        //todo 解析constraintChainInfo
        // 如果是一元的FilterOperation 构造为UnitFilterOperation
        // 如果是多元的FilterOperation 构造为MultipleFilterOperation
        // 如果存在and和or需要使用logicalNode构建逻辑树，计算每个FilterOperation的概率
    }
}
