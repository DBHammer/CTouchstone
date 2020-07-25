package ecnu.db.constraintchain.chain;

/**
 * @author wangqingshuai
 * 约束链节点的类型
 */

public enum ConstraintChainNodeType {
    /**
     * 过滤节点
     */
    Filter,
    /**
     * join操作中的主键节点
     */
    PkJoin,
    /**
     * join操作中的外键节点
     */
    FkJoin
}
