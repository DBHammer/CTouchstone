package ecnu.db.constraintchain.arithmetic;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author wangqingshuai
 */
public interface ArithmeticNode {
    /**
     * 获取当前节点的计算结果
     * @return 返回float类型的计算结果
     * @throws TouchstoneToolChainException 无法获取值向量
     */
    float[] getValue() throws TouchstoneToolChainException;
}
