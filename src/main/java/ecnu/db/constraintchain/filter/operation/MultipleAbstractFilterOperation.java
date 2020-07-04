package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;

/**
 * @author wangqingshuai
 */
public class MultipleAbstractFilterOperation extends AbstractFilterOperation {
    ArithmeticNode arithmeticTree;


    public MultipleAbstractFilterOperation(int id, CompareOperator operator) {
        super(id, operator);
        //todo 构建计算树
    }

    /**
     * todo 通过计算树计算概率，暂时不考虑其他FilterOperation对于此操作的阈值影响
     */
    @Override
    public void calculateParameter() {

    }
}
