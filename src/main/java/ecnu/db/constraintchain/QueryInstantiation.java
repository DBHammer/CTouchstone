package ecnu.db.constraintchain;

import ecnu.db.constraintchain.chain.*;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.constraintchain.filter.operation.MultiVarFilterOperation;
import ecnu.db.constraintchain.filter.operation.RangeFilterOperation;
import ecnu.db.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.column.AbstractColumn;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.*;

/**
 * @author wangqingshuai
 */
public class QueryInstantiation {
    public static void compute(List<ConstraintChain> constraintChains) throws TouchstoneException {
        //todo 1. 对于数值型的filter, 首先计算单元的filter, 然后计算多值的filter，
        //        对于bet操作，先记录阈值，然后选择合适的区间插入，等值约束也需选择合适的区间
        //        每个filter operation内部保存自己实例化后的结果
        //     2. 对于字符型的filter, 只有like和eq的运算，直接计算即可
        List<AbstractFilterOperation> filterOperations = new LinkedList<>();
        for (ConstraintChain constraintChain : constraintChains) {
            for (ConstraintChainNode node : constraintChain.getNodes()) {
                if (node instanceof ConstraintChainFilterNode) {
                    List<AbstractFilterOperation> operations = ((ConstraintChainFilterNode) node).pushDownProbability();
                    filterOperations.addAll(operations);
                } else if (node instanceof ConstraintChainPkJoinNode) {
                    assert true;
                } else if (node instanceof ConstraintChainFkJoinNode) {
                    assert true;
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        }

        // uni-var non-eq
        List<UniVarFilterOperation> uniVarFilters = filterOperations.stream()
                .filter((f) -> f instanceof UniVarFilterOperation)
                .filter((f) -> f.getOperator().getType() == LESS || f.getOperator().getType() == GREATER)
                .map((f) -> (UniVarFilterOperation) f)
                .collect(Collectors.toList());
        for (UniVarFilterOperation operation : uniVarFilters) {
            AbstractColumn column = ColumnManager.getInstance().getColumn(operation.getCanonicalColumnName());
            operation.instantiateUniParamCompParameter(column);
        }
        ColumnManager.getInstance().initAllEqProbabilityBucket();
        // uni-var eq
        uniVarFilters = filterOperations.stream()
                .filter((f) -> f instanceof UniVarFilterOperation)
                .filter((f) -> f.getOperator().getType() == EQUAL)
                .map((f) -> (UniVarFilterOperation) f)
                .collect(Collectors.toList());
        for (UniVarFilterOperation operation : uniVarFilters) {
            AbstractColumn column = ColumnManager.getInstance().getColumn(operation.getCanonicalColumnName());
            operation.instantiateEqualParameter(column);
        }
        // uni-var bet
        List<RangeFilterOperation> rangeFilters = filterOperations.stream()
                .filter((f) -> f instanceof RangeFilterOperation)
                .map((f) -> (RangeFilterOperation) f)
                .collect(Collectors.toList());

        for (RangeFilterOperation operation : rangeFilters) {
            AbstractColumn column = ColumnManager.getInstance().getColumn(operation.getCanonicalColumnName());
            operation.instantiateBetweenParameter(column);
        }
        // init eq params
        ColumnManager.getInstance().initAllEqParameter();
        // multi-var non-eq
        List<MultiVarFilterOperation> multiVarFilters = filterOperations.stream()
                .filter((f) -> f instanceof MultiVarFilterOperation)
                .map((f) -> (MultiVarFilterOperation) f)
                .collect(Collectors.toList());
        for (MultiVarFilterOperation operation : multiVarFilters) {
            operation.instantiateMultiVarParameter();
        }
    }
}


