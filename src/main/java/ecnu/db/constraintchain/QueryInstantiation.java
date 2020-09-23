package ecnu.db.constraintchain;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.chain.*;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.constraintchain.filter.operation.MultiVarFilterOperation;
import ecnu.db.constraintchain.filter.operation.RangeFilterOperation;
import ecnu.db.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.utils.CommonUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.*;

/**
 * @author wangqingshuai
 */
public class QueryInstantiation {
    public static void compute(List<ConstraintChain> constraintChains, Map<String, Schema> schemas) throws TouchstoneToolChainException {
        //todo 1. 对于数值型的filter, 首先计算单元的filter, 然后计算多值的filter，
        //        对于bet操作，先记录阈值，然后选择合适的区间插入，等值约束也需选择合适的区间
        //        每个filter operation内部保存自己实例化后的结果
        //     2. 对于字符型的filter, 只有like和eq的运算，直接计算即可
        Multimap<Schema, AbstractFilterOperation> schema2filters = ArrayListMultimap.create();
        for (ConstraintChain constraintChain : constraintChains) {
            Schema schema = schemas.get(constraintChain.getTableName());
            for (ConstraintChainNode node : constraintChain.getNodes()) {
                if (node instanceof ConstraintChainFilterNode) {
                    List<AbstractFilterOperation> operations = ((ConstraintChainFilterNode) node).pushDownProbability();
                    schema2filters.putAll(schema, operations);
                } else if (node instanceof ConstraintChainPkJoinNode) {
                    assert true;
                } else if (node instanceof ConstraintChainFkJoinNode) {
                    assert true;
                } else {
                    throw new UnsupportedOperationException();
                }
            }
        }
        for (Schema schema : schema2filters.keySet()) {
            // uni-var non-eq
            List<UniVarFilterOperation> uniVarFilters = schema2filters.get(schema).stream()
                    .filter((f) -> f instanceof UniVarFilterOperation)
                    .filter((f) -> f.getOperator().getType() == LESS || f.getOperator().getType() == GREATER)
                    .map((f) -> (UniVarFilterOperation) f)
                    .collect(Collectors.toList());
            for (UniVarFilterOperation operation : uniVarFilters) {
                String columnName = CommonUtils.extractSimpleColumnName(operation.getColumnName());
                AbstractColumn column = schema.getColumn(columnName);
                operation.instantiateUniParamCompParameter(column);
            }
            // init eq bucket
            schema.getColumns().forEach((s2, col) -> col.initEqProbabilityBucket());
            // uni-var eq
            uniVarFilters = schema2filters.get(schema).stream()
                    .filter((f) -> f instanceof UniVarFilterOperation)
                    .filter((f) -> f.getOperator().getType() == EQUAL)
                    .map((f) -> (UniVarFilterOperation) f)
                    .collect(Collectors.toList());
            for (UniVarFilterOperation operation : uniVarFilters) {
                String columnName = CommonUtils.extractSimpleColumnName(operation.getColumnName());
                AbstractColumn column = schema.getColumn(columnName);
                operation.instantiateEqualParameter(column);
            }
            // uni-var bet
            List<RangeFilterOperation> rangeFilters = schema2filters.get(schema).stream()
                    .filter((f) -> f instanceof RangeFilterOperation)
                    .map((f) -> (RangeFilterOperation) f)
                    .collect(Collectors.toList());
            for (RangeFilterOperation operation : rangeFilters) {
                String columnName = CommonUtils.extractSimpleColumnName(operation.getColumnName());
                AbstractColumn column = schema.getColumn(columnName);
                operation.instantiateBetweenParameter(column);
            }
            // init eq params
            schema.getColumns().values().forEach(AbstractColumn::initEqParameter);
            // multi-var non-eq
            List<MultiVarFilterOperation> multiVarFilters = schema2filters.get(schema).stream()
                    .filter((f) -> f instanceof MultiVarFilterOperation)
                    .map((f) -> (MultiVarFilterOperation) f)
                    .collect(Collectors.toList());
            for (MultiVarFilterOperation operation : multiVarFilters) {
                operation.instantiateMultiVarParameter(schema);
            }
        }
    }

}
