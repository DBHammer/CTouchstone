package ecnu.db.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.constraintchain.arithmetic.value.ColumnNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.exception.InstantiateParameterException;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.Schema;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;

import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.GREATER;
import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.LESS;

/**
 * @author wangqingshuai
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MultiVarFilterOperation extends AbstractFilterOperation {
    private ArithmeticNode arithmeticTree;

    public MultiVarFilterOperation() {
        super(null);
    }

    public MultiVarFilterOperation(CompareOperator operator, ArithmeticNode arithmeticTree) {
        super(operator);
        this.arithmeticTree = arithmeticTree;
    }

    /**
     * 获取计算树访问的所有列名
     *
     * @return 计算树访问的所有列名
     */
    public HashSet<String> getColNames() {
        HashSet<String> colNames = new HashSet<>();
        getColNames(arithmeticTree, colNames);
        return colNames;
    }

    private void getColNames(ArithmeticNode node, HashSet<String> colNames) {
        if (node == null) {
            return;
        }
        if (node.getType() == ArithmeticNodeType.COLUMN) {
            colNames.add(String.format("%s.%s", ((ColumnNode) node).getCanonicalTableName(), ((ColumnNode) node).getColumnName()));
        }
        getColNames(node.getLeftNode(), colNames);
        getColNames(node.getRightNode(), colNames);
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.MULTI_FILTER_OPERATION;
    }

    @Override
    public String toString() {
        return String.format("%s(%s, %s)", operator.toString().toLowerCase(),
                arithmeticTree.toString(),
                parameters.stream().map(Parameter::toString).collect(Collectors.joining(", ")));
    }

    public ArithmeticNode getArithmeticTree() {
        return arithmeticTree;
    }

    public void setArithmeticTree(ArithmeticNode arithmeticTree) {
        this.arithmeticTree = arithmeticTree;
    }

    /**
     * todo 通过计算树计算概率，暂时不考虑其他FilterOperation对于此操作的阈值影响
     */
    public void instantiateMultiVarParameter(Schema schema) throws TouchstoneToolChainException {
        int pos;
        BigDecimal nonNullProbability = BigDecimal.ONE;
        // 假定null都是均匀独立分布的
        for (String columnName : getColNames()) {
            String simpleColumnName = CommonUtils.extractSimpleColumnName(columnName);
            BigDecimal colNullProbability = BigDecimal.valueOf(schema.getColumn(simpleColumnName).getNullPercentage());
            nonNullProbability = nonNullProbability.multiply(BigDecimal.ONE.subtract(colNullProbability));
        }
        if (operator.getType() == GREATER) {
            probability = nonNullProbability.subtract(probability);
        } else if (operator.getType() != LESS) {
            throw new InstantiateParameterException("多变量计算节点仅接受非等值约束");
        }
        float[] vector = arithmeticTree.getVector(schema);
        pos = probability.multiply(BigDecimal.valueOf(vector.length)).intValue();
        Arrays.sort(vector);
        parameters.forEach(param -> {
            if (CommonUtils.isInteger(param.getData())) {
                param.setData(Integer.toString((int) vector[pos]));
            } else if (CommonUtils.isFloat(param.getData())) {
                param.setData(Float.toString(vector[pos]));
            } else {
                throw new UnsupportedOperationException();
            }
        });
    }
}
