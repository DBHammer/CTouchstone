package ecnu.db.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.constraintchain.arithmetic.value.ColumnNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.exception.compute.InstantiateParameterException;
import ecnu.db.exception.schema.CannotFindColumnException;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;

import static ecnu.db.constraintchain.filter.operation.CompareOperator.*;
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

    private void getCanonicalColumnNamesColNames(ArithmeticNode node, HashSet<String> colNames) {
        if (node == null) {
            return;
        }
        if (node.getType() == ArithmeticNodeType.COLUMN) {
            colNames.add(((ColumnNode) node).getColumnName());
        }
        getCanonicalColumnNamesColNames(node.getLeftNode(), colNames);
        getCanonicalColumnNamesColNames(node.getRightNode(), colNames);
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.MULTI_FILTER_OPERATION;
    }

    @Override
    public boolean[] evaluate() throws CannotFindColumnException {
        double[] data = arithmeticTree.calculate();
        boolean[] ret = new boolean[data.length];
        if (operator == LE) {
            double param = Double.parseDouble(parameters.get(0).getData());
            for (int i = 0; i < data.length; i++) {
                ret[i] = (data[i] <= param);
            }
        } else if (operator == LT) {
            double param = Double.parseDouble(parameters.get(0).getData());
            for (int i = 0; i < data.length; i++) {
                ret[i] = (data[i] < param);
            }
        } else if (operator == GE) {
            double param = Double.parseDouble(parameters.get(0).getData());
            for (int i = 0; i < data.length; i++) {
                ret[i] = (data[i] >= param);
            }
        } else if (operator == GT) {
            double param = Double.parseDouble(parameters.get(0).getData());
            for (int i = 0; i < data.length; i++) {
                ret[i] = (data[i] > param);
            }
        } else {
            throw new UnsupportedOperationException();
        }
        HashSet<String> canonicalColumnNames = new HashSet<>();
        getCanonicalColumnNamesColNames(arithmeticTree, canonicalColumnNames);
        boolean[] nullEvaluations = new boolean[data.length];
        for (String columnName : canonicalColumnNames) {
            AbstractColumn column = ColumnManager.getColumn(columnName);
            boolean[] columnNullEvaluations = column.getIsnullEvaluations();
            for (int i = 0; i < nullEvaluations.length; i++) {
                nullEvaluations[i] = false;
                nullEvaluations[i] = (nullEvaluations[i] | columnNullEvaluations[i]);
            }
        }
        for (int i = 0; i < data.length; i++) {
            ret[i] = (ret[i] & !nullEvaluations[i]);
        }

        return ret;
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
    public void instantiateMultiVarParameter() throws TouchstoneException {
        int pos;
        BigDecimal nonNullProbability = BigDecimal.ONE;
        // 假定null都是均匀独立分布的
        HashSet<String> canonicalColumnNames = new HashSet<>();
        getCanonicalColumnNamesColNames(arithmeticTree, canonicalColumnNames);
        for (String canonicalColumnName : canonicalColumnNames) {
            BigDecimal colNullProbability = BigDecimal.valueOf(ColumnManager.getColumn(canonicalColumnName).getNullPercentage());
            nonNullProbability = nonNullProbability.multiply(BigDecimal.ONE.subtract(colNullProbability));
        }
        if (operator.getType() == GREATER) {
            probability = nonNullProbability.subtract(probability);
        } else if (operator.getType() != LESS) {
            throw new InstantiateParameterException("多变量计算节点仅接受非等值约束");
        }
        float[] vector = arithmeticTree.getVector();
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
