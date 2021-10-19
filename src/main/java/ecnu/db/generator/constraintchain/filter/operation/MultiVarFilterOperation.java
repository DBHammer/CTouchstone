package ecnu.db.generator.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.generator.constraintchain.filter.BoolExprNode;
import ecnu.db.generator.constraintchain.filter.arithmetic.ArithmeticNode;
import ecnu.db.generator.constraintchain.filter.arithmetic.ArithmeticNodeType;
import ecnu.db.generator.constraintchain.filter.arithmetic.ColumnNode;
import ecnu.db.generator.constraintchain.filter.BoolExprType;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.schema.CannotFindColumnException;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author wangqingshuai
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MultiVarFilterOperation extends AbstractFilterOperation {
    private ArithmeticNode arithmeticTree;

    public MultiVarFilterOperation() {
        super(null);
    }

    public MultiVarFilterOperation(CompareOperator operator, ArithmeticNode arithmeticTree, List<Parameter> parameters) {
        super(operator);
        this.arithmeticTree = arithmeticTree;
        this.parameters = parameters;
    }

    public Set<String> getAllCanonicalColumnNames() {
        HashSet<String> allTables = new HashSet<>();
        getCanonicalColumnNamesColNames(arithmeticTree, allTables);
        return allTables;
    }

    private void getCanonicalColumnNamesColNames(ArithmeticNode node, HashSet<String> colNames) {
        if (node == null) {
            return;
        }
        if (node.getType() == ArithmeticNodeType.COLUMN) {
            colNames.add(((ColumnNode) node).getCanonicalColumnName());
        }
        getCanonicalColumnNamesColNames(node.getLeftNode(), colNames);
        getCanonicalColumnNamesColNames(node.getRightNode(), colNames);
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.MULTI_FILTER_OPERATION;
    }

    /**
     * todo 暂时不考虑NULL
     *
     * @return 多值表达式的计算结果
     * @throws CannotFindColumnException 计算树中对应的数据列找不到
     */
    @Override
    public boolean[] evaluate() throws CannotFindColumnException {
        double[] data = arithmeticTree.calculate();
        boolean[] ret = new boolean[data.length];
        double parameterValue = (double) parameters.get(0).getData() / CommonUtils.SAMPLE_DOUBLE_PRECISION;
        switch (operator) {
            case LT -> IntStream.range(0, ret.length).parallel().forEach(index -> ret[index] = data[index] < parameterValue);
            case LE -> IntStream.range(0, ret.length).parallel().forEach(index -> ret[index] = data[index] <= parameterValue);
            case GT -> IntStream.range(0, ret.length).parallel().forEach(index -> ret[index] = data[index] > parameterValue);
            case GE -> IntStream.range(0, ret.length).parallel().forEach(index -> ret[index] = data[index] >= parameterValue);
            default -> throw new UnsupportedOperationException();
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
     * todo 暂时不考虑null
     */
    public void instantiateMultiVarParameter() {
        switch (operator) {
            case GE:
            case GT:
                probability = BigDecimal.ONE.subtract(probability);
            case LE:
            case LT:
                break;
            default:
                throw new UnsupportedOperationException("多变量计算节点仅接受非等值约束");
        }
        double[] vector = arithmeticTree.calculate();
        int pos = probability.multiply(BigDecimal.valueOf(vector.length)).intValue();
        Arrays.sort(vector);
        long internalValue = (long) (vector[pos] * CommonUtils.SAMPLE_DOUBLE_PRECISION) / CommonUtils.SAMPLE_DOUBLE_PRECISION;
        parameters.forEach(param -> param.setData(internalValue));
    }
}
