package ecnu.db.generator.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.generator.constraintchain.filter.BoolExprType;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.arithmetic.ArithmeticNode;
import ecnu.db.generator.constraintchain.filter.arithmetic.ArithmeticNodeType;
import ecnu.db.generator.constraintchain.filter.arithmetic.ColumnNode;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.schema.CannotFindColumnException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
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

    private static double select(double[] a, int l, int r, int k) {
        if (r - l < 75) {
            insertSort(a, l, r);    //用快速排序进行排序
            return a[l + k - 1];
        }
        int group = (r - l + 5) / 5;
        for (int i = 0; i < group; i++) {
            int left = l + 5 * i;
            int right = Math.min((l + i * 5 + 4), r);  //如果超出右边界就用右边界赋值
            int mid = (left + right) / 2;
            insertSort(a, left, right);
            swap(a, l + i, mid);     // 将各组中位数与前i个
        }
        double pivot = select(a, l, l + group - 1, (group + 1) / 2);  //找出中位数的中位数
        int p = partition(a, l, r, pivot);    //用中位数的中位数作为基准的位置
        int leftNum = p - l;       //leftNum用来记录基准位置的前边的元素个数
        if (k == leftNum + 1)
            return a[p];
        else if (k <= leftNum)
            return select(a, l, p - 1, k);
        else                    //若k在基准位子的后边，则要从基准位置的后边数起，即第（k - leftNum - 1）个
            return select(a, p + 1, r, k - leftNum - 1);
    }

    private static int partition(double[] a, int l, int r, double pivot) {   //适用于线性时间选择的partition方法
        int i = l;
        int j = r;
        while (true) {
            while (a[i] <= pivot && i < r)
                ++i;   //i一直向后移动，直到出现a[i]>pivot
            while (a[j] > pivot)
                --j;   //j一直向前移动，直到出现a[j]<pivot
            if (i >= j) break;
            swap(a, i, j);
        }
        a[l] = a[j];
        a[j] = pivot;
        return j;
    }

    private static void insertSort(double[] a, int law, int high) {    //插入排序
        for (int i = law + 1; i <= high; i++) {
            double key = a[i];
            int j = i - 1;
            while (j >= law && a[j] > key) {
                a[j + 1] = a[j];
                j--;
            }
            a[j + 1] = key;
        }
    }

    private static void swap(double[] a, int i, int j) {
        double temp = a[i];
        a[i] = a[j];
        a[j] = temp;
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
    public boolean hasKeyColumn() {
        return hasKeyColumn(arithmeticTree);
    }

    @Override
    public void getColumn2ParameterBucket(Map<String, Map<String, List<Integer>>> column2Value2ParameterList) {
    }

    private boolean hasKeyColumn(ArithmeticNode node) {
        boolean hasKeyColumn = false;
        if (node != null) {
            hasKeyColumn = hasKeyColumn(node.getLeftNode()) || hasKeyColumn(node.getRightNode());
            if (node.getType() == ArithmeticNodeType.COLUMN) {
                ColumnNode columnNode = (ColumnNode) node;
                hasKeyColumn = hasKeyColumn ||
                        TableManager.getInstance().isPrimaryKey(columnNode.getCanonicalColumnName()) ||
                        TableManager.getInstance().isForeignKey(columnNode.getCanonicalColumnName());
            }
        }
        return hasKeyColumn;
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.MULTI_FILTER_OPERATION;
    }

    /**
     * todo 暂时不考虑NULL
     *
     * @return 多值表达式的计算结果
     */
    @Override
    public boolean[] evaluate() {
        double[] data = arithmeticTree.calculate();
        boolean[] ret = new boolean[data.length];
        double parameterValue = parameters.get(0).getData();
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
    public List<String> getColumns() {
        return arithmeticTree.getColumns();
    }

    @JsonIgnore
    @Override
    public boolean isDifferentTable(String tableName) {
        return arithmeticTree.isDifferentTable(tableName);
    }

    @Override
    public String toSQL() {
        return arithmeticTree.toSQL() + CompareOperator.toSQL(operator) + parameters.get(0).getDataValue();
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
     * todo 暂时不考虑null
     */
    public void instantiateMultiVarParameter() {
        switch (operator) {
            case GE, GT:
                probability = BigDecimal.ONE.subtract(probability);
                break;
            case LE, LT:
                break;
            default:
                throw new UnsupportedOperationException("多变量计算节点仅接受非等值约束");
        }
        long start = System.currentTimeMillis();
        double[] vector = arithmeticTree.calculate();
        start = System.currentTimeMillis();
        int pos;
        if (probability.equals(BigDecimal.ONE)) {
            pos = vector.length - 1;
        } else {
            pos = probability.multiply(BigDecimal.valueOf(vector.length)).setScale(0, RoundingMode.HALF_UP).intValue();
        }
        double PosthSmallestNumber = select(vector, 0, vector.length - 1, pos + 1);
        long internalValue = (long) (PosthSmallestNumber * CommonUtils.SAMPLE_DOUBLE_PRECISION) / CommonUtils.SAMPLE_DOUBLE_PRECISION;
        parameters.forEach(param -> param.setData(internalValue));
        //todo check parameter type
        parameters.forEach(param -> param.setDataValue("interval '" + internalValue + "' day"));
    }
}
