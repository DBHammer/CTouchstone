package ecnu.db.generator.constraintchain.filter.logical;

import ch.obermuhlner.math.big.BigDecimalMath;
import ecnu.db.generator.constraintchain.filter.BoolExprNode;
import ecnu.db.generator.constraintchain.filter.BoolExprType;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
import ecnu.db.generator.constraintchain.filter.operation.IsNullFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.utils.exception.schema.CannotFindColumnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;

/**
 * @author wangqingshuai
 */
public class AndNode implements BoolExprNode {
    private final Logger logger = LoggerFactory.getLogger(AndNode.class);
    private BoolExprType type = BoolExprType.AND;
    private LinkedList<BoolExprNode> children;

    public AndNode() {
        this.children = new LinkedList<>();
    }

    public void addChild(BoolExprNode logicalNode) {
        children.add(logicalNode);
    }

    /**
     * todo 当前And计算概率时，未考虑单值算子和多值算子的相互影响
     * <p>
     * todo 当前And计算概率时，假设可以合并的operation在同一组children中
     *
     * @param probability 当前节点的总概率值
     */
    @Override
    public List<AbstractFilterOperation> pushDownProbability(BigDecimal probability, Set<String> columns) {
        if (probability.compareTo(BigDecimal.ZERO) == 0) {
            logger.info("{}的概率为0", this);
            return new ArrayList<>();
        }
        List<BoolExprNode> otherNodes = new LinkedList<>();
        Map<String, Collection<UniVarFilterOperation>> col2uniFilters = new HashMap<>();
        for (BoolExprNode child : children) {
            switch (child.getType()) {
                case AND, OR, MULTI_FILTER_OPERATION -> otherNodes.add(child);
                case UNI_FILTER_OPERATION -> {
                    UniVarFilterOperation uvfChild = (UniVarFilterOperation) child;
                    CompareOperator operator = uvfChild.getOperator();
                    switch (operator) {
                        case GE, GT, LE, LT -> {
                            if (!col2uniFilters.containsKey(uvfChild.getCanonicalColumnName())) {
                                col2uniFilters.put(uvfChild.getCanonicalColumnName(), new ArrayList<>());
                            }
                            col2uniFilters.get((uvfChild.getCanonicalColumnName())).add(uvfChild);
                        }
                        case EQ, NE, LIKE, NOT_LIKE, IN, NOT_IN -> otherNodes.add(child);
                        default -> throw new UnsupportedOperationException();
                    }
                }
                case ISNULL_FILTER_OPERATION -> {
                    String columnName = ((IsNullFilterOperation) child).getColumnName();
                    boolean hasNot = ((IsNullFilterOperation) child).getOperator().equals(CompareOperator.IS_NOT_NULL);
                    if (columns.contains(columnName)) {
                        if (!hasNot) {
                            throw new UnsupportedOperationException(String.format("and中包含了isnull(%s)与其他运算, 冲突而总概率不为0", ((IsNullFilterOperation) child).getColumnName()));
                        }
                    } else {
                        BigDecimal nullProbability = ((IsNullFilterOperation) child).getProbability();
                        BigDecimal toDivide = hasNot ? BigDecimal.ONE.subtract(nullProbability) : nullProbability;
                        if (toDivide.compareTo(BigDecimal.ZERO) == 0) {
                            throw new UnsupportedOperationException(String.format("'%s'的概率为0而and总概率不为0", child));
                        } else {
                            probability = probability.divide(toDivide, BIG_DECIMAL_DEFAULT_PRECISION);
                        }
                    }
                }
                default -> throw new UnsupportedOperationException();
            }
        }

        UniVarFilterOperation.merge(otherNodes, col2uniFilters, true);

        if (!otherNodes.isEmpty()) {
            probability = BigDecimalMath.pow(probability, BigDecimal.ONE.divide(BigDecimal.valueOf(otherNodes.size()), BIG_DECIMAL_DEFAULT_PRECISION), BIG_DECIMAL_DEFAULT_PRECISION);
        } else if (probability.compareTo(BigDecimal.ONE) != 0) {
            throw new UnsupportedOperationException(String.format("全部为isnull计算，但去除isnull后的总概率为'%s', 不等于1", probability));
        }

        List<AbstractFilterOperation> operations = new LinkedList<>();
        for (BoolExprNode node : otherNodes) {
            operations.addAll(node.pushDownProbability(probability, columns));
        }

        return operations;
    }

    @Override
    public BoolExprType getType() {
        return type;
    }

    public void setType(BoolExprType type) {
        this.type = type;
    }

    @Override
    public boolean[] evaluate() throws CannotFindColumnException {
        boolean[][] computeVectors = new boolean[children.size()][];
        for (int i = 0; i < children.size(); i++) {
            computeVectors[i] = children.get(i).evaluate();
        }
        boolean[] resultVector = new boolean[computeVectors[0].length];
        Arrays.fill(resultVector, true);
        for (boolean[] computeVector : computeVectors) {
            IntStream.range(0, resultVector.length).parallel().forEach(i -> resultVector[i] &= computeVector[i]);
        }
        return resultVector;
    }

    @Override
    public List<Parameter> getParameters() {
        return children.stream().map(BoolExprNode::getParameters).flatMap(Collection::stream).toList();
    }

    public LinkedList<BoolExprNode> getChildren() {
        return children;
    }

    public void setChildren(LinkedList<BoolExprNode> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        if (children.size() > 1) {
            return String.format("and(%s)", children.stream().map(BoolExprNode::toString).collect(Collectors.joining("," + System.lineSeparator())));
        } else {
            return String.format("and(%s)", children.get(0).toString());
        }
    }
}
