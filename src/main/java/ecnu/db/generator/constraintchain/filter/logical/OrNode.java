package ecnu.db.generator.constraintchain.filter.logical;

import ch.obermuhlner.math.big.BigDecimalMath;
import com.fasterxml.jackson.annotation.JsonIgnore;
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

import static ecnu.db.generator.constraintchain.filter.operation.UniVarFilterOperation.merge;
import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;

/**
 * @author wangqingshuai
 */
public class OrNode implements BoolExprNode {
    private final Logger logger = LoggerFactory.getLogger(OrNode.class);
    private static final BoolExprType type = BoolExprType.OR;
    private LinkedList<BoolExprNode> children;

    public OrNode() {
        this.children = new LinkedList<>();
    }

    public void addChild(BoolExprNode logicalNode) {
        children.add(logicalNode);
    }
    /**
     * todo 考虑算子之间的相互依赖
     * todo 考虑or算子中包含isnull算子
     * <p>
     * 算子的概率为P(A or B) = P(!(!A and !B)) = M
     * P(A) = P(B) = 1-Sqrt(1-M)
     *
     * @param probability 当前节点的总概率值
     */
    @Override
    public List<AbstractFilterOperation> pushDownProbability(BigDecimal probability) {
        if (probability.compareTo(BigDecimal.ZERO) == 0) {
            logger.info("{}的概率为0", this);
            return new ArrayList<>();
        }

        List<BoolExprNode> otherNodes = new LinkedList<>();
        Map<String, Collection<UniVarFilterOperation>> lessCol2UniFilters = new HashMap<>();
        Map<String, Collection<UniVarFilterOperation>> greaterCol2UniFilters = new HashMap<>();
        for (BoolExprNode child : children) {
            switch (child.getType()) {
                case AND, OR, MULTI_FILTER_OPERATION -> otherNodes.add(child);
                case UNI_FILTER_OPERATION -> {
                    UniVarFilterOperation operation = (UniVarFilterOperation) child;
                    switch (operation.getOperator()) {
                        case GE, GT -> {
                            if (!greaterCol2UniFilters.containsKey(operation.getCanonicalColumnName())) {
                                greaterCol2UniFilters.put(operation.getCanonicalColumnName(), new ArrayList<>());
                            }
                            greaterCol2UniFilters.get(operation.getCanonicalColumnName()).add(operation);
                        }
                        case LE, LT -> {
                            if (!lessCol2UniFilters.containsKey(operation.getCanonicalColumnName())) {
                                lessCol2UniFilters.put(operation.getCanonicalColumnName(), new ArrayList<>());
                            }
                            lessCol2UniFilters.get(operation.getCanonicalColumnName()).add(operation);
                        }
                        case EQ, LIKE, NE, IN -> otherNodes.add(child);
                        default -> throw new UnsupportedOperationException();
                    }
                }
                case ISNULL_FILTER_OPERATION -> {
                    IsNullFilterOperation isNullFilterOperation = ((IsNullFilterOperation) child);
                    String columnName = isNullFilterOperation.getColumnName();
                    boolean hasNot = isNullFilterOperation.getOperator().equals(CompareOperator.IS_NOT_NULL);
//                    if (columns.contains(columnName)) {
//                        if (hasNot && !probability.equals(isNullFilterOperation.getProbability())) {
//                            throw new UnsupportedOperationException("or中包含了not(isnull(%s))与其他运算, 总概率不等于not isnull的概率");
//                        }
//                    } else {
                        BigDecimal nullProbability = isNullFilterOperation.getProbability();
                        BigDecimal toDivide = hasNot ? nullProbability : BigDecimal.ONE.subtract(nullProbability);
                        if (toDivide.compareTo(BigDecimal.ZERO) == 0) {
                            if (probability.compareTo(BigDecimal.ONE) != 0) {
                                throw new UnsupportedOperationException(String.format("'%s'的概率为1而总概率不为1", child));
                            }
                        } else {
                            probability = BigDecimal.ONE.subtract(BigDecimal.ONE.subtract(probability).divide(toDivide, BIG_DECIMAL_DEFAULT_PRECISION));
                        }
//                    }
                }
                default -> throw new UnsupportedOperationException();
            }
        }

        merge(otherNodes, lessCol2UniFilters, false);
        merge(otherNodes, greaterCol2UniFilters, false);

        probability = BigDecimalMath.pow(BigDecimal.ONE.subtract(probability), BigDecimal.ONE.divide(BigDecimal.valueOf(otherNodes.size()), BIG_DECIMAL_DEFAULT_PRECISION), BIG_DECIMAL_DEFAULT_PRECISION);

        List<AbstractFilterOperation> operations = new LinkedList<>();
        for (BoolExprNode node : otherNodes) {
            operations.addAll(node.pushDownProbability(BigDecimal.ONE.subtract(probability)));
        }

        return operations;
    }

    @JsonIgnore
    @Override
    public List<Parameter> getParameters() {
        return children.stream().map(BoolExprNode::getParameters).flatMap(Collection::stream).toList();
    }

    @Override
    public void reverse() {

    }

    @Override
    public boolean isTrue() {
        return false;
    }

    @Override
    public List<BoolExprNode> initProbability() {
        return null;
    }

    @Override
    public void setType(BoolExprType type) {

    }


    @Override
    public BoolExprType getType() {
        return type;
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
    public String toString() {
        if (children.size() > 1) {
            return String.format("or(%s)", children.stream().map(BoolExprNode::toString).collect(Collectors.joining("," + System.lineSeparator())));
        } else {
            return String.format("or(%s)", children.get(0).toString());
        }
    }
}
