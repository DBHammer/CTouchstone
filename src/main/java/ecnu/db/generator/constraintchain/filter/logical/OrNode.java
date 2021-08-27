package ecnu.db.generator.constraintchain.filter.logical;

import ch.obermuhlner.math.big.BigDecimalMath;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.generator.constraintchain.filter.BoolExprNode;
import ecnu.db.generator.constraintchain.filter.BoolExprType;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.IsNullFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.utils.exception.schema.CannotFindColumnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.IntStream;

import static ecnu.db.generator.constraintchain.filter.operation.UniVarFilterOperation.merge;
import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;

/**
 * @author wangqingshuai
 */
public class OrNode implements BoolExprNode {
    private final Logger logger = LoggerFactory.getLogger(OrNode.class);
    private final BoolExprType type = BoolExprType.OR;
    private BoolExprNode leftNode;
    private BoolExprNode rightNode;

    public BoolExprNode getLeftNode() {
        return leftNode;
    }

    public void setLeftNode(BoolExprNode leftNode) {
        this.leftNode = leftNode;
    }

    public BoolExprNode getRightNode() {
        return rightNode;
    }

    public void setRightNode(BoolExprNode rightNode) {
        this.rightNode = rightNode;
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
    public List<AbstractFilterOperation> pushDownProbability(BigDecimal probability, Set<String> columns) {
        if (probability.compareTo(BigDecimal.ZERO) == 0) {
            logger.info(String.format("'%s'的概率为0", this));
            return new ArrayList<>();
        }

        List<BoolExprNode> otherNodes = new LinkedList<>();
        Multimap<String, UniVarFilterOperation> lessCol2UniFilters = ArrayListMultimap.create();
        Multimap<String, UniVarFilterOperation> greaterCol2UniFilters = ArrayListMultimap.create();
        for (BoolExprNode child : Arrays.asList(leftNode, rightNode)) {
            switch (child.getType()) {
                case AND:
                case OR:
                case MULTI_FILTER_OPERATION:
                    otherNodes.add(child);
                    break;
                case UNI_FILTER_OPERATION:
                    UniVarFilterOperation operation = (UniVarFilterOperation) child;
                    switch (operation.getOperator()) {
                        case GE:
                        case GT:
                            greaterCol2UniFilters.put(operation.getCanonicalColumnName(), operation);
                            break;
                        case LE:
                        case LT:
                            lessCol2UniFilters.put(operation.getCanonicalColumnName(), operation);
                            break;
                        case EQ:
                        case LIKE:
                        case NE:
                        case IN:
                            otherNodes.add(child);
                            break;
                        default:
                            throw new UnsupportedOperationException();
                    }
                    break;
                case ISNULL_FILTER_OPERATION:
                    IsNullFilterOperation isNullFilterOperation = ((IsNullFilterOperation) child);
                    String columnName = isNullFilterOperation.getColumnName();
                    boolean hasNot = isNullFilterOperation.getHasNot();
                    if (columns.contains(columnName)) {
                        if (hasNot && !probability.equals(isNullFilterOperation.getProbability())) {
                            throw new UnsupportedOperationException("or中包含了not(isnull(%s))与其他运算, 总概率不等于not isnull的概率");
                        }
                    } else {
                        BigDecimal nullProbability = isNullFilterOperation.getProbability();
                        BigDecimal toDivide = hasNot ? nullProbability : BigDecimal.ONE.subtract(nullProbability);
                        if (toDivide.compareTo(BigDecimal.ZERO) == 0) {
                            if (probability.compareTo(BigDecimal.ONE) != 0) {
                                throw new UnsupportedOperationException(String.format("'%s'的概率为1而总概率不为1", child));
                            }
                        } else {
                            probability = BigDecimal.ONE.subtract(BigDecimal.ONE.subtract(probability).divide(toDivide, BIG_DECIMAL_DEFAULT_PRECISION));
                        }
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        merge(otherNodes, lessCol2UniFilters, false);
        merge(otherNodes, greaterCol2UniFilters, false);

        probability = BigDecimalMath.pow(BigDecimal.ONE.subtract(probability), BigDecimal.ONE.divide(BigDecimal.valueOf(otherNodes.size()), BIG_DECIMAL_DEFAULT_PRECISION), BIG_DECIMAL_DEFAULT_PRECISION);

        List<AbstractFilterOperation> operations = new LinkedList<>();
        for (BoolExprNode node : otherNodes) {
            operations.addAll(node.pushDownProbability(BigDecimal.ONE.subtract(probability), columns));
        }

        return operations;
    }

    @Override
    public BoolExprType getType() {
        return type;
    }

    @Override
    public boolean[] evaluate() throws CannotFindColumnException {
        boolean[] leftValue = leftNode.evaluate(), rightValue = rightNode.evaluate();
        IntStream.range(0, leftValue.length).parallel().forEach(i -> leftValue[i] |= rightValue[i]);
        return leftValue;
    }


    @Override
    public String toString() {
        return String.format("or(%s, %s)", leftNode.toString(), rightNode.toString());
    }
}
