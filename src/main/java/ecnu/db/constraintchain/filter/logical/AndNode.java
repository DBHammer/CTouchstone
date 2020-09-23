package ecnu.db.constraintchain.filter.logical;

import ch.obermuhlner.math.big.BigDecimalMath;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.constraintchain.filter.operation.CompareOperator;
import ecnu.db.constraintchain.filter.operation.IsNullFilterOperation;
import ecnu.db.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.exception.PushDownProbabilityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static ecnu.db.constraintchain.filter.BoolExprType.*;
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
    public List<AbstractFilterOperation> pushDownProbability(BigDecimal probability, Set<String> columns) throws PushDownProbabilityException {
        if (probability.compareTo(BigDecimal.ZERO) == 0) {
            logger.info(String.format("'%s'的概率为0", toString()));
            return new ArrayList<>();
        }

        List<BoolExprNode> otherNodes = new LinkedList<>();
        Multimap<String, UniVarFilterOperation> col2uniFilters = ArrayListMultimap.create();
        for (BoolExprNode child : children) {
            if (child.getType() == AND || child.getType() == OR || child.getType() == MULTI_FILTER_OPERATION) {
                otherNodes.add(child);
            } else if (child.getType() == UNI_FILTER_OPERATION) {
                CompareOperator.TYPE type = ((UniVarFilterOperation) child).getOperator().getType();
                if (type == CompareOperator.TYPE.GREATER || type == CompareOperator.TYPE.LESS) {
                    col2uniFilters.put(((UniVarFilterOperation) child).getColumnName(), (UniVarFilterOperation) child);
                } else if (type == CompareOperator.TYPE.EQUAL) {
                    otherNodes.add(child);
                } else {
                    throw new UnsupportedOperationException();
                }
            } else if (child.getType() == ISNULL_FILTER_OPERATION) {
                String columnName = ((IsNullFilterOperation) child).getColumnName();
                boolean hasNot = ((IsNullFilterOperation) child).getHasNot();
                if (columns.contains(columnName)) {
                    if (!hasNot) {
                        throw new PushDownProbabilityException(String.format("and中包含了isnull(%s)与其他运算, 冲突而总概率不为0", ((IsNullFilterOperation) child).getColumnName()));
                    }
                } else {
                    BigDecimal nullProbability = ((IsNullFilterOperation) child).getProbability();
                    BigDecimal toDivide = hasNot ? BigDecimal.ONE.subtract(nullProbability) : nullProbability;
                    if (toDivide.compareTo(BigDecimal.ZERO) == 0) {
                        throw new PushDownProbabilityException(String.format("'%s'的概率为0而and总概率不为0", child.toString()));
                    } else {
                        probability = probability.divide(toDivide, BIG_DECIMAL_DEFAULT_PRECISION);
                    }
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        UniVarFilterOperation.merge(otherNodes, col2uniFilters, true);
        if (otherNodes.size() != 0) {
            probability = BigDecimalMath.pow(probability, BigDecimal.ONE.divide(BigDecimal.valueOf(otherNodes.size()), BIG_DECIMAL_DEFAULT_PRECISION), BIG_DECIMAL_DEFAULT_PRECISION);
        } else if (probability.compareTo(BigDecimal.ONE) != 0) {
            throw new PushDownProbabilityException(String.format("全部为isnull计算，但去除isnull后的总概率为'%s', 不等于1", probability));
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

    public LinkedList<BoolExprNode> getChildren() {
        return children;
    }

    public void setChildren(LinkedList<BoolExprNode> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        return String.format("and(%s)", children.stream().map(BoolExprNode::toString).collect(Collectors.joining(", ")));
    }
}
