package ecnu.db.generator.constraintchain.filter;

import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
import ecnu.db.utils.exception.schema.CannotFindColumnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ecnu.db.generator.constraintchain.filter.BoolExprType.AND;
import static ecnu.db.generator.constraintchain.filter.BoolExprType.OR;

public class LogicNode implements BoolExprNode {
    private final Logger logger = LoggerFactory.getLogger(LogicNode.class);
    private BoolExprType type;
    private List<BoolExprNode> children;

    /**
     * 是否在化简的过程中被reverse过，默认为false
     */
    public boolean isReverse = false;

    public List<BoolExprNode> getChildren() {
        return children;
    }

    public void setChildren(List<BoolExprNode> children) {
        this.children = children;
    }

    public LogicNode(boolean isAnd) {
        type = isAnd ? AND : OR;
        this.children = new LinkedList<>();
    }

    public void addChild(BoolExprNode logicalNode) {
        children.add(logicalNode);
    }


    public List<AbstractFilterOperation> pushDownProbability(BigDecimal probability) {
        if (probability.compareTo(BigDecimal.ZERO) == 0) {
            logger.info("{}的概率为0", this);
            return new ArrayList<>();
        }
        List<BoolExprNode> simpleExpr = this.initProbability();
        List<AbstractFilterOperation> result = new ArrayList<>();
        for (BoolExprNode child : simpleExpr) {
            AbstractFilterOperation a = (AbstractFilterOperation) child;
            if (a.getOperator() == CompareOperator.EQ || a.getOperator() == CompareOperator.IN || a.getOperator() == CompareOperator.LIKE) {
                a.setProbability(probability);
            }
            result.add(a);
        }
        return result;
    }

    @Override
    public BoolExprType getType() {
        return this.type;
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

    @Override
    public void reverse() {
        if (type == AND || type == OR) {
            isReverse = !isReverse;
        } else {
            throw new UnsupportedOperationException();
        }
        for (BoolExprNode child : children) {
            child.reverse();
        }
    }

    @Override
    public boolean isTrue() {
        return switch (getRealType()) {
            case AND -> children.stream().allMatch(BoolExprNode::isTrue);
            case OR -> children.stream().anyMatch(BoolExprNode::isTrue);
            default -> throw new UnsupportedOperationException();
        };
    }

    private BoolExprType getRealType() {
        if (isReverse) {
            return switch (type) {
                case AND -> OR;
                case OR -> AND;
                default -> throw new UnsupportedOperationException();
            };
        } else {
            return type;
        }
    }

    @Override
    public List<BoolExprNode> initProbability() {
        if (getRealType() == OR) {
            this.reverse();
        }
        List<BoolExprNode> simpleExpr = new ArrayList<>();
        boolean alltrue = true;
        for (BoolExprNode child : children) {
            if (!child.isTrue()) {
                alltrue = false;
                if (child.getType() != AND && child.getType() != OR) {
                    simpleExpr.add(child);
                } else {
                    simpleExpr.addAll(child.initProbability());
                }
            }
        }
        if (alltrue) {
            for (BoolExprNode child : children) {
                if (type == BoolExprType.UNI_FILTER_OPERATION) {
                    simpleExpr.add(child);
                }
            }
            simpleExpr.addAll(this.children.get(0).initProbability());
        }
        return simpleExpr;
    }

    @Override
    public String toString() {
        String lowerType = switch (type) {
            case AND -> "and";
            case OR -> "or";
            default -> throw new UnsupportedOperationException();
        };
        if (children.size() > 1) {
            return String.format("%s(%s)", lowerType, children.stream().map(BoolExprNode::toString).collect(Collectors.joining("," + System.lineSeparator())));
        } else {
            return String.format("%s(%s)", lowerType, children.get(0).toString());
        }
    }
}
