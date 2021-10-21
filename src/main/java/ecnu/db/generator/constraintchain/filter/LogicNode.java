package ecnu.db.generator.constraintchain.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.utils.exception.schema.CannotFindColumnException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ecnu.db.generator.constraintchain.filter.BoolExprType.AND;
import static ecnu.db.generator.constraintchain.filter.BoolExprType.OR;

public class LogicNode implements BoolExprNode {
    private BoolExprType type;
    private List<BoolExprNode> children;

    /**
     * 是否在化简的过程中被reverse过，默认为false
     */
    private boolean isReverse = false;

    public void setType(BoolExprType type) {
        this.type = type;
    }

    public List<BoolExprNode> getChildren() {
        return children;
    }

    public void setChildren(List<BoolExprNode> children) {
        this.children = children;
    }


    public List<AbstractFilterOperation> pushDownProbability(BigDecimal probability) {
        List<AbstractFilterOperation> operations = new ArrayList<>();
        // 如果上层要求设置概率为 1， 则不需要处理，直接下推概率1到所有的filter operation中
        if (probability.compareTo(BigDecimal.ONE) == 0) {
            for (BoolExprNode child : children) {
                operations.addAll(child.pushDownProbability(BigDecimal.ONE));
            }
        } else {
            // 如果当前节点为 OR 且 概率不为 1， 则反转该树为AND
            if (getRealType() == OR) {
                this.reverse();
                probability = BigDecimal.ONE.subtract(probability);
            }

            boolean allTrue = true;
            for (BoolExprNode child : children) {
                if (child.isTrue()) {
                    operations.addAll(child.pushDownProbability(BigDecimal.ONE));
                } else {
                    allTrue = false;
                    operations.addAll(child.pushDownProbability(probability));
                }
            }
            if (allTrue) {
                operations.removeAll(children.get(0).pushDownProbability(BigDecimal.ONE));
                operations.addAll(0, children.get(0).pushDownProbability(probability));
            }
        }
        return operations;
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
        if (getRealType() == AND) {
            Arrays.stream(computeVectors).forEach(
                    computeVector -> IntStream.range(0, resultVector.length)
                            .forEach(i -> resultVector[i] &= computeVector[i]));
        } else if (getRealType() == OR) {
            Arrays.stream(computeVectors).forEach(
                    computeVector -> IntStream.range(0, resultVector.length)
                            .forEach(i -> resultVector[i] |= computeVector[i]));
        }
        return resultVector;
    }

    @JsonIgnore
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
