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

public class LogicNode extends BoolExprNode {
    private BoolExprType type;
    private List<BoolExprNode> children;

    public List<BoolExprNode> getChildren() {
        return children;
    }

    public void setChildren(List<BoolExprNode> children) {
        this.children = children;
    }

    @Override
    public boolean hasKeyColumn() {
        return children.stream().anyMatch(BoolExprNode::hasKeyColumn);
    }

    @Override
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

    public void removeOtherTablesOperation(String tableName) {
        BoolExprNode root = this.children.get(0);
        if (root.getType() == AND) {
            LogicNode andRoot = (LogicNode) root;
            List<BoolExprNode> prepareForRemove = andRoot.getChildren().stream()
                    .filter(child -> child.isDifferentTable(tableName)).toList();
            if (prepareForRemove.size() > 1) {
                throw new UnsupportedOperationException();
            }
            andRoot.getChildren().removeAll(prepareForRemove);
        }
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
        if (getRealType() == AND) {
            Arrays.fill(resultVector, true);
            Arrays.stream(computeVectors).forEach(
                    computeVector -> IntStream.range(0, resultVector.length)
                            .forEach(i -> resultVector[i] &= computeVector[i]));
        } else if (getRealType() == OR) {
            Arrays.fill(resultVector, false);
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

    @JsonIgnore
    @Override
    public List<String> getColumns() {
        return children.stream().map(BoolExprNode::getColumns).flatMap(Collection::stream).toList();
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

    @Override
    public boolean isDifferentTable(String tableName) {
        return children.stream().anyMatch(child -> child.isDifferentTable(tableName));
    }

    @Override
    public String toSQL() {
        String lowerType = switch (type) {
            case AND -> "and";
            case OR -> "or";
            default -> throw new UnsupportedOperationException();
        };
        if (children.size() > 1) {
            return "(" + children.stream().map(BoolExprNode::toSQL).collect(Collectors.joining(" " + lowerType + " ")) + ")";
        } else {
            return children.get(0).toSQL();
        }
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
