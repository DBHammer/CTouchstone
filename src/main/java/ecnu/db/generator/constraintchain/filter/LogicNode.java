package ecnu.db.generator.constraintchain.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.CompareOperator;
import ecnu.db.generator.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.schema.ColumnManager;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

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
        List<List<AbstractFilterOperation>> operationsOfTrees = new ArrayList<>();
        // 如果当前节点为 OR 且 概率不为 1， 则反转该树为AND
        boolean hasReverse = false;
        if (getRealType() == OR) {
            this.reverse();
            hasReverse = true;
            probability = BigDecimal.ONE.subtract(probability);
        }

        boolean allTrue = true;
        List<Integer> onlyNoEqlOperations = new ArrayList<>();
        for (int index = 0; index < children.size(); index++) {
            BoolExprNode child = children.get(index);
            if (child.isTrue()) {
                List<AbstractFilterOperation> subOperations = child.pushDownProbability(BigDecimal.ONE);
                if (subOperations.stream().noneMatch(operation -> operation.getOperator().isEqual())) {
                    onlyNoEqlOperations.add(index);
                }
                operationsOfTrees.add(subOperations);
            } else {
                allTrue = false;
                operationsOfTrees.add(child.pushDownProbability(probability));
            }
        }
        if (allTrue && probability.compareTo(BigDecimal.ONE) < 0) {
            int index = onlyNoEqlOperations.isEmpty() ? children.size() - 1 : onlyNoEqlOperations.remove(0);
            operationsOfTrees.set(index, children.get(index).pushDownProbability(probability));
        }
        if (hasReverse) {
            this.reverse();
        }
        return operationsOfTrees.stream().flatMap(Collection::stream).toList();
    }

    @Override
    public void getColumn2ParameterBucket(Map<String, Map<String, List<Integer>>> column2Value2ParameterList, String predicate) {
        for (BoolExprNode child : children) {
            if (child instanceof LogicNode || child instanceof UniVarFilterOperation) {
                child.getColumn2ParameterBucket(column2Value2ParameterList, predicate);
            }
        }
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
    public boolean[] evaluate() {
        boolean[][] computeVectors = new boolean[children.size()][];
        for (int i = 0; i < children.size(); i++) {
            computeVectors[i] = children.get(i).evaluate();
        }
        boolean[] resultVector = computeVectors[0];
        if (getRealType() == AND) {
            for (int i = 0; i < resultVector.length; i++) {
                for (int j = 1; j < computeVectors.length; j++) {
                    resultVector[i] &= computeVectors[j][i];
                }
            }
        } else if (getRealType() == OR) {
            for (int i = 0; i < resultVector.length; i++) {
                for (int j = 1; j < computeVectors.length; j++) {
                    resultVector[i] |= computeVectors[j][i];
                }
            }
        }
        return resultVector;
    }

    @JsonIgnore
    public boolean isRangePredicate() {
        // range predicate的条件为2个1UCC
        if (type != AND) {
            return false;
        }
        if (children.size() == 1 && children.get(0) instanceof LogicNode logicNode && logicNode.type == AND) {
            return logicNode.isRangePredicate();
        } else if (children.size() == 2 && children.stream().allMatch(UniVarFilterOperation.class::isInstance)) {
            List<UniVarFilterOperation> uniVarFilterOperations = children.stream()
                    .map(UniVarFilterOperation.class::cast).collect(Collectors.toList());
            // 两个UCC的列名必须一致
            if (uniVarFilterOperations.get(0).getCanonicalColumnName().equals(uniVarFilterOperations.get(1).getCanonicalColumnName()) &&
                    //2个UCC不能包含等值操作符
                    uniVarFilterOperations.stream().map(AbstractFilterOperation::getOperator).noneMatch(CompareOperator::isEqual)) {
                // 如果后一个为>符号则交换
                if (uniVarFilterOperations.get(1).getOperator().isBigger()) {
                    Collections.swap(children, 0, 1);
                    Collections.swap(uniVarFilterOperations, 0, 1);
                }
                // 交换后应该为小于号
                return !uniVarFilterOperations.get(1).getOperator().isBigger();
            }
        }
        return false;
    }

    @JsonIgnore
    public List<AbstractFilterOperation> getRangeOperations() {
        LogicNode logicNode = (LogicNode) children.get(0);
        return logicNode.children.stream().map(AbstractFilterOperation.class::cast).toList();
    }

    @JsonIgnore
    public String generateRangeRightBoundPredicate() {
        LogicNode logicNode = (LogicNode) children.get(0);
        return logicNode.children.get(1).toString();
    }

    @JsonIgnore
    public void setRangeProbability(BigDecimal rangeProbability, BigDecimal rightBoundProbability) {
        LogicNode logicNode = (LogicNode) children.get(0);
        List<BoolExprNode> rangeOperations = logicNode.children;
        String columnName = ((UniVarFilterOperation) rangeOperations.get(0)).getCanonicalColumnName();
        BigDecimal nullProbability = ColumnManager.getInstance().getNullPercentage(columnName);
        BigDecimal validProbability = BigDecimal.ONE.subtract(nullProbability);
        BigDecimal leftLessThanBound = rightBoundProbability.subtract(rangeProbability);
        BigDecimal leftBoundProbability = validProbability.subtract(leftLessThanBound);
        rangeOperations.get(0).pushDownProbability(leftBoundProbability);
        rangeOperations.get(1).pushDownProbability(rightBoundProbability);
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
    public BigDecimal getNullProbability() {
        BigDecimal maxNull = BigDecimal.ZERO;
        if (type == AND) {
            for (BoolExprNode child : children) {
                maxNull = maxNull.max(child.getNullProbability());
            }
        } else if (type == OR) {
            for (BoolExprNode child : children) {
                if (child.getFilterProbability().compareTo(BigDecimal.ZERO) > 0) {
                    maxNull = maxNull.max(child.getNullProbability());
                }
            }
        } else {
            throw new UnsupportedOperationException();
        }
        return maxNull;
    }

    @Override
    public BigDecimal getFilterProbability() {
        if (type == AND) {
            BigDecimal minProbability = BigDecimal.ONE;
            for (BoolExprNode child : children) {
                minProbability = minProbability.min(child.getFilterProbability());
            }
            return minProbability;
        } else if (type == OR) {
            BigDecimal maxProbability = BigDecimal.ZERO;
            for (BoolExprNode child : children) {
                maxProbability = maxProbability.max(child.getFilterProbability());
            }
            return maxProbability;
        } else {
            throw new UnsupportedOperationException();
        }
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

    private void randomMoveOperations(List<UniVarFilterOperation> operationWithProbability) {
        // 如果只有一个operation，则不需要移动range
        if (operationWithProbability.size() == 1) {
            return;
        }
        UniVarFilterOperation validOperation = operationWithProbability.stream().
                filter(AbstractFilterOperation::probabilityValid).findFirst().orElse(null);
        if (validOperation == null) {
            throw new IllegalStateException();
        }
        boolean isBigger = validOperation.getOperator().isBigger();
        // 找到operation操作符与其相反的其他operation
        List<UniVarFilterOperation> pairOperations = operationWithProbability.stream()
                .filter(operation -> operation.getOperator().isBigger() != isBigger).toList();
        // 如果不存在则返回
        if (pairOperations.isEmpty()) {
            return;
        }
        // 随机移动range
        BigDecimal nullProbability = ColumnManager.getInstance().getNullPercentage(validOperation.getCanonicalColumnName());
        double remainingBound = BigDecimal.ONE.subtract(nullProbability).subtract(validOperation.getProbability()).doubleValue();
        remainingBound = Math.min(0.5, remainingBound);
        BigDecimal moveProbability = BigDecimal.valueOf(ThreadLocalRandom.current().nextDouble(remainingBound));
        validOperation.setProbability(validOperation.getProbability().add(moveProbability));
        BigDecimal boundProbability = BigDecimal.ONE.subtract(nullProbability).subtract(moveProbability);
        for (UniVarFilterOperation pairOperation : pairOperations) {
            if (pairOperation.getProbability().compareTo(BigDecimal.ONE) != 0) {
                throw new IllegalStateException();
            }
            pairOperation.setProbability(boundProbability);
        }
    }

    @Override
    public void randomMoveRangePredicate() {
        if (type == AND) {
            // 按照列名对一元的operation分组
            Map<String, List<UniVarFilterOperation>> columnName2Operation = children.stream()
                    .filter(UniVarFilterOperation.class::isInstance).map(UniVarFilterOperation.class::cast)
                    .filter(uniVarFilterOperation -> !uniVarFilterOperation.getOperator().isEqual())
                    .collect(Collectors.groupingBy(UniVarFilterOperation::getCanonicalColumnName));
            // 找到有概率约束的组
            List<List<UniVarFilterOperation>> validOperations = columnName2Operation.values().stream()
                    .filter(operations -> operations.stream().anyMatch(AbstractFilterOperation::probabilityValid)).toList();
            if (validOperations.isEmpty()) {
                children.stream().filter(LogicNode.class::isInstance).map(LogicNode.class::cast)
                        .forEach(LogicNode::randomMoveRangePredicate);
            } else if (validOperations.size() == 1) {
                randomMoveOperations(validOperations.get(0));
            } else {
                throw new IllegalStateException();
            }
        } else if (type != OR) {
            throw new UnsupportedOperationException();
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
            return "(" + children.stream().map(BoolExprNode::toString).collect(Collectors.joining(" " + lowerType + System.lineSeparator())) + ")";
        } else {
            return children.get(0).toString();
        }
    }
}
