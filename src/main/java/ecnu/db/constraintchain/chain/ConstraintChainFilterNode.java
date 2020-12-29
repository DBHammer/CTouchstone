package ecnu.db.constraintchain.chain;

import ecnu.db.constraintchain.filter.logical.AndNode;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.utils.exception.TouchstoneException;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFilterNode extends ConstraintChainNode {
    private AndNode root;
    private BigDecimal probability;
    private Set<String> columns;

    public ConstraintChainFilterNode() {
        super(ConstraintChainNodeType.FILTER);
    }

    public ConstraintChainFilterNode(BigDecimal probability, AndNode root, Set<String> columns) {
        super(ConstraintChainNodeType.FILTER);
        this.probability = probability;
        this.root = root;
        this.columns = columns;
    }

    public List<AbstractFilterOperation> pushDownProbability() {
        return root.pushDownProbability(probability, columns);
    }

    public AndNode getRoot() {
        return root;
    }

    public void setRoot(AndNode root) {
        this.root = root;
    }

    public BigDecimal getProbability() {
        return probability;
    }

    public void setProbability(BigDecimal probability) {
        this.probability = probability;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void setColumns(Set<String> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return root.toString();
    }

    public boolean[] evaluate() throws TouchstoneException {
        return root.evaluate();
    }
}
