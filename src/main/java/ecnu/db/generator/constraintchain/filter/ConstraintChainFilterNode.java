package ecnu.db.generator.constraintchain.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ecnu.db.generator.constraintchain.ConstraintChainNode;
import ecnu.db.generator.constraintchain.ConstraintChainNodeType;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.utils.exception.schema.CannotFindColumnException;

import java.math.BigDecimal;
import java.util.List;

/**
 * @author wangqingshuai
 */
public class ConstraintChainFilterNode extends ConstraintChainNode {
    private LogicNode root;
    private BigDecimal probability;

    public ConstraintChainFilterNode() {
        super(ConstraintChainNodeType.FILTER);
    }

    public ConstraintChainFilterNode(BigDecimal probability, LogicNode root) {
        super(ConstraintChainNodeType.FILTER);
        this.probability = probability.stripTrailingZeros();
        this.root = root;
    }

    public List<String> getColumns(){
        return root.getColumns();
    }

    @JsonIgnore
    public boolean hasKeyColumn() {
        return root.hasKeyColumn();
    }

    public List<AbstractFilterOperation> pushDownProbability() {
        return root.pushDownProbability(probability);
    }

    @JsonIgnore
    public List<Parameter> getParameters() {
        return root.getParameters();
    }

    public LogicNode getRoot() {
        return root;
    }

    public void setRoot(LogicNode root) {
        this.root = root;
    }

    public BigDecimal getProbability() {
        return probability;
    }

    public void setProbability(BigDecimal probability) {
        this.probability = probability.stripTrailingZeros();
    }

    @Override
    public String toString() {
        return root.toString();
    }

    public boolean[] evaluate() throws CannotFindColumnException {
        return root.evaluate();
    }
}
