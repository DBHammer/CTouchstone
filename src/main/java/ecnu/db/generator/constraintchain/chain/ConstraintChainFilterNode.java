package ecnu.db.generator.constraintchain.chain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ecnu.db.generator.constraintchain.filter.BoolExprType;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.LogicNode;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.utils.exception.TouchstoneException;

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
        this.probability = probability;
        this.root = root;
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
        this.probability = probability;
    }

    @Override
    public String toString() {
        return root.toString();
    }

    public boolean[] evaluate() throws TouchstoneException {
        return root.evaluate();
    }
}
