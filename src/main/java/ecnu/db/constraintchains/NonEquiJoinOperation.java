package ecnu.db.constraintchains;

import java.util.ArrayList;
import java.util.List;

public class NonEquiJoinOperation {

    private final int id;

    // multiple basic operations are not supported currently
    // TODO
    private String expression = null;
    private String operator = null;

    // 'probability' is the probability over the whole domain space (inputDataSize)
    private final float probability;

    // to simplify implementation, 'inputDataSize' is input manually
    private final float inputDataSize;

    // the id of child nodes
    private List<Integer> children = null;

    public NonEquiJoinOperation(int id, String expression, String operator, float probability, float inputDataSize) {
        this.id = id;
        this.expression = expression;
        this.operator = operator;
        this.probability = probability;
        this.inputDataSize = inputDataSize;
        this.children = new ArrayList<Integer>();
    }

    public int getId() {
        return id;
    }

    public String getExpression() {
        return expression;
    }

    public String getOperator() {
        return operator;
    }

    public float getProbability() {
        return probability;
    }

    public float getInputDataSize() {
        return inputDataSize;
    }

    public List<Integer> getChildren() {
        return children;
    }

    @Override
    public String toString() {
        return "\n\tNonEquiJoinConstraint [id=" + id + ", expression=" + expression + ", operator=" + operator
                + ", probability=" + probability + ", inputDataSize=" + inputDataSize + ", children=" + children + "]";
    }
}
