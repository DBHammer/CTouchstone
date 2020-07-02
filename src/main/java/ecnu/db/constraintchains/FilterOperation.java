package ecnu.db.constraintchains;

import java.util.List;
import java.util.Map;

public class FilterOperation {
    private final int id;
    private final float probability;
    private final transient List<String> attrNames = null;
    private final transient List<String> values = null;
    private final String expression;
    private final String operator;

    public FilterOperation(int id, String expression, String operator, float probability) {
        super();
        this.id = id;
        this.expression = expression;
        this.operator = operator;
        this.probability = probability;
    }

    public FilterOperation(FilterOperation filterOperation) {
        super();
        this.id = filterOperation.id;
        this.expression = filterOperation.expression;
        this.operator = filterOperation.operator;
        this.probability = filterOperation.probability;
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


    /**
     * 给定的参数是否满足该filter
     *
     * @param attributeValueMap 传入的参数集合
     * @return 是否满足该filter
     */
    public boolean isSatisfied(Map<String, String> attributeValueMap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "\n\t\tFilterOperation [id=" + id + ", expression=" + expression + ", operator=" + operator
                + ", probability=" + probability + "]";
    }

}
