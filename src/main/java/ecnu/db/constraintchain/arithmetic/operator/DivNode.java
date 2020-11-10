package ecnu.db.constraintchain.arithmetic.operator;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.exception.schema.CannotFindColumnException;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.schema.Schema;

/**
 * @author wangqingshuai
 * todo 处理rightValue可能出现的0或很小的值
 */
public class DivNode extends ArithmeticNode {
    public DivNode() {
        super(ArithmeticNodeType.DIV);
    }

    @Override
    public float[] getVector(Schema schema) throws TouchstoneException {
        float[] leftValue = leftNode.getVector(schema), rightValue = rightNode.getVector(schema);
        for (int i = 0; i < leftValue.length; i++) {
            leftValue[i] /= rightValue[i];
        }
        return leftValue;
    }

    @Override
    public double[] calculate(Schema schema, int size) throws CannotFindColumnException {
        double[] leftValue = leftNode.calculate(schema, size), rightValue = rightNode.calculate(schema, size);
        for (int i = 0; i < leftValue.length; i++) {
            leftValue[i] /= rightValue[i];
        }
        return leftValue;
    }

    @Override
    public String toString() {
        return String.format("div(%s, %s)", leftNode.toString(), rightNode.toString());
    }
}
