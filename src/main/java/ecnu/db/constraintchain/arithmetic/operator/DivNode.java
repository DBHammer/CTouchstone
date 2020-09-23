package ecnu.db.constraintchain.arithmetic.operator;

import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeType;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.schema.Schema;

/**
 * @author wangqingshuai
 */
public class DivNode extends ArithmeticNode {
    public DivNode() {
        super(ArithmeticNodeType.DIV);
    }

    @Override
    public float[] getVector(Schema schema) throws TouchstoneToolChainException {
        float[] leftValue = leftNode.getVector(schema), rightValue = rightNode.getVector(schema);

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
