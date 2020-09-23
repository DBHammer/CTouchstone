package ecnu.db.constraintchain.arithmetic;

import ecnu.db.constraintchain.arithmetic.operator.DivNode;
import ecnu.db.constraintchain.arithmetic.operator.MinusNode;
import ecnu.db.constraintchain.arithmetic.operator.MulNode;
import ecnu.db.constraintchain.arithmetic.operator.PlusNode;
import ecnu.db.constraintchain.arithmetic.value.ColumnNode;
import ecnu.db.constraintchain.arithmetic.value.NumericNode;
import ecnu.db.exception.TouchstoneToolChainException;

/**
 * @author alan
 */
public class ArithmeticNodeFactory {
    public static ArithmeticNode create(ArithmeticNodeType type) throws TouchstoneToolChainException {
        ArithmeticNode node;
        switch (type) {
            case DIV:
                node = new DivNode();
                break;
            case MUL:
                node = new MulNode();
                break;
            case PLUS:
                node = new PlusNode();
                break;
            case MINUS:
                node = new MinusNode();
                break;
            case CONSTANT:
                node = new NumericNode();
                break;
            case COLUMN:
                node = new ColumnNode();
                break;
            default:
                throw new TouchstoneToolChainException(String.format("未识别的ArithmeticNodeType %s", type));
        }

        return node;
    }
}
