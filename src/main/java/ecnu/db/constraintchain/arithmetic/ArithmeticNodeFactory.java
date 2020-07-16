package ecnu.db.constraintchain.arithmetic;

import ecnu.db.constraintchain.arithmetic.operator.DivNode;
import ecnu.db.constraintchain.arithmetic.operator.MinusNode;
import ecnu.db.constraintchain.arithmetic.operator.MulNode;
import ecnu.db.constraintchain.arithmetic.operator.PlusNode;
import ecnu.db.constraintchain.arithmetic.value.ColumnNode;
import ecnu.db.constraintchain.arithmetic.value.NumericNode;
import ecnu.db.utils.TouchstoneToolChainException;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * @author alan
 */
public class ArithmeticNodeFactory {
    public static ArithmeticNode create(@NonNull ArithmeticNodeType type) throws TouchstoneToolChainException {
        ArithmeticNode node = null;
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
            case PARAMETER:
                node = new ColumnNode();
                break;
            default:
                throw new TouchstoneToolChainException("未识别的ArithmeticNodeType");
        }

        return node;
    }
}
