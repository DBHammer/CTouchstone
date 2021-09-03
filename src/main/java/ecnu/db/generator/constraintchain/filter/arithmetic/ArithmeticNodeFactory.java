package ecnu.db.generator.constraintchain.filter.arithmetic;

/**
 * @author alan
 */
public class ArithmeticNodeFactory {

    private ArithmeticNodeFactory() {
    }

    public static ArithmeticNode create(ArithmeticNodeType type) {
        return switch (type) {
            case DIV -> new MathNode(ArithmeticNodeType.DIV);
            case MUL -> new MathNode(ArithmeticNodeType.MUL);
            case PLUS -> new MathNode(ArithmeticNodeType.PLUS);
            case MINUS -> new MathNode(ArithmeticNodeType.MINUS);
            case CONSTANT -> new NumericNode();
            case COLUMN -> new ColumnNode();
        };
    }
}
