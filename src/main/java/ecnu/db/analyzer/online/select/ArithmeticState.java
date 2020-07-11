package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
public class ArithmeticState extends State {
    public ArithmeticState(State preState, SelectNode root) {
        super(preState, root);
    }

    @Override
    public State handle(Yytoken yytoken) throws TouchstoneToolChainException {
        SelectNode newRoot;
        switch (yytoken.type) {
            case ARITHMETIC_OPERATOR:
                newRoot = new SelectNode(yytoken);
                return new ArithmeticState(this, newRoot);
            case CANONICAL_COL_NAME:
            case CONSTANT:
                newRoot = new SelectNode(yytoken);
                this.addArgument(newRoot);
                return this;
            case RIGHT_PARENTHESIS:
                preState.addArgument(this.root);
                return preState;
            default:
                throw new TouchstoneToolChainException(String.format("非法的token %s", yytoken));
        }
    }

    @Override
    public void addArgument(SelectNode node) {
        root.addChild(node);
    }
}
