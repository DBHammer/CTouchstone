package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;
import ecnu.db.utils.exception.IllegalTokenException;

/**
 * @author alan
 */
public class ArithmeticState extends BaseState {
    public ArithmeticState(BaseState preState, SelectNode root) {
        super(preState, root);
    }

    @Override
    public BaseState handle(Token yytoken) throws TouchstoneToolChainException {
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
                throw new IllegalTokenException(yytoken);
        }
    }

    @Override
    public void addArgument(SelectNode node) {
        root.addChild(node);
    }
}
