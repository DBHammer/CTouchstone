package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;
import ecnu.db.utils.exception.IllegalTokenException;

/**
 * @author alan
 * 只能接受常量节点,列名节点,算术节点作为root子节点
 */
public class ArithmeticState extends BaseState {
    public ArithmeticState(BaseState preState, SelectNode root) {
        super(preState, root);
    }

    @Override
    public BaseState handle(Token token) throws TouchstoneToolChainException {
        SelectNode newRoot;
        switch (token.type) {
            case ARITHMETIC_OPERATOR:
                newRoot = new SelectNode(token);
                return new ArithmeticState(this, newRoot);
            case CANONICAL_COL_NAME:
            case CONSTANT:
                newRoot = new SelectNode(token);
                this.addArgument(newRoot);
                return this;
            case RIGHT_PARENTHESIS:
                preState.addArgument(this.root);
                return preState;
            default:
                throw new IllegalTokenException(token);
        }
    }

    @Override
    public void addArgument(SelectNode node) {
        root.addChild(node);
    }
}
