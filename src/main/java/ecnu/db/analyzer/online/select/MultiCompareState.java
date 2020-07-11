package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;
import ecnu.db.utils.exception.IllegalTokenException;

/**
 * @author alan
 */
public class MultiCompareState extends BaseState {
    public MultiCompareState(BaseState preState, SelectNode root) {
        super(preState, root);
    }

    @Override
    public BaseState handle(Token token) throws TouchstoneToolChainException {
        SelectNode newRoot;
        switch (token.type) {
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
