package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;
import ecnu.db.utils.exception.IllegalTokenException;

/**
 * @author alan
 */
public class LogicalState extends BaseState {
    public LogicalState(BaseState preState, SelectNode root) {
        super(preState, root);
    }

    @Override
    public BaseState handle(Token token) throws TouchstoneToolChainException {
        SelectNode newRoot;
        switch (token.type) {
            case LOGICAL_OPERATOR:
                newRoot = new SelectNode(token);
                return new LogicalState(this, newRoot);
            case UNI_COMPARE_OPERATOR:
                newRoot = new SelectNode(token);
                return new UniCompareState(this, newRoot);
            case MULTI_COMPARE_OPERATOR:
                newRoot = new SelectNode(token);
                return new MultiCompareState(this, newRoot);
            case ISNULL_OPERATOR:
                newRoot = new SelectNode(token);
                return new IsNullState(this, newRoot);
            case NOT_OPERATOR:
                newRoot = new SelectNode(token);
                return new NotState(this, newRoot);
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
