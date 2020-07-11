package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;
import ecnu.db.utils.exception.IllegalTokenException;

/**
 * @author alan
 * 只能接受ISNULL节点,多变量比较节点(in,like)节点作为root子节点
 */
public class NotState extends BaseState {
    public NotState(BaseState preState, SelectNode root) {
        super(preState, root);
    }

    @Override
    public BaseState handle(Token token) throws TouchstoneToolChainException {
        SelectNode newRoot;
        switch (token.type) {
            case ISNULL_OPERATOR:
                newRoot = new SelectNode(token);
                return new IsNullState(this, newRoot);
            case MULTI_COMPARE_OPERATOR:
                newRoot = new SelectNode(token);
                return new MultiCompareState(this, newRoot);
            case RIGHT_PARENTHESIS:
                preState.addArgument(root);
                return preState;
            default:
                throw new IllegalTokenException(token);
        }
    }

    @Override
    public void addArgument(SelectNode node) throws TouchstoneToolChainException {
        root.addChild(node);
    }
}
