package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
public class NotState extends State {
    public NotState(State preState, SelectNode root) {
        super(preState, root);
    }

    @Override
    public State handle(Yytoken yytoken) throws TouchstoneToolChainException {
        SelectNode newRoot;
        switch (yytoken.type) {
            case ISNULL_OPERATOR:
                newRoot = new SelectNode(yytoken);
                return new IsNullState(this, newRoot);
            case MULTI_COMPARE_OPERATOR:
                newRoot = new SelectNode(yytoken);
                return new MultiCompareState(this, newRoot);
            case RIGHT_PARENTHESIS:
                preState.addArgument(root);
                return preState;
            default:
                throw new TouchstoneToolChainException(String.format("非法的token %s", yytoken));
        }
    }

    @Override
    public void addArgument(SelectNode node) throws TouchstoneToolChainException {
        root.addChild(node);
    }
}
