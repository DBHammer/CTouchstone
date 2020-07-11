package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
public class IsNullState extends BaseState {
    public IsNullState(BaseState preState, SelectNode root) {
        super(preState, root);
    }

    @Override
    public BaseState handle(Token yytoken) throws TouchstoneToolChainException {
        SelectNode newRoot;
        switch (yytoken.type) {
            case CANONICAL_COL_NAME:
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
