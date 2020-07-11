package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
class LogicalState extends State {
    public LogicalState(State preState, SelectNode root) {
        super(preState, root);
    }

    @Override
    public State handle(Yytoken yytoken) throws TouchstoneToolChainException {
        SelectNode newRoot;
        switch (yytoken.type) {
            case LOGICAL_OPERATOR:
                newRoot = new SelectNode(yytoken);
                return new LogicalState(this, newRoot);
            case UNI_COMPARE_OPERATOR:
                newRoot = new SelectNode(yytoken);
                return new UniCompareState(this, newRoot);
            case MULTI_COMPARE_OPERATOR:
                newRoot = new SelectNode(yytoken);
                return new MultiCompareState(this, newRoot);
            case ISNULL_OPERATOR:
                newRoot = new SelectNode(yytoken);
                return new IsNullState(this, newRoot);
            case NOT_OPERATOR:
                newRoot = new SelectNode(yytoken);
                return new NotState(this, newRoot);
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
