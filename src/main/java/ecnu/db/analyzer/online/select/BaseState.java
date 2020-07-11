package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
public abstract class BaseState {
    protected BaseState preState;
    protected SelectNode root;
    public abstract BaseState handle(Token yytoken) throws TouchstoneToolChainException;
    public abstract void addArgument(SelectNode node) throws TouchstoneToolChainException;
    public BaseState(BaseState preState, SelectNode root) {
        this.preState = preState;
        this.root = root;
    }
}
