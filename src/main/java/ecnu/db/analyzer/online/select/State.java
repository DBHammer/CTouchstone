package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
abstract class State {
    protected State preState;
    protected SelectNode root;
    public abstract State handle(Yytoken yytoken) throws TouchstoneToolChainException;
    public abstract void addArgument(SelectNode node) throws TouchstoneToolChainException;
    public State(State preState, SelectNode root) {
        this.preState = preState;
        this.root = root;
    }
}
