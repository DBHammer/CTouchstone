package ecnu.db.analyzer.online.select;

import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
public abstract class BaseState {
    protected BaseState preState;
    protected SelectNode root;

    public BaseState(BaseState preState, SelectNode root) {
        this.preState = preState;
        this.root = root;
    }

    /**
     * 状态转移
     *
     * @param token 需要处理的token
     * @return 转移后的状态
     * @throws TouchstoneToolChainException 不符合语法
     */
    public abstract BaseState handle(Token token) throws TouchstoneToolChainException;

    /**
     * 为root添加参数节点
     *
     * @param node 被添加的参数节点
     * @throws TouchstoneToolChainException 添加失败
     */
    public abstract void addArgument(SelectNode node) throws TouchstoneToolChainException;
}
