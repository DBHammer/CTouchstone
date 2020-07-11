package ecnu.db.analyzer.online.select;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author alan
 */
public class SelectNode {
    private Token token;
    private List<SelectNode> children = new ArrayList<>();
    public SelectNode(Token token) {
        this.token = token;
    }
    public void addChild(SelectNode node) {
        this.children.add(node);
    }

    public Token getToken() {
        return token;
    }

    public List<SelectNode> getChildren() {
        return children;
    }

    public void setChildren(List<SelectNode> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        if (token.type == TokenType.CONSTANT || token.type == TokenType.CANONICAL_COL_NAME) {
            if ("STRING".equals(token.constantType)) {
                return String.format("\"%s\"", token.data);
            }
            return token.data;
        }
        String arguments = children.stream().map(SelectNode::toString).collect(Collectors.joining(", "));
        String str = String.format("%s(%s)", token.data, arguments);
        return str;
    }

    public void setToken(Token token) {
        this.token = token;
    }
}
