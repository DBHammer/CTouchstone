package ecnu.db.analyzer.online.select;
import java_cup.runtime.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static ecnu.db.analyzer.online.select.tidb.TidbSelectSymbol.*;

/**
 * @author alan
 */
public class SelectNode {
    private int symbolID;
    private Symbol symbol;
    private List<SelectNode> children = new ArrayList<>();

    public SelectNode(int symbolID, Symbol symbol) {
        this.symbolID = symbolID;
        this.symbol = symbol;
    }

    public void addChild(SelectNode node) {
        this.children.add(node);
    }

    public int getSymbolID() {
        return symbolID;
    }

    public Symbol getSymbol() {
        return symbol;
    }

    public List<SelectNode> getChildren() {
        return children;
    }

    public void setChildren(List<SelectNode> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        if (symbolID == CANONICAL_COLUMN_NAME || symbolID == STRING || symbolID == INTEGER || symbolID == FLOAT) {
            return String.format("%s(%s)", terminalNames[symbolID].toLowerCase(), symbol.value);
        }
        String arguments = children.stream().map(SelectNode::toString).collect(Collectors.joining(", "));

        return String.format("%s(%s)", terminalNames[symbolID].toLowerCase(), arguments);
    }
}
