package ecnu.db.analyzer.online.select;

import ecnu.db.analyzer.online.select.tidb.TidbSelectSymbol;
import java_cup.runtime.ComplexSymbolFactory;

/**
 * @author alan
 * lexer返回的token
 */
public class Token extends ComplexSymbolFactory.ComplexSymbol {
    /**
     * token所在的第一个字符的位置，从当前行开始计数
     */
    private int column;

    public Token(int type, int column) {
        this(type, column, null);
    }

    public Token(int type, int column, Object value) {
        super(TidbSelectSymbol.terminalNames[type].toLowerCase(), type, new ComplexSymbolFactory.Location(1, column), new ComplexSymbolFactory.Location(1, column), value);
        this.column = column;
    }

    @Override
    public String toString() {
        return "column "
                + column
                + ", sym: "
                + TidbSelectSymbol.terminalNames[this.sym].toLowerCase()
                + (value == null ? "" : (", value: '" + value + "'"));
    }
}
