package ecnu.db.analyzer.online.select;

import ecnu.db.analyzer.online.select.tidb.TidbSelectSymbol;
import java_cup.runtime.ComplexSymbolFactory;

/**
 * @author alan
 * lexer返回的token
 */
public class Token extends ComplexSymbolFactory.ComplexSymbol {
    /**
     * token所在的行
     */
    private int line;
    /**
     * token所在的第一个字符的位置，从当前行开始计数
     */
    private int column;

    public Token(int type, int line, int column) {
        this(type, line, column, null);
    }

    public Token(int type, int line, int column, Object value) {
        super(TidbSelectSymbol.terminalNames[type].toLowerCase(), type, new ComplexSymbolFactory.Location(line, column), new ComplexSymbolFactory.Location(line, column), value);
        this.line = line;
        this.column = column;
    }

    @Override
    public String toString() {
        return "line "
                + line
                + ", column "
                + column
                + ", sym: "
                + TidbSelectSymbol.terminalNames[this.sym].toLowerCase()
                + (value == null ? "" : (", value: '" + value + "'"));
    }
}
