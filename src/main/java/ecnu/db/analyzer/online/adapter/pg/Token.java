package ecnu.db.analyzer.online.adapter.pg;

import ecnu.db.analyzer.online.adapter.pg.parser.PgSelectSymbol;
import java_cup.runtime.ComplexSymbolFactory;

public class Token extends ComplexSymbolFactory.ComplexSymbol {
    /**
     * token所在的第一个字符的位置，从当前行开始计数
     */
    private final int column;

    public Token(int type, int column) {
        this(type, column, null);
    }

    public Token(int type, int column, Object value) {
        super(PgSelectSymbol.terminalNames[type].toLowerCase(), type, new ComplexSymbolFactory.Location(1, column), new ComplexSymbolFactory.Location(1, column), value);
        this.column = column;
    }

    @Override
    public String toString() {
        return "column "
                + column
                + ", sym: "
                + PgSelectSymbol.terminalNames[this.sym].toLowerCase()
                + (value == null ? "" : (", value: '" + value + "'"));
    }
}
