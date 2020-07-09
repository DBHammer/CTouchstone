package ecnu.db.analyzer.online.select;

/**
 * @author alan
 */
class Yytoken {
    public TokenType type;
    public String data;

    Yytoken(TokenType type, String data) {
        this.type = type;
        this.data = data;
    }

    @Override
    public String toString() {
        return "Yytoken{" +
                "type=" + type +
                ", data='" + data + '\'' +
                '}';
    }
}
