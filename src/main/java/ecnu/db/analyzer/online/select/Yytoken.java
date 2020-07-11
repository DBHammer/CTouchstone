package ecnu.db.analyzer.online.select;

/**
 * @author alan
 */
class Yytoken {
    public TokenType type;
    public String data;
    public String constantType;

    Yytoken(TokenType type, String data) {
        this.type = type;
        this.data = data;
    }

    Yytoken(TokenType type, String data, String constantType) {
        this.type = type;
        this.data = data;
        this.constantType = constantType;
    }

    @Override
    public String toString() {
        return "Yytoken{" +
                "type=" + type +
                ", data='" + data + '\'' +
                ", constantType='" + constantType + '\'' +
                '}';
    }
}
