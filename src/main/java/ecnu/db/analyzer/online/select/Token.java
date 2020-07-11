package ecnu.db.analyzer.online.select;

/**
 * @author alan
 */
public class Token {
    public TokenType type;
    public String data;
    public String constantType;
    public int line;
    public long begin;

    public Token(TokenType type, String data, int line, long begin) {
        this.type = type;
        this.data = data;
        this.line = line;
        this.begin = begin;
    }

    public Token(TokenType type, String data, String constantType, int line, long begin) {
        this.type = type;
        this.data = data;
        this.constantType = constantType;
        this.line = line;
        this.begin = begin;
    }

    @Override
    public String toString() {
        return "Token{" +
                "type=" + type +
                ", data='" + data + '\'' +
                ", constantType='" + constantType + '\'' +
                '}' + String.format(" at line:%d, pos:%d", line, begin);
    }
}
