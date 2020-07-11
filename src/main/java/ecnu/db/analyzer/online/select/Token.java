package ecnu.db.analyzer.online.select;

import java.util.Objects;

/**
 * @author alan
 */
public class Token {
    public TokenType type;
    public String data;
    public String constantType;
    public int line;
    public long begin;

    public Token(Token token) {
        this.type = token.type;
        this.data = token.data;
        this.constantType = token.constantType;
        this.line = token.line;
        this.begin = token.begin;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Token token = (Token) o;
        return type == token.type &&
                Objects.equals(data, token.data) &&
                Objects.equals(constantType, token.constantType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, data, constantType);
    }
}
