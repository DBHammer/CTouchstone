package ecnu.db.utils.exception;

import ecnu.db.analyzer.online.select.Token;
import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
public class IllegalArgumentSizeException extends TouchstoneToolChainException {
    public IllegalArgumentSizeException(Token token, String expected, int actual) {
        super(String.format("%s预期有%s个参数，实际有%d个", token, expected, actual));
    }
}
