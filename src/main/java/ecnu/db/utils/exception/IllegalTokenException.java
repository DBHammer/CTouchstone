package ecnu.db.utils.exception;

import ecnu.db.analyzer.online.select.Token;
import ecnu.db.utils.TouchstoneToolChainException;

/**
 * @author alan
 */
public class IllegalTokenException extends TouchstoneToolChainException {
    public IllegalTokenException(Token token) {
        super(String.format("非法的token %s", token));
    }
}
