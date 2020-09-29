package ecnu.db.exception;

/**
 * @author alan
 */
public class CircularReferenceException extends TouchstoneToolChainException {
    public CircularReferenceException() {
        super("circular reference of tables");
    }
}
