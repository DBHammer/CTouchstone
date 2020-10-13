package ecnu.db.exception;

/**
 * @author alan
 */
public class CircularReferenceException extends TouchstoneException {
    public CircularReferenceException() {
        super("circular reference of tables");
    }
}
