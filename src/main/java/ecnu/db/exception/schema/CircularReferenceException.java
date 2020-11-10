package ecnu.db.exception.schema;

import ecnu.db.exception.TouchstoneException;

/**
 * @author alan
 */
public class CircularReferenceException extends TouchstoneException {
    public CircularReferenceException() {
        super("circular reference of tables");
    }
}
