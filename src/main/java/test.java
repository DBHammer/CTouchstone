import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class test {
    private static Logger logger = LoggerFactory.getLogger(test.class);
    public static void main(String[] args) {
        logger.debug("======debug");
        logger.info("======info");
        logger.warn("======warn");
        logger.error("======error");
    }
} 