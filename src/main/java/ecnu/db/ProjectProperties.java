package ecnu.db;

import java.io.IOException;
import java.util.Properties;

/**
 * @author alan
 */
public class ProjectProperties {
    String getVersion() throws IOException {
        final Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream("project.properties"));
        return properties.getProperty("version");
    }
}
