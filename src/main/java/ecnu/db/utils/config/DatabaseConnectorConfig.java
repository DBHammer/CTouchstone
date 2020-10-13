package ecnu.db.utils.config;

/**
 * @author alan
 */
public interface DatabaseConnectorConfig {
    public String getDatabaseIp();
    public String getDatabasePort();
    public String getDatabaseUser();
    public String getDatabasePwd();
    public String getDatabaseName();
    public Boolean isCrossMultiDatabase();

}
