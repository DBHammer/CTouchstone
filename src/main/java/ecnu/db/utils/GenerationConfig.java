package ecnu.db.utils;

/**
 * @author alan
 */
public class GenerationConfig {
    private String databaseIp;
    private String databasePort;
    private String databaseUser;
    private String databasePwd;
    private double sizeFactor;

    public String getDatabaseIp() {
        return databaseIp;
    }

    public void setDatabaseIp(String databaseIp) {
        this.databaseIp = databaseIp;
    }

    public String getDatabasePort() {
        return databasePort;
    }

    public void setDatabasePort(String databasePort) {
        this.databasePort = databasePort;
    }

    public String getDatabaseUser() {
        return databaseUser;
    }

    public void setDatabaseUser(String databaseUser) {
        this.databaseUser = databaseUser;
    }

    public String getDatabasePwd() {
        return databasePwd;
    }

    public void setDatabasePwd(String databasePwd) {
        this.databasePwd = databasePwd;
    }

    public double getSizeFactor() {
        return sizeFactor;
    }

    public void setSizeFactor(double sizeFactor) {
        this.sizeFactor = sizeFactor;
    }
}
