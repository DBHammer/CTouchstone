package ecnu.db.utils.config;

/**
 * @author qingshuai.wang
 */
public class PrepareConfig {
    private DatabaseConnectorConfig databaseConnectorConfig;
    private String resultDirectory;
    private String queriesDirectory;
    private int sampleSize = 10_000;
    private Double skipNodeThreshold = 0.01;

    public DatabaseConnectorConfig getDatabaseConnectorConfig() {
        return databaseConnectorConfig;
    }

    public void setDatabaseConnectorConfig(DatabaseConnectorConfig databaseConnectorConfig) {
        this.databaseConnectorConfig = databaseConnectorConfig;
    }

    public String getResultDirectory() {
        return resultDirectory;
    }

    public void setResultDirectory(String resultDirectory) {
        this.resultDirectory = resultDirectory;
    }

    public String getQueriesDirectory() {
        return queriesDirectory;
    }

    public void setQueriesDirectory(String queriesDirectory) {
        this.queriesDirectory = queriesDirectory;
    }

    public Double getSkipNodeThreshold() {
        return skipNodeThreshold;
    }

    public void setSkipNodeThreshold(Double skipNodeThreshold) {
        this.skipNodeThreshold = skipNodeThreshold;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }
}
