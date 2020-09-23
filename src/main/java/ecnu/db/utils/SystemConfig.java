package ecnu.db.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import ecnu.db.schema.column.ColumnType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author qingshuai.wang
 */
public class SystemConfig {

    private String databaseIp;
    private String databasePort;
    private String databaseUser;
    private String databasePwd;
    private String databaseName;
    private boolean crossMultiDatabase;
    private String resultDirectory;
    private String tidbHttpPort;
    private TouchstoneSupportedDatabaseVersion databaseVersion;
    private String sqlsDirectory;
    private String loadDirectory;
    private String dumpDirectory;
    private String logDirectory;
    private int sampleSize;

    private Double skipNodeThreshold;

    public SystemConfig() {
        databaseIp = "127.0.0.1";
        databaseUser = "root";
        databasePwd = "";
        databasePort = "4000";
        databaseName = "tpch";
    }

    public static SystemConfig readConfig(String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        StringBuilder configJson = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            configJson.append(line);
        }
        return new ObjectMapper().readValue(configJson.toString(), SystemConfig.class);
    }

    public boolean isCrossMultiDatabase() {
        return crossMultiDatabase;
    }

    public void setCrossMultiDatabase(boolean crossMultiDatabase) {
        this.crossMultiDatabase = crossMultiDatabase;
    }

    public String getResultDirectory() {
        return resultDirectory;
    }

    public void setResultDirectory(String resultDirectory) {
        this.resultDirectory = resultDirectory;
    }

    public String getSqlsDirectory() {
        return sqlsDirectory;
    }

    public void setSqlsDirectory(String sqlsDirectory) {
        this.sqlsDirectory = sqlsDirectory;
    }

    public String getLoadDirectory() {
        return loadDirectory;
    }

    public void setLoadDirectory(String loadDirectory) {
        this.loadDirectory = loadDirectory;
    }

    public String getDumpDirectory() {
        return dumpDirectory;
    }

    public void setDumpDirectory(String dumpDirectory) {
        this.dumpDirectory = dumpDirectory;
    }

    public String getTidbHttpPort() {
        return tidbHttpPort;
    }

    public void setTidbHttpPort(String tidbHttpPort) {
        this.tidbHttpPort = tidbHttpPort;
    }

    public ColumnType getColumnType(String readType) {
        return ColumnType.INTEGER;
    }

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

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public TouchstoneSupportedDatabaseVersion getDatabaseVersion() {
        return databaseVersion;
    }

    public void setDatabaseVersion(TouchstoneSupportedDatabaseVersion databaseVersion) {
        this.databaseVersion = databaseVersion;
    }

    public Double getSkipNodeThreshold() {
        return skipNodeThreshold;
    }

    public void setSkipNodeThreshold(Double skipNodeThreshold) {
        this.skipNodeThreshold = skipNodeThreshold;
    }

    public String getLogDirectory() {
        return logDirectory;
    }

    public void setLogDirectory(String logDirectory) {
        this.logDirectory = logDirectory;
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = sampleSize;
    }
}
