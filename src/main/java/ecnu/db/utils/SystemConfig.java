package ecnu.db.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import ecnu.db.schema.column.ColumnType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

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
    private String dataSource;
    private String databaseVersion;
    private String sqlsDirectory;
    private String loadDirectory;
    private String dumpDirectory;
    private HashMap<ColumnType, HashSet<String>> typeConvert;
    private HashMap<String, String> tidbSelectArgs;
    private Integer skipNodeThreshold;

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
        SystemConfig systemConfig = new ObjectMapper().readValue(configJson.toString(), SystemConfig.class);
        ConfigConvert.setTypeConvert(systemConfig.getTypeConvert());
        return systemConfig;
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

    public HashMap<String, String> getTidbSelectArgs() {
        return tidbSelectArgs;
    }

    public void setTidbSelectArgs(HashMap<String, String> tidbSelectArgs) {
        this.tidbSelectArgs = tidbSelectArgs;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getTidbHttpPort() {
        return tidbHttpPort;
    }

    public void setTidbHttpPort(String tidbHttpPort) {
        this.tidbHttpPort = tidbHttpPort;
    }

    public HashMap<ColumnType, HashSet<String>> getTypeConvert() {
        return typeConvert;
    }

    public void setTypeConvert(HashMap<ColumnType, HashSet<String>> typeConvert) {
        this.typeConvert = typeConvert;
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

    public String getDatabaseVersion() {
        return databaseVersion;
    }

    public void setDatabaseVersion(String databaseVersion) {
        this.databaseVersion = databaseVersion;
    }

    public Integer getSkipNodeThreshold() {
        return skipNodeThreshold;
    }

    public void setSkipNodeThreshold(Integer skipNodeThreshold) {
        this.skipNodeThreshold = skipNodeThreshold;
    }
}
