package ecnu.db.utils.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author alan
 */
public class GenerationConfig implements DatabaseConnectorConfig {
    private String databaseIp;
    private String databasePort;
    private String databaseName;
    private String databaseUser;
    private String databasePwd;
    private String inputPath;
    private String outputPath;
    private String joinInfoPath;
    private int epochSize;
    private int threadNum;
    private Boolean isCrossMultiDatabase;
    private int threadPoolSize;

    public static GenerationConfig readConfig(String filePath) throws IOException {
        String configJson = FileUtils.readFileToString(new File(filePath), UTF_8);
        return new ObjectMapper().readValue(configJson, GenerationConfig.class);
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

    public Boolean isCrossMultiDatabase() {
        return isCrossMultiDatabase;
    }

    public void setCrossMultiDatabase(Boolean crossMultiDatabase) {
        isCrossMultiDatabase = crossMultiDatabase;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public int getEpochSize() {
        return epochSize;
    }

    public void setEpochSize(int epochSize) {
        this.epochSize = epochSize;
    }

    public String getJoinInfoPath() {
        return joinInfoPath;
    }

    public void setJoinInfoPath(String joinInfoPath) {
        this.joinInfoPath = joinInfoPath;
    }


    public int getThreadNum() {
        return threadNum;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }
}
