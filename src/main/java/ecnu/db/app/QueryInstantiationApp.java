package ecnu.db.app;

import ecnu.db.dbconnector.InputService;
import ecnu.db.tidb.Tidb4Connector;
import ecnu.db.tidb.TidbAnalyzer;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.config.PrepareConfig;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * @author alan
 */

@CommandLine.Command(name = "prepare", description = "get database information, instantiate queries, prepare for data generation",
        mixinStandardHelpOptions = true, sortOptions = false)
class QueryInstantiationApp implements Callable<Integer> {
    @CommandLine.Option(names = {"-c", "--config_path"}, description = "file path to read query instantiation configuration, " +
            "other settings in command line will override the settings in the file")
    private String configPath;
    @CommandLine.Option(names = {"-i", "--ip"}, defaultValue = "localhost", description = "database ip, default value: '${DEFAULT-VALUE}'")
    private String databaseIp;
    @CommandLine.Option(names = {"-P", "--port"}, defaultValue = "4000", description = "database port, default value: '${DEFAULT-VALUE}'")
    private String databasePort;
    @CommandLine.Option(names = {"-u", "--user"}, description = "database user name")
    private String databaseUser;
    @CommandLine.Option(names = {"-p", "--password"}, description = "database password")
    private String databasePwd;
    @CommandLine.Option(names = {"--database_name"}, description = "database name")
    private String databaseName;
    @CommandLine.Option(names = {"--cross_multibase"}, defaultValue = "false", negatable = true, description = "is cross multi-database")
    private Boolean crossMultiDatabase;
    @CommandLine.Option(names = {"-o", "--output"}, description = "output directory")
    private String resultDirectory;
    @CommandLine.Option(names = {"--db_version"}, description = "database version: ${COMPLETION-CANDIDATES}")
    private String sqlsDirectory;
    @CommandLine.Option(names = {"--load_input"}, description = "optional, if not present, then will not load")
    private String loadDirectory;
    @CommandLine.Option(names = {"--dump_input"}, description = "optional, if not present, then will not dump")
    private String dumpDirectory;
    @CommandLine.Option(names = {"--log_output"}, description = "log output directory")
    private String logDirectory;
    @CommandLine.Option(names = {"--sample_size"}, defaultValue = "10000", description = "sample size for query instantiation")
    private int sampleSize;
    @CommandLine.Option(names = {"--skip_threshold"}, description = "skip threshold, if passsing this threshold, then we will skip the node")
    private Double skipNodeThreshold;

    @Override
    public Integer call() throws Exception {
        PrepareConfig config = new PrepareConfig();
        if (configPath != null) {
            config = PrepareConfig.readConfig(configPath);
        }
        if (databaseIp != null && config.getDatabaseIp() == null) {
            config.setDatabaseIp(databaseIp);
        }
        if (databasePort != null && config.getDatabasePort() == null) {
            config.setDatabasePort(databasePort);
        }
        if (databaseUser != null) {
            config.setDatabaseUser(databaseUser);
        }
        if (databasePwd != null) {
            config.setDatabasePwd(databasePwd);
        }
        if (databaseName != null) {
            config.setDatabaseName(databaseName);
        }
        if (crossMultiDatabase != null && config.isCrossMultiDatabase() == null) {
            config.setCrossMultiDatabase(crossMultiDatabase);
        }
        if (resultDirectory != null) {
            config.setResultDirectory(resultDirectory);
        }
        if (sqlsDirectory != null) {
            config.setSqlsDirectory(sqlsDirectory);
        }
        if (loadDirectory != null) {
            config.setLoadDirectory(loadDirectory);
        }
        if (dumpDirectory != null) {
            config.setDumpDirectory(dumpDirectory);
        }
        if (logDirectory != null) {
            config.setLogDirectory(logDirectory);
        }
        if (config.getSampleSize() == 0) {
            config.setSampleSize(sampleSize);
        }
        if (skipNodeThreshold != null) {
            config.setSkipNodeThreshold(skipNodeThreshold);
        }

        if (!config.isCrossMultiDatabase()) {
            CommonUtils.DEFAULT_DATABASE = config.getDatabaseName();
        }
        InputService.getInputService().setDatabaseConnectorInterface(new Tidb4Connector(config));
        Extractor.extract(config.getSqlsDirectory(), new Tidb4Connector(config), new TidbAnalyzer());
        return 0;
    }
}
