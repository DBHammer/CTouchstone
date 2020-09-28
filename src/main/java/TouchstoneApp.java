import ecnu.db.Extractor;
import ecnu.db.Generator;
import ecnu.db.utils.GenerationConfig;
import ecnu.db.utils.PrepareConfig;
import ecnu.db.utils.TouchstoneSupportedDatabaseVersion;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

/**
 * @author alan
 */

@Command(name = "prepare", description = "get database information, instantiate queries, prepare for data generation",
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
    @CommandLine.Option(names = {"--cross_multibase"}, negatable = true, description = "is cross multi-database")
    private Boolean crossMultiDatabase;
    @CommandLine.Option(names = {"-o", "--output"}, description = "output directory")
    private String resultDirectory;
    @CommandLine.Option(names = {"--db_version"}, description = "database version: ${COMPLETION-CANDIDATES}")
    private TouchstoneSupportedDatabaseVersion databaseVersion;
    @CommandLine.Option(names = {"--sql_input"}, description = "sql input directory")
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
        if (databaseIp != null) {
            config.setDatabaseIp(databaseIp);
        }
        if (databasePort != null) {
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
        if (crossMultiDatabase != null) {
            config.setCrossMultiDatabase(crossMultiDatabase);
        }
        if (resultDirectory != null) {
            config.setResultDirectory(resultDirectory);
        }
        if (databaseVersion != null) {
            config.setDatabaseVersion(databaseVersion);
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
        Extractor.extract(config);
        return 0;
    }
}

@Command(name = "generate", description = "generate database according to gathered information",
        mixinStandardHelpOptions = true, sortOptions = false)
class DataGeneratorApp implements Callable<Integer> {
    @CommandLine.Option(names = {"-c", "--config_path"}, description = "file path to read data generation configuration, " +
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
    @CommandLine.Option(names = {"--cross_multibase"}, negatable = true, description = "is cross multi-database")
    private Boolean crossMultiDatabase;
    @CommandLine.Option(names = {"-I", "--intput_path"}, description = "input path for data generation")
    private String inputPath;
    @CommandLine.Option(names = {"-o", "--output_path"}, description = "output path for instantiated queries")
    private String outputPath;
    @CommandLine.Option(names = {"-J", "--join_info_path"}, description = "temporary storage path for join information")
    private String joinInfoPath;
    @CommandLine.Option(names = {"--db_version"}, description = "database version: ${COMPLETION-CANDIDATES}")
    private TouchstoneSupportedDatabaseVersion databaseVersion;
    @CommandLine.Option(names = {"--epoch_size"}, defaultValue = "1000", description = "generating tuple size for each iteration per thread, default value : '${DEFAULT-VALUE}'")
    private int epochSize;
    @CommandLine.Option(names = {"--thread_num"}, defaultValue = "16", description = "maximum number of threads per schema, default value: '${DEFAULT-VALUE}'")
    private int threadNum;
    @CommandLine.Option(names = {"--thread_pool_size"}, defaultValue = "256", description = "number of threads in thread pool, default value: '${DEFAULT-VALUE}'")
    private int threadPoolSize;

    @Override
    public Integer call() throws Exception {
        GenerationConfig config = new GenerationConfig();
        if (configPath != null) {
            config = GenerationConfig.readConfig(configPath);
        }
        if (databaseIp != null) {
            config.setDatabaseIp(databaseIp);
        }
        if (databasePort != null) {
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
        if (crossMultiDatabase != null) {
            config.setCrossMultiDatabase(crossMultiDatabase);
        }
        if (inputPath != null) {
            config.setInputPath(inputPath);
        }
        if (outputPath != null) {
            config.setOutputPath(outputPath);
        }
        if (joinInfoPath != null) {
            config.setJoinInfoPath(joinInfoPath);
        }
        if (databaseVersion != null) {
            config.setDatabaseVersion(databaseVersion);
        }
        if (config.getEpochSize() == 0) {
            config.setEpochSize(epochSize);
        }
        if (config.getThreadNum() == 0) {
            config.setThreadNum(threadNum);
        }
        if (config.getThreadPoolSize() == 0) {
            config.setThreadPoolSize(threadPoolSize);
        }
        Generator.generate(config);
        return 0;
    }
}

@Command(name = "touchstone",
        version = {"touchstone 0.1.0",
                "JVM: ${java.version} (${java.vendor} ${java.vm.name} ${java.vm.version})",
                "OS: ${os.name} ${os.version} ${os.arch}"},
        description = "tool for generating test database", sortOptions = false,
        subcommands = {QueryInstantiationApp.class, DataGeneratorApp.class},
        mixinStandardHelpOptions = true,
        commandListHeading = "Commands:\n",
        header = {
                "@|green  _____                _         _ |@",
                "@|green |_   _|__  _   _  ___| |__  ___| |_ ___  _ __   ___ |@",
                "@|green   | |/ _ \\| | | |/ __| '_ \\/ __| __/ _ \\| '_ \\ / _ \\ |@",
                "@|green   | | (_) | |_| | (__| | | \\__ \\ || (_) | | | |  __/ |@",
                "@|green   |_|\\___/ \\__,_|\\___|_| |_|___/\\__\\___/|_| |_|\\___| |@",
                ""}
        )
public class TouchstoneApp implements Runnable {
    @Override
    public void run() {}

    public static void main(String... args) {
        int exitCode = new CommandLine(new TouchstoneApp()).execute(args);
        System.exit(exitCode);
    }
}
