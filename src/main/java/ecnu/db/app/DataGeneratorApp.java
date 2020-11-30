package ecnu.db.app;

import ecnu.db.utils.config.GenerationConfig;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "generate", description = "generate database according to gathered information",
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
