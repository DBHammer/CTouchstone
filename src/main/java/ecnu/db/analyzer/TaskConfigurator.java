package ecnu.db.analyzer;

import com.alibaba.druid.DbType;
import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.adapter.tidb.Tidb3Connector;
import ecnu.db.analyzer.online.adapter.tidb.Tidb4Connector;
import ecnu.db.analyzer.online.adapter.tidb.TidbAnalyzer;
import ecnu.db.analyzer.statical.QueryReader;
import ecnu.db.analyzer.statical.QueryWriter;
import ecnu.db.dbconnector.DbConnector;
import ecnu.db.generator.constraintchain.chain.ConstraintChain;
import ecnu.db.generator.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.generator.constraintchain.chain.ConstraintChainManager;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.MultiVarFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.Schema;
import ecnu.db.schema.SchemaManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.DatabaseConnectorConfig;
import ecnu.db.utils.exception.TouchstoneException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author alan
 */

@CommandLine.Command(name = "prepare", description = "get database information, instantiate queries, prepare for data generation",
        mixinStandardHelpOptions = true)
public class TaskConfigurator implements Callable<Integer> {
    private static final Pattern PATTERN = Pattern.compile("'([0-9]+)'");
    private final Logger logger = LoggerFactory.getLogger(TaskConfigurator.class);
    private String resultDir;
    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
    private TaskConfiguratorConfig taskConfiguratorConfig;

    /**
     * 1. 对于数值型的filter, 首先计算单元的filter, 然后计算多值的filter，对于bet操作，先记录阈值，然后选择合适的区间插入，
     * 等值约束也需选择合适的区间每个filter operation内部保存自己实例化后的结果
     * 2. 对于字符型的filter, 只有like和eq的运算，直接计算即可
     *
     * @param constraintChains 待计算的约束链
     * @param samplingSize     多元非等值的采样大小
     */
    public static Map<Integer, Parameter> queryInstantiation(List<ConstraintChain> constraintChains, int samplingSize) {
        List<AbstractFilterOperation> filterOperations = constraintChains.stream()
                .map(constraintChain -> constraintChain.getNodes().stream()
                        .filter(node -> node instanceof ConstraintChainFilterNode)
                        .map(node -> ((ConstraintChainFilterNode) node).pushDownProbability())
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList()))
                .flatMap(Collection::stream).collect(Collectors.toList());

        // uni-var operation
        filterOperations.stream().filter((f) -> f instanceof UniVarFilterOperation)
                .map(f -> (UniVarFilterOperation) f).forEach(UniVarFilterOperation::instantiateParameter);

        // init eq params
        ColumnManager.getInstance().initAllEqParameter();

        // multi-var non-eq sampling
        Set<String> prepareSamplingColumnName = filterOperations.parallelStream()
                .filter((f) -> f instanceof MultiVarFilterOperation)
                .map(f -> ((MultiVarFilterOperation) f).getAllCanonicalColumnNames())
                .flatMap(Collection::stream).collect(Collectors.toSet());
        ColumnManager.getInstance().prepareGeneration(prepareSamplingColumnName, samplingSize);
        filterOperations.parallelStream()
                .filter((f) -> f instanceof MultiVarFilterOperation)
                .map(f -> (MultiVarFilterOperation) f)
                .forEach(MultiVarFilterOperation::instantiateMultiVarParameter);
        Map<Integer, Parameter> id2Parameter = new HashMap<>();
        filterOperations.stream().map(AbstractFilterOperation::getParameters).flatMap(Collection::stream)
                .forEach(parameter -> id2Parameter.put(parameter.getId(), parameter));
        return id2Parameter;
    }

    @Override
    public Integer call() throws Exception {
        ecnu.db.utils.TaskConfiguratorConfig config;
        if (taskConfiguratorConfig.othersConfig.fileConfigInfo != null) {
            config = MAPPER.readValue(FileUtils.readFileToString(
                    new File(taskConfiguratorConfig.othersConfig.fileConfigInfo.configPath), UTF_8), ecnu.db.utils.TaskConfiguratorConfig.class);
        } else {
            CliConfigInfo cliConfigInfo = taskConfiguratorConfig.othersConfig.cliConfigInfo;
            config = new ecnu.db.utils.TaskConfiguratorConfig();
            config.setDatabaseConnectorConfig(new DatabaseConnectorConfig(cliConfigInfo.databaseIp, cliConfigInfo.databasePort,
                    cliConfigInfo.databaseUser, cliConfigInfo.databasePwd, cliConfigInfo.databaseName));
        }
        QueryReader queryReader = new QueryReader(config.getDatabaseConnectorConfig().getDatabaseName(), config.getQueriesDirectory());
        QueryWriter queryWriter = new QueryWriter(config.getResultDirectory() + QUERY_DIR);
        SchemaManager.getInstance().setResultDir(config.getResultDirectory());
        ColumnManager.getInstance().setResultDir(config.getResultDirectory());
        resultDir = config.getResultDirectory();
        if (taskConfiguratorConfig.isLoad) {
            SchemaManager.getInstance().loadSchemaInfo();
            ColumnManager.getInstance().loadColumnDistribution();
        }
        DbConnector dbConnector;
        AbstractAnalyzer analyzer;
        switch (taskConfiguratorConfig.dbType) {
            case TiDB3:
                dbConnector = new Tidb3Connector(config.getDatabaseConnectorConfig());
                queryWriter.setDbType(DbType.mysql);
                queryReader.setDbType(DbType.mysql);
                analyzer = new TidbAnalyzer();
                break;
            case TiDB4:
                dbConnector = new Tidb4Connector(config.getDatabaseConnectorConfig());
                queryWriter.setDbType(DbType.mysql);
                queryReader.setDbType(DbType.mysql);
                analyzer = new TidbAnalyzer();
                break;
            default:
                throw new TouchstoneException("不支持的数据库类型");
        }
        analyzer.setDefaultDatabase(config.getDatabaseConnectorConfig().getDatabaseName());
        extract(dbConnector, analyzer, queryReader, queryWriter, config.getSampleSize());
        return 0;
    }

    private List<File> querySchemaMetadataAndColumnMetadata(QueryReader queryReader, DbConnector dbConnector)
            throws IOException, TouchstoneException, SQLException {
        List<File> queryFiles = queryReader.loadQueryFiles();
        List<String> tableNames = queryReader.fetchTableNames(queryFiles);
        logger.info("获取表名成功，表名为:" + tableNames);
        for (String canonicalTableName : tableNames) {
            logger.info("开始获取表" + canonicalTableName + "的列元数据信息");
            if (SchemaManager.getInstance().containSchema(canonicalTableName)) {
                logger.info("表" + canonicalTableName + "的列元数据信息已经load");
            } else {
                Schema schema = new Schema(canonicalTableName, dbConnector.getColumnMetadata(canonicalTableName));
                schema.setTableSize(dbConnector.getTableSize(canonicalTableName));
                schema.setPrimaryKeys(dbConnector.getPrimaryKeys(canonicalTableName));
                SchemaManager.getInstance().addSchema(canonicalTableName, schema);
                logger.info("获取表" + canonicalTableName + "的列元数据信息成功");
                logger.info("开始获取表" + canonicalTableName + "的数据分布");
                ColumnManager.getInstance().setDataRangeBySqlResult(schema.getCanonicalColumnNames(),
                        dbConnector.getDataRange(canonicalTableName, schema.getCanonicalColumnNames()));
                logger.info("获取表" + canonicalTableName + "的数据分布成功");
            }

        }
        logger.info("获取表结构和数据分布成功");
        return queryFiles;
    }

    public void extract(DbConnector dbConnector, AbstractAnalyzer queryAnalyzer, QueryReader queryReader,
                        QueryWriter queryWriter, int samplingSize) throws IOException, TouchstoneException, SQLException {
        List<File> queryFiles = querySchemaMetadataAndColumnMetadata(queryReader, dbConnector);
        Map<String, String> queryName2QueryTemplates = new HashMap<>();
        Map<String, List<ConstraintChain>> query2constraintChains = new LinkedHashMap<>();
        logger.info("开始获取查询计划");
        queryAnalyzer.setDbConnector(dbConnector);
        for (File queryFile : queryFiles) {
            if (queryFile.isFile() && queryFile.getName().endsWith(SQL_FILE_POSTFIX)) {
                List<String> queries = queryReader.getQueriesFromFile(queryFile.getPath());
                int index = 0;
                for (String query : queries) {
                    index++;
                    String queryCanonicalName = queryFile.getName().replace(SQL_FILE_POSTFIX, "_" + index + SQL_FILE_POSTFIX);
                    logger.info(String.format("%-15s Status:开始获取", queryCanonicalName));
                    queryAnalyzer.setAliasDic(queryReader.getTableAlias(query));
                    List<ConstraintChain> constraintChains = queryAnalyzer.extractQuery(query);
                    List<Parameter> parameters = constraintChains.stream().flatMap((c -> c.getParameters().stream())).collect(Collectors.toList());
                    query2constraintChains.put(queryCanonicalName, constraintChains);
                    String queryTemplate = queryWriter.templatizeSql(queryCanonicalName, query, parameters);
                    queryName2QueryTemplates.put(queryCanonicalName, queryTemplate);
                }
            }
        }
        logger.info("获取查询计划完成");
        logger.info("开始实例化查询计划");
        List<ConstraintChain> allConstraintChains = query2constraintChains.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        Map<Integer, Parameter> id2Parameter = queryInstantiation(allConstraintChains, samplingSize);
        logger.info("实例化查询计划成功, 实例化的参数为");
        logger.info(String.valueOf(id2Parameter.values()));
        logger.info("开始持久化表结构信息");
        SchemaManager.getInstance().storeSchemaInfo();
        logger.info("持久化表结构信息成功");
        logger.info("开始持久化数据分布信息");
        ColumnManager.getInstance().storeColumnDistribution();
        logger.info("持久化数据分布信息成功");
        logger.info("开始持久化查询计划");
        String allConstraintChainsContent = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(query2constraintChains);
        FileUtils.writeStringToFile(new File(resultDir + CONSTRAINT_CHAINS_INFO), allConstraintChainsContent, UTF_8);
        for (Map.Entry<String, List<ConstraintChain>> stringListEntry : query2constraintChains.entrySet()) {
            FileUtils.writeStringToFile(new File(resultDir + "/pic/" + stringListEntry.getKey() + ".gv"),
                    ConstraintChainManager.presentConstraintChains(stringListEntry.getKey(), stringListEntry.getValue()), UTF_8);
        }
        logger.info("持久化查询计划完成");
        logger.info("开始填充查询模版");
        for (Map.Entry<String, String> queryName2QueryTemplate : queryName2QueryTemplates.entrySet()) {
            String query = queryName2QueryTemplate.getValue();
            List<List<String>> matches = matchPattern(PATTERN, query);
            for (List<String> group : matches) {
                int parameterId = Integer.parseInt(group.get(1));
                Parameter parameter = id2Parameter.remove(parameterId);
                if (parameter != null) {
                    String parameterData = parameter.getDataValue();
                    try {
                        query = query.replaceAll(group.get(0), String.format("'%s'", parameterData));
                    } catch (IllegalArgumentException e) {
                        logger.error("query is " + query + "; group is " + group + "; parameter data is " + parameterData, e);
                    }
                }
                queryWriter.writeQuery(queryName2QueryTemplate.getKey(), query);
            }
        }
        logger.info("填充查询模版完成");
        if (id2Parameter.size() > 0) {
            logger.error("未被成功替换的参数如下" + id2Parameter.values());
        }
    }

    static class TaskConfiguratorConfig {
        @CommandLine.ArgGroup
        private OthersConfig othersConfig;
        @CommandLine.Option(names = {"-t", "--db_type"}, required = true, description = "database version: ${COMPLETION-CANDIDATES}")
        private TouchstoneDbType dbType;
        @CommandLine.Option(names = {"-l", "--load"})
        private boolean isLoad;
    }

    static class OthersConfig {
        @CommandLine.ArgGroup(exclusive = false, heading = "Input configuration by file%n")
        FileConfigInfo fileConfigInfo;
        @CommandLine.ArgGroup(exclusive = false, heading = "Input configuration by cli%n")
        CliConfigInfo cliConfigInfo;
    }

    static class FileConfigInfo {
        @CommandLine.Option(names = {"-c", "--config_path"}, required = true, description = "file path to read query instantiation configuration, " +
                "other settings in command line will override the settings in the file")
        private String configPath;

    }

    static class CliConfigInfo {
        @CommandLine.Option(names = {"-H", "--host"}, required = true, defaultValue = "localhost", description = "database ip, default value: '${DEFAULT-VALUE}'")
        private String databaseIp;
        @CommandLine.Option(names = {"-P", "--port"}, required = true, defaultValue = "4000", description = "database port, default value: '${DEFAULT-VALUE}'")
        private String databasePort;
        @CommandLine.Option(names = {"-u", "--user"}, required = true, description = "database user name")
        private String databaseUser;
        @CommandLine.Option(names = {"-p", "--password"}, required = true, description = "database password", interactive = true)
        private String databasePwd;
        @CommandLine.Option(names = {"-D", "--database_name"}, description = "database name")
        private String databaseName;
        @CommandLine.Option(names = {"-i", "--load_input"}, required = true, description = "the dir path of queries")
        private String queriesDirectory;
        @CommandLine.Option(names = {"-o", "--output"}, required = true, description = "the dir path for output")
        private String resultDirectory;
        @CommandLine.Option(names = {"--sample_size"}, defaultValue = "10000", description = "sample size for query instantiation")
        private int sampleSize;
        @CommandLine.Option(names = {"--skip_threshold"}, description = "skip threshold, if passsing this threshold, then we will skip the node")
        private Double skipNodeThreshold;
    }
}
