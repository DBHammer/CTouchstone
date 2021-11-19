package ecnu.db.analyzer;

import com.alibaba.druid.DbType;
import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.QueryAnalyzer;
import ecnu.db.analyzer.online.adapter.pg.PgAnalyzer;
import ecnu.db.analyzer.online.adapter.tidb.TidbAnalyzer;
import ecnu.db.analyzer.statical.QueryReader;
import ecnu.db.analyzer.statical.QueryWriter;
import ecnu.db.dbconnector.DbConnector;
import ecnu.db.dbconnector.adapter.PgConnector;
import ecnu.db.dbconnector.adapter.Tidb3Connector;
import ecnu.db.dbconnector.adapter.Tidb4Connector;
import ecnu.db.generator.constraintchain.chain.ConstraintChain;
import ecnu.db.generator.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.generator.constraintchain.chain.ConstraintChainManager;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.MultiVarFilterOperation;
import ecnu.db.generator.constraintchain.filter.operation.UniVarFilterOperation;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.Table;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.DatabaseConnectorConfig;
import ecnu.db.utils.exception.TouchstoneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.MAPPER;

/**
 * @author alan
 */

@CommandLine.Command(name = "prepare", description = "get database information, instantiate queries, prepare for data generation",
        mixinStandardHelpOptions = true)
public class TaskConfigurator implements Callable<Integer> {
    private final Logger logger = LoggerFactory.getLogger(TaskConfigurator.class);
    public static final String SQL_FILE_POSTFIX = ".sql";
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
                        .filter(ConstraintChainFilterNode.class::isInstance)
                        .map(ConstraintChainFilterNode.class::cast)
                        .map(ConstraintChainFilterNode::pushDownProbability)
                        .flatMap(Collection::stream).toList())
                .flatMap(Collection::stream).toList();

        // uni-var operation
        filterOperations.stream()
                .filter(UniVarFilterOperation.class::isInstance)
                .map(UniVarFilterOperation.class::cast)
                .forEach(UniVarFilterOperation::instantiateParameter);

        // init eq params
        ColumnManager.getInstance().initAllEqParameter();

        // multi-var non-eq sampling
        Set<String> prepareSamplingColumnName = filterOperations.parallelStream()
                .filter(MultiVarFilterOperation.class::isInstance)
                .map(MultiVarFilterOperation.class::cast)
                .map(MultiVarFilterOperation::getAllCanonicalColumnNames)
                .flatMap(Collection::stream).collect(Collectors.toSet());

        ColumnManager.getInstance().prepareGeneration(prepareSamplingColumnName, samplingSize);

        filterOperations.parallelStream()
                .filter(MultiVarFilterOperation.class::isInstance)
                .map(MultiVarFilterOperation.class::cast)
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
            config = MAPPER.readValue(CommonUtils.readFile(taskConfiguratorConfig.othersConfig.fileConfigInfo.configPath),
                    ecnu.db.utils.TaskConfiguratorConfig.class);
        } else {
            CliConfigInfo cliConfigInfo = taskConfiguratorConfig.othersConfig.cliConfigInfo;
            config = new ecnu.db.utils.TaskConfiguratorConfig();
            config.setDatabaseConnectorConfig(new DatabaseConnectorConfig(cliConfigInfo.databaseIp, cliConfigInfo.databasePort,
                    cliConfigInfo.databaseUser, cliConfigInfo.databasePwd, cliConfigInfo.databaseName));
        }
        QueryReader queryReader = new QueryReader(config.getDefaultSchemaName(), config.getQueriesDirectory());
        QueryWriter queryWriter = new QueryWriter(config.getResultDirectory());
        TableManager.getInstance().setResultDir(config.getResultDirectory());
        ColumnManager.getInstance().setResultDir(config.getResultDirectory());
        ConstraintChainManager.getInstance().setResultDir(config.getResultDirectory());
        if (taskConfiguratorConfig.isLoad) {
            TableManager.getInstance().loadSchemaInfo();
            ColumnManager.getInstance().loadColumnDistribution();
        }
        DbConnector dbConnector;
        AbstractAnalyzer abstractAnalyzer;
        switch (taskConfiguratorConfig.dbType) {
            case TIDB3 -> {
                dbConnector = new Tidb3Connector(config.getDatabaseConnectorConfig());
                abstractAnalyzer = new TidbAnalyzer();
                queryWriter.setDbType(DbType.mysql);
                queryReader.setDbType(DbType.mysql);
            }
            case TIDB4 -> {
                dbConnector = new Tidb4Connector(config.getDatabaseConnectorConfig());
                abstractAnalyzer = new TidbAnalyzer();
                queryWriter.setDbType(DbType.mysql);
                queryReader.setDbType(DbType.mysql);
            }
            case POSTGRESQL -> {
                dbConnector = new PgConnector(config.getDatabaseConnectorConfig());
                abstractAnalyzer = new PgAnalyzer();
                queryWriter.setDbType(DbType.mysql);
                queryReader.setDbType(DbType.mysql);
            }
            default -> throw new TouchstoneException("不支持的数据库类型");
        }
        QueryAnalyzer analyzer = new QueryAnalyzer(abstractAnalyzer, dbConnector);
        extract(dbConnector, analyzer, queryReader, queryWriter, config.getSampleSize());
        return 0;
    }

    private List<File> querySchemaMetadataAndColumnMetadata(QueryReader queryReader, DbConnector dbConnector)
            throws IOException, TouchstoneException, SQLException {
        List<File> queryFiles = queryReader.loadQueryFiles();
        List<String> tableNames = queryReader.fetchTableNames(queryFiles);
        logger.info("获取表名成功，表名为:{}", tableNames);
        for (String canonicalTableName : tableNames) {
            logger.info("开始获取表{}的列元数据信息", canonicalTableName);
            if (TableManager.getInstance().containSchema(canonicalTableName)) {
                logger.info("表{}的列元数据信息已经load", canonicalTableName);
            } else {
                Table table = new Table(dbConnector.getColumnMetadata(canonicalTableName),
                        dbConnector.getTableSize(canonicalTableName), dbConnector.getPrimaryKeyList(canonicalTableName));
                TableManager.getInstance().addSchema(canonicalTableName, table);
                logger.info("获取表{}的列元数据信息成功", canonicalTableName);
                logger.info("开始获取表{}的数据分布", canonicalTableName);
                ColumnManager.getInstance().setDataRangeBySqlResult(table.getCanonicalColumnNames(),
                        dbConnector.getDataRange(canonicalTableName, table.getCanonicalColumnNames()));
                logger.info("获取表{}的数据分布成功", canonicalTableName);
            }

        }
        logger.info("获取表结构和数据分布成功");
        logger.info("开始持久化表结构信息");
        TableManager.getInstance().storeSchemaInfo();
        logger.info("持久化表结构信息成功");
        logger.info("开始持久化数据分布信息");
        ColumnManager.getInstance().storeColumnDistribution();
        logger.info("持久化数据分布信息成功");
        return queryFiles;
    }

    public void extract(DbConnector dbConnector, QueryAnalyzer queryAnalyzer, QueryReader queryReader,
                        QueryWriter queryWriter, int samplingSize) throws IOException, TouchstoneException, SQLException {
        List<File> queryFiles = querySchemaMetadataAndColumnMetadata(queryReader, dbConnector);
        Map<String, List<ConstraintChain>> query2constraintChains = new HashMap<>();
        Map<String, String> queryName2QueryTemplates = new HashMap<>();
        logger.info("开始获取查询计划");
        queryFiles = queryFiles.stream().filter(File::isFile)
                .filter(queryFile -> queryFile.getName().endsWith(SQL_FILE_POSTFIX)).toList();
        queryFiles = new LinkedList<>(queryFiles);
        queryFiles.sort(Comparator.comparing(File::getName));
        for (File queryFile : queryFiles) {
            List<String> queries = queryReader.getQueriesFromFile(queryFile.getPath());
            for (int i = 0; i < queries.size(); i++) {
                String query = queries.get(i);
                String queryCanonicalName = queryFile.getName().replace(SQL_FILE_POSTFIX, "_" + (i + 1) + SQL_FILE_POSTFIX);
                logger.info("开始获取{}", queryCanonicalName);
                queryAnalyzer.setAliasDic(queryReader.getTableAlias(query));
                List<Parameter> parameters = new ArrayList<>();
                List<List<ConstraintChain>> constraintChainsOfMultiplePlans = queryAnalyzer.extractQuery(query);
                int subPlanIndex = 0;
                for (List<ConstraintChain> constraintChains : constraintChainsOfMultiplePlans) {
                    if (subPlanIndex++ > 0) {
                        query2constraintChains.put(queryCanonicalName + "_" + subPlanIndex, constraintChains);
                    } else {
                        query2constraintChains.put(queryCanonicalName, constraintChains);
                    }
                    parameters.addAll(constraintChains.stream().flatMap((c -> c.getParameters().stream())).toList());
                }
                queryName2QueryTemplates.put(queryCanonicalName, queryWriter.templatizeSql(queryCanonicalName, query, parameters));
            }
        }
        logger.info("获取查询计划完成");
        logger.info("开始实例化查询计划");
        List<ConstraintChain> allConstraintChains = query2constraintChains.values().stream().flatMap(Collection::stream).toList();
        Map<Integer, Parameter> id2Parameter = queryInstantiation(allConstraintChains, samplingSize);
        logger.info("实例化查询计划成功, 实例化的参数为{}", id2Parameter.values());
        logger.info("开始持久化查询计划");
        ConstraintChainManager.getInstance().storeConstraintChain(query2constraintChains);
        logger.info("持久化查询计划完成");
        logger.info("开始填充查询模版");
        queryWriter.writeQuery(queryName2QueryTemplates, id2Parameter);
        logger.info("填充查询模版完成");
        if (id2Parameter.size() > 0) {
            logger.error("未被成功替换的参数如下{}", id2Parameter.values());
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
