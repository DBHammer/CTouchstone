package ecnu.db.app;


import com.alibaba.druid.sql.SQLUtils;
import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.statical.QueryReader;
import ecnu.db.analyzer.statical.QueryWriter;
import ecnu.db.constraintchain.QueryInstantiation;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.dbconnector.DbConnector;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.Schema;
import ecnu.db.schema.SchemaManager;
import ecnu.db.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author wangqingshuai
 */
public class Extractor {

    private static final Logger logger = LoggerFactory.getLogger(Extractor.class);
    private static final Pattern pattern = Pattern.compile("'(([0-9]+),[0,1])'");

//    private static void loadSchemaMetadataAndColumnMetadata(PrepareConfig config) throws IOException, TouchstoneException, CsvException {
//        StorageManager storageManager = new StorageManager(config.getResultDirectory(), config.getDumpDirectory(), config.getLoadDirectory(), config.getLogDirectory());
//        storageManager.init();
//        List<String> tableNames;
//        // todo 静态文件读取时，需要选择合适的数据库信息
//        tableNames = storageManager.loadTableNames();
//        logger.info("加载表名成功，表名为:" + tableNames);
//        Map<String, List<String[]>> queryPlanMap = storageManager.loadQueryPlans();
//        Map<String, Integer> multiColNdvMap = storageManager.loadMultiColMap();
//        SchemaManager.getInstance().loadSchemas();
//        DumpFileConnector connector = new DumpFileConnector(queryPlanMap, multiColNdvMap);
//        logger.info("数据加载完毕");
//    }

    private static void querySchemaMetadataAndColumnMetadata(DbConnector dbConnector, List<File> queryFiles)
            throws IOException, TouchstoneException, SQLException {
        List<String> tableNames = dbConnector.fetchTableNames(queryFiles);
        logger.info("获取表名成功，表名为:" + tableNames);
        for (String canonicalTableName : tableNames) {
            logger.info("开始获取表" + canonicalTableName + "的列元数据信息");
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
        logger.info("获取表结构和数据分布成功");
        logger.info("开始持久化表结构信息");
        SchemaManager.getInstance().storeSchemaInfo();
        logger.info("持久化表结构信息成功");
        logger.info("开始持久化数据分布信息");
        ColumnManager.getInstance().storeColumnDistribution();
        logger.info("持久化数据分布信息成功");
    }

    private static List<File> loadQueries(String sqlDir) {
        return Optional.ofNullable(new File(sqlDir).listFiles())
                .map(Arrays::asList)
                .orElse(new ArrayList<>())
                .stream()
                .filter((file) -> file.isFile() && file.getName().endsWith(".sql"))
                .collect(Collectors.toList());
    }

    public static void extract(String sqlDir, DbConnector dbConnector, AbstractAnalyzer queryAnalyzer) throws Exception {
        List<File> queryFiles = loadQueries(sqlDir);
        querySchemaMetadataAndColumnMetadata(dbConnector, queryFiles);
        Map<String, String> queryName2QueryTemplates = new HashMap<>();
        Map<String, List<ConstraintChain>> query2constraintChains = new LinkedHashMap<>();
        logger.info("开始获取查询计划");
        for (File queryFile : queryFiles) {
            if (queryFile.isFile() && queryFile.getName().endsWith(SQL_FILE_POSTFIX)) {
                List<String> queries = QueryReader.getQueriesFromFile(queryFile.getPath(), queryAnalyzer.getDbType());
                int index = 0;
                List<String[]> queryPlan = new ArrayList<>();
                for (String query : queries) {
                    index++;
                    String queryCanonicalName = queryFile.getName().replace(SQL_FILE_POSTFIX, "_" + index + SQL_FILE_POSTFIX);
                    try {
                        logger.info(String.format("%-15s Status:开始获取", queryCanonicalName));
                        queryPlan = dbConnector.explainQuery(query);
                        List<ConstraintChain> constraintChains = queryAnalyzer.extractQueryInfos(queryPlan);
                        logger.info(String.format("%-15s Status:获取成功", queryCanonicalName));
                        List<Parameter> parameters = constraintChains.stream().flatMap((c -> c.getParameters().stream())).collect(Collectors.toList());
                        query2constraintChains.put(queryCanonicalName, constraintChains);
                        String queryTemplate = QueryWriter.templatizeSql(queryCanonicalName, query, queryAnalyzer.getDbType(), parameters);
                        queryName2QueryTemplates.put(queryCanonicalName, queryTemplate);
                    } catch (TouchstoneException e) {
                        logger.error(String.format("%-15s Status:获取失败", queryCanonicalName), e);
                        if (queryPlan != null && !queryPlan.isEmpty()) {
                            String queryPlanContent = queryPlan.stream().map((strs) -> String.join(";", strs))
                                    .collect(Collectors.joining(System.lineSeparator()));
                            logger.error(queryPlanContent);
                        }
                    }
                }
            }
        }
        logger.info("获取查询计划完成");
        logger.info("开始持久化查询计划");
        String allConstraintChainsContent = CommonUtils.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(query2constraintChains);
        FileUtils.writeStringToFile(new File(CommonUtils.getResultDir().getPath() + constraintChainsInfo), allConstraintChainsContent, UTF_8);
        logger.info("持久化查询计划完成");
        logger.info("开始实例化查询计划");
        List<ConstraintChain> allConstraintChains = query2constraintChains.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        QueryInstantiation.compute(allConstraintChains);
        logger.info("实例化查询计划成功, 实例化的参数为");
        Map<Integer, Parameter> id2Parameter = new HashMap<>();
        allConstraintChains.stream()
                .flatMap((l) -> l.getParameters().stream())
                .collect(Collectors.toList())
                .forEach((param) -> id2Parameter.put(param.getId(), param));
        logger.info(String.valueOf(id2Parameter.values()));
        logger.info("开始填充查询模版");
        for (Map.Entry<String, String> queryName2QueryTemplate : queryName2QueryTemplates.entrySet()) {
            String query = queryName2QueryTemplate.getValue();
            List<List<String>> matches = matchPattern(pattern, query);
            for (List<String> group : matches) {
                int parameterId = Integer.parseInt(group.get(2));
                String parameterData = id2Parameter.remove(parameterId).getData();
                try {
                    query = query.replaceAll(group.get(0), String.format("'%s'", parameterData));
                } catch (IllegalArgumentException e) {
                    logger.error("query is " + query + "; group is " + group + "; parameter data is " + parameterData, e);
                }
            }
            String formatQuery = SQLUtils.format(query, queryAnalyzer.getDbType(), SQLUtils.DEFAULT_LCASE_FORMAT_OPTION) + System.lineSeparator();
            FileUtils.writeStringToFile(new File(CommonUtils.getResultDir().getPath() + queryDir + queryName2QueryTemplate.getKey()), formatQuery, UTF_8);
        }
        logger.info("填充查询模版完成");
        if (id2Parameter.size() > 0) {
            logger.error("未被成功替换的参数如下" + id2Parameter.values());
        }

    }
}
