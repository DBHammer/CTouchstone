package ecnu.db;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.TidbAnalyzer;
import ecnu.db.analyzer.online.node.ExecutionNode;
import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.dbconnector.DatabaseConnectorInterface;
import ecnu.db.dbconnector.DumpFileConnector;
import ecnu.db.dbconnector.TidbConnector;
import ecnu.db.schema.Schema;
import ecnu.db.schema.generation.AbstractSchemaGenerator;
import ecnu.db.schema.generation.TidbSchemaGenerator;
import ecnu.db.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.INIT_HASHMAP_SIZE;

/**
 * @author wangqingshuai
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        SystemConfig config = SystemConfig.readConfig(args[0]);
        List<File> files = Optional.ofNullable(new File(config.getSqlsDirectory()).listFiles())
                .map(Arrays::asList)
                .orElse(new ArrayList<>())
                .stream()
                .filter((file) -> file.isFile() && file.getName().endsWith(".sql"))
                .collect(Collectors.toList());

        StorageManager storageManager = new StorageManager(config.getResultDirectory(), config.getDumpDirectory(), config.getLoadDirectory(), config.getLogDirectory());
        storageManager.init();

        DatabaseConnectorInterface dbConnector;
        Map<String, Schema> schemas = new HashMap<>(INIT_HASHMAP_SIZE);
        List<String> tableNames;
        if (storageManager.isLoad()) {
            tableNames = storageManager.loadTableNames();
            logger.info("加载表名成功，表名为:" + tableNames);
            Map<String, List<String[]>> queryPlanMap = storageManager.loadQueryPlans();
            Map<String, Integer> multiColNdvMap = storageManager.loadMultiColMap();
            schemas = storageManager.loadSchemas();
            dbConnector = new DumpFileConnector(tableNames, queryPlanMap, multiColNdvMap);
            logger.info("数据加载完毕");
        } else {
            dbConnector = new TidbConnector(config);
            tableNames = ((AbstractDbConnector) dbConnector).fetchTableNames(config.isCrossMultiDatabase(), config.getDatabaseName(), files);
            logger.info("获取表名成功，表名为:" + tableNames);
            AbstractSchemaGenerator dbSchemaGenerator = new TidbSchemaGenerator();
            for (String canonicalTableName : tableNames) {
                Schema schema = ((AbstractDbConnector) dbConnector).fetchSchema(dbSchemaGenerator, canonicalTableName);
                schemas.put(canonicalTableName, schema);
            }
            Schema.initFks(((AbstractDbConnector) dbConnector).databaseMetaData, schemas);
            logger.info("获取表结构和数据分布成功");
        }
        AbstractAnalyzer queryAnalyzer = getAnalyzer(config, dbConnector, schemas);
        List<String> queryInfos = new LinkedList<>();
        boolean needLog = false;
        logger.info("开始获取查询计划");
        for (File sqlFile : files) {
            if (sqlFile.isFile() && sqlFile.getName().endsWith(".sql")) {
                List<String> queries = ReadQuery.getQueriesFromFile(sqlFile.getPath(), queryAnalyzer.getDbType());
                int index = 0;
                List<String[]> queryPlan = new ArrayList<>();
                for (String query : queries) {
                    index++;
                    String queryCanonicalName = String.format("%s_%d", sqlFile.getName(), index);
                    List<String> cannotFindArgs = new ArrayList<>();
                    List<String> conflictArgs = new ArrayList<>();
                    try {
                        logger.info(String.format("%-15s Status:开始获取", queryCanonicalName));
                        queryInfos.add("## " + queryCanonicalName);
                        queryPlan = queryAnalyzer.getQueryPlan(queryCanonicalName, query);
                        if (storageManager.isDump()) {
                            storageManager.dumpQueryPlan(queryPlan, queryCanonicalName);
                        }
                        ExecutionNode root = queryAnalyzer.getExecutionTree(queryPlan);
                        queryInfos.addAll(queryAnalyzer.extractQueryInfos(queryCanonicalName, root));
                        logger.info(String.format("%-15s Status:获取成功", queryCanonicalName));
                        queryAnalyzer.outputSuccess(true);
                        query = SqlTemplateHelper.templatizeSql(query, queryAnalyzer, cannotFindArgs, conflictArgs);
                        if (cannotFindArgs.size() > 0) {
                            logger.warn(String.format("请注意%s中有参数无法完成替换，请查看该sql输出，手动替换;", queryCanonicalName));
                            query = SqlTemplateHelper.appendArgs(queryAnalyzer.getArgsAndIndex(), "cannotFindArgs", cannotFindArgs) + query;
                        }
                        if (conflictArgs.size() > 0) {
                            logger.warn(String.format("请注意%s中有参数出现多次，无法智能，替换请查看该sql输出，手动替换;", queryCanonicalName));
                            query = SqlTemplateHelper.appendArgs(queryAnalyzer.getArgsAndIndex(), "conflictArgs", conflictArgs) + query;
                        }
                        storageManager.storeSqlResult(sqlFile, query, queryAnalyzer.getDbType());
                    } catch (TouchstoneToolChainException e) {
                        queryAnalyzer.outputSuccess(false);
                        logger.error(String.format("%-15s Status:获取失败", queryCanonicalName), e);
                        needLog = true;
                        if (queryPlan != null && !queryPlan.isEmpty()) {
                            storageManager.logQueryPlan(queryPlan, queryCanonicalName);
                            logger.info(String.format("失败的query %s的查询计划已经存盘到'%s'", queryCanonicalName, storageManager.getLogDir().getAbsolutePath()));
                        }
                    }
                }
            }
        }
        logger.info("获取查询计划完成");
        if (storageManager.isDump()) {
            storageManager.dumpStaticInfo(dbConnector.getMultiColNdvMap(), schemas, tableNames);
            logger.info("表结构和数据分布持久化成功");
        }
        if (needLog) {
            storageManager.logStaticInfo(dbConnector.getMultiColNdvMap(), schemas, tableNames);
            logger.info(String.format("关于表的日志信息已经存盘到'%s'", storageManager.getLogDir().getAbsolutePath()));
        }
        storageManager.storeSchemaResult(schemas);
        storageManager.storeConstrainChainResult(queryInfos);
    }

    private static AbstractAnalyzer getAnalyzer(SystemConfig config, DatabaseConnectorInterface dbConnector, Map<String, Schema> schemas) {
        Multimap<String, String> tblName2CanonicalTblName = ArrayListMultimap.create();
        for (String canonicalTableName : schemas.keySet()) {
            tblName2CanonicalTblName.put(canonicalTableName.split("\\.")[1], canonicalTableName);
        }
        return new TidbAnalyzer(config, dbConnector, schemas, tblName2CanonicalTblName);
    }


}
