package ecnu.db;


import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.TidbAnalyzer;
import ecnu.db.analyzer.online.node.ExecutionNode;
import ecnu.db.analyzer.statical.QueryTableName;
import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.dbconnector.DatabaseConnectorInterface;
import ecnu.db.dbconnector.DumpFileConnector;
import ecnu.db.dbconnector.TidbConnector;
import ecnu.db.schema.Schema;
import ecnu.db.schema.generation.AbstractSchemaGenerator;
import ecnu.db.schema.generation.TidbSchemaGenerator;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.ReadQuery;
import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneToolChainException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.isEndOfConditionExpr;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author wangqingshuai
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String DUMP_FILE_POSTFIX = "dump";

    private static final int INIT_HASHMAP_SIZE = 16;

    /**
     * 模板化SQL语句
     *
     * @param sql            需要处理的SQL语句
     * @param argsAndIndex   需要替换的arguments
     * @param cannotFindArgs 找不到的arguments
     * @param conflictArgs   矛盾的arguments
     * @return 模板化的SQL语句
     * @throws TouchstoneToolChainException 检测不到停止的语法词
     */
    public static String templatizeSql(String sql, Map<String, List<String>> argsAndIndex, ArrayList<String> cannotFindArgs,
                                       ArrayList<String> conflictArgs) throws TouchstoneToolChainException {
        for (Map.Entry<String, List<String>> argAndIndexes : argsAndIndex.entrySet()) {
            int lastIndex = 0;
            int count = 0;
            if (argAndIndexes.getKey().contains("isnull")) {
                continue;
            }
            if (argAndIndexes.getKey().contains("in(")) {
                while (lastIndex != -1) {
                    lastIndex = sql.indexOf("in (", lastIndex);
                    if (lastIndex != -1) {
                        count++;
                        lastIndex += argAndIndexes.getKey().length();
                    }
                }
                if (count > 1) {
                    conflictArgs.add(argAndIndexes.getKey());
                } else {
                    String backString = sql.substring(sql.indexOf("in (") + "in (".length());
                    backString = backString.substring(backString.indexOf(")") + 1);
                    sql = sql.substring(0, sql.indexOf("in (") + "in ".length()) + argAndIndexes.getValue().get(0) + backString;
                }
            } else {
                while (lastIndex != -1) {
                    lastIndex = sql.indexOf(argAndIndexes.getKey(), lastIndex);
                    if (lastIndex != -1) {
                        count++;
                        lastIndex += argAndIndexes.getKey().length();
                    }
                }
                if (count == 0) {
                    cannotFindArgs.add(argAndIndexes.getKey());
                } else if (count == 1) {
                    int front = sql.indexOf(argAndIndexes.getKey()) + argAndIndexes.getKey().length();
                    StringBuilder backString = new StringBuilder(sql.substring(front + 1));
                    String[] sqlTuples = backString.toString().split(" ");
                    int i = 0;
                    boolean hasBetween = false;
                    if (argAndIndexes.getKey().contains(" bet")) {
                        hasBetween = true;
                    }
                    for (; i < sqlTuples.length; i++) {
                        if (!hasBetween) {
                            if (isEndOfConditionExpr(sqlTuples[i].toLowerCase()) || sqlTuples[i].contains(";")) {
                                break;
                            }
                        } else {
                            if ("and".equals(sqlTuples[i])) {
                                hasBetween = false;
                            }
                        }
                    }
                    if (i < sqlTuples.length) {
                        backString = new StringBuilder();
                        if (sqlTuples[i].contains(";")) {
                            backString.append(";");
                        } else {
                            for (; i < sqlTuples.length; i++) {
                                backString.append(" ").append(sqlTuples[i]);
                            }
                        }
                        if (argAndIndexes.getKey().contains(" bet")) {
                            sql = sql.substring(0, front) + argAndIndexes.getValue().get(0) + backString.toString();
                        } else {
                            sql = String.format("%s'%s'%s", sql.substring(0, front + 1), argAndIndexes.getValue().get(0), backString.toString());
                        }

                    } else {
                        throw new TouchstoneToolChainException("检测不到停止的语法词");
                    }
                } else if (count > 1) {
                    conflictArgs.add(argAndIndexes.getKey());
                }
            }
        }
        return sql;
    }

    public static void main(String[] args) throws Exception {
        SystemConfig systemConfig = SystemConfig.readConfig(args[0]);

        String sqlsDirectory = systemConfig.getSqlsDirectory();
        File sqlInput = new File(systemConfig.getSqlsDirectory());
        File[] files = Optional.ofNullable(sqlInput.listFiles())
                .orElseThrow(() -> new TouchstoneToolChainException(String.format("%s文件夹下没有sql文件", sqlsDirectory)));

        File resultDirectory = new File(systemConfig.getResultDirectory()),
                retDir = new File(systemConfig.getResultDirectory()),
                retSqlDir = new File(systemConfig.getResultDirectory(), "sql"),
                dumpDir = Optional.ofNullable(systemConfig.getDumpDirectory()).map(File::new).orElse(null),
                loadDir = Optional.ofNullable(systemConfig.getLoadDirectory()).map(File::new).orElse(null),
                logDir = new File(systemConfig.getResultDirectory(), "log");
        if (retSqlDir.isDirectory()) {
            FileUtils.deleteDirectory(retSqlDir);
        }
        if (dumpDir != null && dumpDir.isDirectory()) {
            FileUtils.deleteDirectory(dumpDir);
        }
        if (retDir.isDirectory()) {
            FileUtils.deleteDirectory(retDir);
        }
        if (!retSqlDir.mkdirs()) {
            throw new TouchstoneToolChainException("无法创建输出文件夹");
        }
        if (dumpDir != null && !dumpDir.mkdirs()) {
            throw new TouchstoneToolChainException("无法创建持久化输出文件夹");
        }

        DatabaseConnectorInterface dbConnector = getDatabaseConnector(systemConfig, loadDir);
        AbstractSchemaGenerator dbSchemaGenerator = new TidbSchemaGenerator();

        logger.info("开始获取表名");
        List<String> tableNames;
        tableNames = getTableNames(systemConfig.isCrossMultiDatabase(), systemConfig.getDatabaseName(), files, dbConnector);
        logger.info("获取表名成功，表名为:" + tableNames);
        if (dumpDir != null && dumpDir.isDirectory()) {
            dumpTableNames(dumpDir, tableNames);
            logger.info("表名持久化成功");
        }
        logger.info("开始获取表结构和数据分布");

        Pair<Map<String, Schema>, Multimap<String, String>> pair = getSchemas(loadDir, dbConnector, dbSchemaGenerator, tableNames);
        Map<String, Schema> schemas = pair.getKey();
        Multimap<String, String> tblName2CanonicalTblName = pair.getValue();
        logger.info("获取表结构和数据分布成功，开始获取query查询计划");

        AbstractAnalyzer queryAnalyzer = new TidbAnalyzer(systemConfig, dbConnector, schemas, tblName2CanonicalTblName);
        List<String> queryInfos = new LinkedList<>();
        boolean needLog = false;
        for (File sqlFile : files) {
            if (sqlFile.isFile() && sqlFile.getName().endsWith(".sql")) {
                List<String> queries = ReadQuery.getQueriesFromFile(sqlFile.getPath(), queryAnalyzer.getDbType());
                int index = 0;
                List<String[]> queryPlan = new ArrayList<>();
                for (String sql : queries) {
                    BufferedWriter sqlWriter = new BufferedWriter(new FileWriter(
                            new File(retSqlDir.getPath(), sqlFile.getName())));
                    index++;
                    String queryCanonicalName = String.format("%s_%d", sqlFile.getName(), index);
                    try {
                        queryInfos.add("## " + queryCanonicalName);
                        queryPlan = queryAnalyzer.getQueryPlan(queryCanonicalName, sql);
                        if (dumpDir != null && dumpDir.isDirectory()) {
                            dumpQueryPlan(dumpDir, queryPlan, queryCanonicalName);
                        }
                        ExecutionNode root = queryAnalyzer.getExecutionTree(queryPlan);
                        queryInfos.addAll(queryAnalyzer.extractQueryInfos(queryCanonicalName, root));
                        logger.info(String.format("%-15s Status:获取成功", queryCanonicalName));
                        queryAnalyzer.outputSuccess(true);

                        ArrayList<String> cannotFindArgs = new ArrayList<>();
                        ArrayList<String> conflictArgs = new ArrayList<>();
                        sql = templatizeSql(sql, queryAnalyzer.getArgsAndIndex(), cannotFindArgs, conflictArgs);
                        ArrayList<String> reProductArgs = new ArrayList<>();

                        for (String cannotFindArg : cannotFindArgs) {
                            if (cannotFindArg.contains(" bet")) {
                                String[] indexInfos = queryAnalyzer.getArgsAndIndex().
                                        get(cannotFindArg).get(0).split(" ");
                                indexInfos[1] = indexInfos[1].replace("'", "");
                                indexInfos[3] = indexInfos[3].replace("'", "");
                                HashMap<String, List<String>> tempInfo = new HashMap<>(INIT_HASHMAP_SIZE);
                                tempInfo.put(cannotFindArg.split(" ")[0] + " >=", Collections.singletonList(indexInfos[1]));
                                ArrayList<String> tempList = new ArrayList<>();
                                sql = templatizeSql(sql, tempInfo, tempList, new ArrayList<>());
                                if (tempList.size() != 0) {
                                    tempInfo.clear();
                                    tempList.clear();
                                    tempInfo.put(cannotFindArg.split(" ")[0] + " >", Collections.singletonList(indexInfos[1]));
                                    sql = templatizeSql(sql, tempInfo, tempList, new ArrayList<>());
                                }
                                tempInfo.clear();
                                tempList.clear();
                                tempInfo.put(cannotFindArg.split(" ")[0] + " <=", Collections.singletonList(indexInfos[3]));
                                sql = templatizeSql(sql, tempInfo, tempList, new ArrayList<>());
                                if (tempList.size() != 0) {
                                    tempInfo.clear();
                                    tempList.clear();
                                    tempInfo.put(cannotFindArg.split(" ")[0] + " <", Collections.singletonList(indexInfos[3]));
                                    sql = templatizeSql(sql, tempInfo, tempList, new ArrayList<>());
                                }
                                reProductArgs.add(cannotFindArg);
                            }
                        }
                        cannotFindArgs.removeAll(reProductArgs);
                        if (cannotFindArgs.size() > 0) {

                            logger.warn(String.format("请注意%s中有参数无法完成替换，请查看该sql输出，手动替换;", queryCanonicalName));
                            sqlWriter.write("-- cannotFindArgs:");
                            for (String cannotFindArg : cannotFindArgs) {
                                sqlWriter.write(cannotFindArg + ":" + queryAnalyzer.getArgsAndIndex().get(cannotFindArg) + ",");
                            }
                            sqlWriter.write(System.lineSeparator());
                        }
                        if (conflictArgs.size() > 0) {
                            logger.warn(String.format("请注意%s中有参数出现多次，无法智能，替换请查看该sql输出，手动替换;", queryCanonicalName));
                            sqlWriter.write("-- conflictArgs:");
                            for (String conflictArg : conflictArgs) {
                                sqlWriter.write(conflictArg + ":" + queryAnalyzer.getArgsAndIndex().get(conflictArg) + ",");
                            }
                            sqlWriter.write(System.lineSeparator());
                        }

                        sqlWriter.write(SQLUtils.format(sql, queryAnalyzer.getDbType(), SQLUtils.DEFAULT_LCASE_FORMAT_OPTION) + System.lineSeparator());
                        sqlWriter.close();
                    } catch (TouchstoneToolChainException e) {
                        queryAnalyzer.outputSuccess(false);
                        logger.error(String.format("%-15s Status:获取失败", queryCanonicalName), e);
                        needLog = true;
                        if (queryPlan != null && !queryPlan.isEmpty()) {
                            dumpQueryPlan(logDir, queryPlan, queryCanonicalName);
                            logger.info(String.format("失败的query %s的查询计划已经存盘到'%s'", queryCanonicalName, logDir.getAbsolutePath()));
                        }
                    }
                }
            }
        }
        logger.info("获取查询计划完成");
        if (dumpDir != null && dumpDir.isDirectory()) {
            dumpSchemas(dumpDir, schemas);
            logger.info("表结构和数据分布持久化成功");
        }
        if (needLog) {
            dumpMultiCol(logDir, dbConnector);
            dumpSchemas(logDir, schemas);
            dumpTableNames(logDir, tableNames);
            logger.info(String.format("关于表的日志信息已经存盘到'%s'", logDir.getAbsolutePath()));
        }

        if (dumpDir != null && dumpDir.isDirectory()) {
            dumpMultiCol(dumpDir, dbConnector);
        }

        BufferedWriter schemaWriter = new BufferedWriter(new FileWriter(
                new File(resultDirectory.getPath(), "schema.conf")));
        for (Schema schema : schemas.values()) {
            String schemaInfo = schema.formatSchemaInfo(), dataDistributionInfo = schema.formatDataDistributionInfo();
            if (schemaInfo != null) {
                schemaWriter.write(schemaInfo + System.lineSeparator());
            }
            if (dataDistributionInfo != null) {
                schemaWriter.write(dataDistributionInfo + System.lineSeparator());
            }
        }
        schemaWriter.close();
        BufferedWriter ccWriter = new BufferedWriter(new FileWriter(
                new File(resultDirectory.getPath(), "constraintsChain.conf")));
        for (String queryInfo : queryInfos) {
            ccWriter.write(queryInfo + System.lineSeparator());
        }
        ccWriter.close();
    }

    private static DatabaseConnectorInterface getDatabaseConnector(SystemConfig systemConfig, File loadDir) throws TouchstoneToolChainException, IOException, CsvException {
        DatabaseConnectorInterface dbConnector;
        if (loadDir != null && loadDir.isDirectory()) {
            List<String> tableNames = loadTableNames(loadDir);
            Map<String, List<String[]>> queryPlanMap = loadQueryPlans(loadDir);
            Map<String, Integer> multiColNdvMap = loadMultiColMap(loadDir);
            dbConnector = new DumpFileConnector(tableNames, queryPlanMap, multiColNdvMap);
        } else {
            dbConnector = new TidbConnector(systemConfig);
        }
        return dbConnector;
    }

    private static void dumpMultiCol(File dumpDir, DatabaseConnectorInterface dbConnector) throws IOException {
        String content = JSON.toJSONString(dbConnector.getMultiColNdvMap(), true);
        File multiColMapFile = new File(dumpDir.getPath(), "multiColNdv");
        FileUtils.writeStringToFile(multiColMapFile, content, UTF_8);
    }

    private static void dumpQueryPlan(File dumpDir, List<String[]> queryPlan, String queryCanonicalName) throws IOException {
        String content = queryPlan.stream().map((strs) -> String.join(";", strs)).collect(Collectors.joining(System.lineSeparator()));
        File queryPlanFile = new File(dumpDir.getPath(), String.format("%s_dump", queryCanonicalName));
        FileUtils.writeStringToFile(queryPlanFile, content, UTF_8);
    }

    private static void dumpSchemas(File dumpDir, Map<String, Schema> schemas) throws IOException {
        File schemaFile = new File(dumpDir, "schemas");
        for (Schema schema: schemas.values()) {
            schema.setJoinTag(1);
            schema.setLastJoinTag(1);
        }
        FileUtils.writeStringToFile(schemaFile, JSON.toJSONString(schemas, true), UTF_8);
    }

    private static void dumpTableNames(File dumpDir, List<String> tableNames) throws IOException {
        String str = String.join(System.lineSeparator(), tableNames);
        File tableNameFile = new File(dumpDir.getPath(), "tableNames");
        FileUtils.writeStringToFile(tableNameFile, str, UTF_8);
    }

    private static Map<String, Integer> loadMultiColMap(File loadDir) throws TouchstoneToolChainException, IOException {
        Map<String, Integer> multiColNdvMap;
        File multiColNdvFile = new File(loadDir.getPath(), "multiColNdv");
        if (!multiColNdvFile.isFile()) {
            throw new TouchstoneToolChainException(String.format("找不到%s", multiColNdvFile.getAbsolutePath()));
        }
        multiColNdvMap = JSON.parseObject(FileUtils.readFileToString(multiColNdvFile, UTF_8), new TypeReference<>() {
        });
        return multiColNdvMap;
    }

    private static Map<String, List<String[]>> loadQueryPlans(File loadDir) throws IOException, CsvException {
        Map<String, List<String[]>> queryPlanMap = new HashMap<>(INIT_HASHMAP_SIZE);
        for (File queryPlanFile : Optional.ofNullable(loadDir.listFiles((dir, name) -> name.endsWith(DUMP_FILE_POSTFIX))).orElse(new File[]{})) {
            String content = FileUtils.readFileToString(queryPlanFile, UTF_8);
            CSVReader csvReader = new CSVReaderBuilder(new StringReader(content))
                                        .withCSVParser(new CSVParserBuilder().withSeparator(';').build())
                                        .build();
            List<String[]> list = csvReader.readAll();
            queryPlanMap.put(queryPlanFile.getName(), list);
        }
        return queryPlanMap;
    }

    private static Pair<Map<String, Schema>, Multimap<String, String>> getSchemas(File loadDir, DatabaseConnectorInterface dbConnector, AbstractSchemaGenerator dbSchemaGenerator, List<String> tableNames) throws TouchstoneToolChainException, IOException, SQLException {
        Map<String, Schema> schemas = new HashMap<>(tableNames.size());
        if (loadDir != null && loadDir.isDirectory()) {
            File schemaFile = new File(loadDir.getPath(), "schemas");
            if (!schemaFile.isFile()) {
                throw new TouchstoneToolChainException(String.format("找不到%s", schemaFile.getAbsolutePath()));
            }
            schemas = JSON.parseObject(FileUtils.readFileToString(schemaFile, UTF_8), new TypeReference<>() {
            });
            logger.info("加载表结构和表数据分布成功");
        } else {
            for (String canonicalTableName : tableNames) {
                logger.info("开始获取" + canonicalTableName + "的信息");
                logger.info("获取表结构...");
                Schema schema = dbSchemaGenerator.generateSchemaNoKeys(canonicalTableName, ((AbstractDbConnector) dbConnector).getTableDdl(canonicalTableName));
                logger.info("成功");
                logger.info("获取表数据分布...");
                dbSchemaGenerator.setDataRangeBySqlResult(schema.getColumns().values(), ((AbstractDbConnector) dbConnector).getDataRange(canonicalTableName,
                        dbSchemaGenerator.getColumnDistributionSql(schema.getTableName(), schema.getColumns().values())));
                dbSchemaGenerator.setDataRangeUnique(schema, ((AbstractDbConnector) dbConnector));
                logger.info("成功");
                schemas.put(canonicalTableName, schema);
            }
            Schema.initFks(((AbstractDbConnector) dbConnector).databaseMetaData, schemas);
        }
        Multimap<String, String> tbName2CanonicalTbName = ArrayListMultimap.create();
        for (String canonicalTableName: schemas.keySet()) {
            tbName2CanonicalTbName.put(canonicalTableName.split("\\.")[1], canonicalTableName);
        }

        return Pair.of(schemas, tbName2CanonicalTbName);
    }

    private static List<String> loadTableNames(File loadDir) throws TouchstoneToolChainException, IOException {
        File tableNameFile = new File(loadDir.getPath(), "tableNames");
        if (!tableNameFile.isFile()) {
            throw new TouchstoneToolChainException(String.format("找不到%s", tableNameFile.getAbsolutePath()));
        }
        return Arrays.asList(FileUtils.readFileToString(tableNameFile, UTF_8).split(System.lineSeparator()));
    }

    private static List<String> getTableNames(boolean isCrossMultiDatabase,
                                              String databaseName,
                                              File[] files,
                                              DatabaseConnectorInterface dbConnector) throws IOException, SQLException, TouchstoneToolChainException {
        List<String> tableNames;
        if (isCrossMultiDatabase) {
            HashSet<String> tableNameSet = new HashSet<>();
            for (File sqlFile : files) {
                if (sqlFile.isFile() && sqlFile.getName().endsWith(".sql")) {
                    List<String> queries = ReadQuery.getQueriesFromFile(sqlFile.getPath(), "mysql");
                    for (String sql : queries) {
                        Set<String> tableNameRefs = QueryTableName.getTableName(sqlFile.getAbsolutePath(), sql, "mysql", true);
                        tableNameSet.addAll(tableNameRefs);
                    }
                }
            }
            tableNames = new ArrayList<>(tableNameSet);
        } else {
            tableNames = dbConnector.getTableNames().stream().map((name) -> CommonUtils.addDBNamePrefix(databaseName, name)).collect(Collectors.toList());
        }
        return tableNames;
    }


}
