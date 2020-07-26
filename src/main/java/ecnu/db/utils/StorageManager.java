package ecnu.db.utils;

import com.alibaba.druid.sql.SQLUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.ColumnDeserializer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.DUMP_FILE_POSTFIX;
import static ecnu.db.utils.CommonUtils.INIT_HASHMAP_SIZE;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author alan
 */
public class StorageManager {
    private final File retDir;
    private final File retSqlDir;
    private final File dumpDir;
    private final File dumpQueryDir;
    private final File loadDir;
    private final File logDir;
    private final File logQueryDir;

    public StorageManager(String resultDirPath, String dumpDirPath, String loadDirPath, String logPath) {
        retDir = new File(resultDirPath);
        retSqlDir = new File(resultDirPath, "sql");
        dumpDir = Optional.ofNullable(dumpDirPath).map(File::new).orElse(null);
        dumpQueryDir = Optional.ofNullable(dumpDirPath).map((dir) -> (new File(dir, "query"))).orElse(null);
        loadDir = Optional.ofNullable(loadDirPath).map(File::new).orElse(null);
        logDir = new File(logPath);
        logQueryDir = Optional.of(logPath).map((dir) -> (new File(dir, "query"))).orElse(null);
    }

    public void storeSqlResult(File sqlFile, String sql, String dbType) throws IOException {
        String content = SQLUtils.format(sql, dbType, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION) + System.lineSeparator();
        FileUtils.writeStringToFile(new File(retSqlDir.getPath(), sqlFile.getName()), content, UTF_8);
    }

    public void storeSchemaResult(Map<String, Schema> schemas) throws ParseException, IOException {
        StringBuilder sb = new StringBuilder();
        for (Schema schema : schemas.values()) {
            String schemaInfo = schema.formatSchemaInfo(), dataDistributionInfo = schema.formatDataDistributionInfo();
            if (schemaInfo != null) {
                sb.append(schemaInfo).append(System.lineSeparator());
            }
            if (dataDistributionInfo != null) {
                sb.append(dataDistributionInfo).append(System.lineSeparator());
            }
        }
        FileUtils.writeStringToFile(new File(retDir.getPath(), "schema.conf"), sb.toString(), UTF_8);
    }

    public void storeConstrainChainResult(List<String> queryInfos) throws IOException {
        String content = queryInfos.stream().collect(Collectors.joining(System.lineSeparator()));
        FileUtils.writeStringToFile(new File(retDir.getPath(), "constraintChain.conf"), content, UTF_8);
    }

    public void dumpStaticInfo(Map<String, Integer> multiColNdvMap, Map<String, Schema> schemas, List<String> tableNames) throws IOException {
        storeStaticInfo(dumpDir, multiColNdvMap, schemas, tableNames);
    }

    public void logStaticInfo(Map<String, Integer> multiColNdvMap, Map<String, Schema> schemas, List<String> tableNames) throws IOException {
        storeStaticInfo(dumpDir, multiColNdvMap, schemas, tableNames);
    }

    public void dumpQueryPlan(List<String[]> queryPlan, String queryCanonicalName) throws IOException {
        storeQueryPlan(dumpQueryDir, queryPlan, queryCanonicalName);
    }

    public void logQueryPlan(List<String[]> queryPlan, String queryCanonicalName) throws IOException {
        storeQueryPlan(logQueryDir, queryPlan, queryCanonicalName);
    }

    private void storeStaticInfo(File dumpDir, Map<String, Integer> multiColNdvMap, Map<String, Schema> schemas, List<String> tableNames) throws IOException {
        File multiColMapFile = new File(dumpDir.getPath(), String.format("multiColNdv.%s", DUMP_FILE_POSTFIX));
        String multiColContent = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(multiColNdvMap);
        FileUtils.writeStringToFile(multiColMapFile, multiColContent, UTF_8);
        File schemaFile = new File(dumpDir, String.format("schemas.%s", DUMP_FILE_POSTFIX));
        for (Schema schema : schemas.values()) {
            schema.setJoinTag(1);
            schema.setLastJoinTag(1);
        }
        String schemasContent = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(schemas);
        FileUtils.writeStringToFile(schemaFile, schemasContent, UTF_8);
        String tableNamesContent = String.join(System.lineSeparator(), tableNames);
        File tableNameFile = new File(dumpDir.getPath(), String.format("tableNames.%s", DUMP_FILE_POSTFIX));
        FileUtils.writeStringToFile(tableNameFile, tableNamesContent, UTF_8);
    }

    private void storeQueryPlan(File dumpDir, List<String[]> queryPlan, String queryCanonicalName) throws IOException {
        String content = queryPlan.stream().map((strs) -> String.join(";", strs)).collect(Collectors.joining(System.lineSeparator()));
        File queryPlanFile = new File(dumpDir.getPath(), String.format("%s.%s", queryCanonicalName, DUMP_FILE_POSTFIX));
        FileUtils.writeStringToFile(queryPlanFile, content, UTF_8);
    }

    public List<String> loadTableNames() throws TouchstoneToolChainException, IOException {
        File tableNameFile = new File(loadDir.getPath(), String.format("tableNames.%s", DUMP_FILE_POSTFIX));
        if (!tableNameFile.isFile()) {
            throw new TouchstoneToolChainException(String.format("找不到%s", tableNameFile.getAbsolutePath()));
        }
        return Arrays.asList(FileUtils.readFileToString(tableNameFile, UTF_8).split(System.lineSeparator()));
    }

    public Map<String, Schema> loadSchemas() throws TouchstoneToolChainException, IOException {
        Map<String, Schema> schemas;
        File schemaFile = new File(loadDir.getPath(), String.format("schemas.%s", DUMP_FILE_POSTFIX));
        if (!schemaFile.isFile()) {
            throw new TouchstoneToolChainException(String.format("找不到%s", schemaFile.getAbsolutePath()));
        }
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(AbstractColumn.class, new ColumnDeserializer());
        mapper.registerModule(module);
        schemas = mapper.readValue(FileUtils.readFileToString(schemaFile, UTF_8), new TypeReference<HashMap<String, Schema>>() {
        });
        return schemas;
    }

    public Map<String, Integer> loadMultiColMap() throws TouchstoneToolChainException, IOException {
        Map<String, Integer> multiColNdvMap;
        File multiColNdvFile = new File(loadDir.getPath(), String.format("multiColNdv.%s", DUMP_FILE_POSTFIX));
        if (!multiColNdvFile.isFile()) {
            throw new TouchstoneToolChainException(String.format("找不到%s", multiColNdvFile.getAbsolutePath()));
        }
        multiColNdvMap = new ObjectMapper().readValue(FileUtils.readFileToString(multiColNdvFile, UTF_8), new TypeReference<HashMap<String, Integer>>() {
        });
        return multiColNdvMap;
    }

    public Map<String, List<String[]>> loadQueryPlans() throws IOException, CsvException {
        Map<String, List<String[]>> queryPlanMap = new HashMap<>(INIT_HASHMAP_SIZE);
        for (File queryPlanFile : Optional.ofNullable((new File(loadDir, "query")).listFiles()).orElse(new File[]{})) {
            String content = FileUtils.readFileToString(queryPlanFile, UTF_8);
            CSVReader csvReader = new CSVReaderBuilder(new StringReader(content))
                    .withCSVParser(new CSVParserBuilder().withSeparator(';').build())
                    .build();
            List<String[]> list = csvReader.readAll();
            queryPlanMap.put(queryPlanFile.getName(), list);
        }
        return queryPlanMap;
    }

    public void init() throws TouchstoneToolChainException, IOException {
        deleteDirIfExists(dumpDir);
        deleteDirIfExists(retDir);
        deleteDirIfExists(logDir);
        if (!retSqlDir.mkdirs()) {
            throw new TouchstoneToolChainException("无法创建结果文件夹");
        }
        if (!logQueryDir.mkdirs()) {
            throw new TouchstoneToolChainException("无法创建日志文件夹");
        }
        if (dumpQueryDir != null && !dumpQueryDir.mkdirs()) {
            throw new TouchstoneToolChainException("无法创建持久化文件夹");
        }
    }

    private void deleteDirIfExists(File dir) throws IOException {
        if (dir != null && dir.isDirectory()) {
            FileUtils.deleteDirectory(dir);
        }
    }

    public boolean isLoad() {
        return loadDir != null && loadDir.isDirectory();
    }

    public boolean isDump() {
        return dumpDir != null && dumpDir.isDirectory();
    }

    public File getLogDir() {
        return logDir;
    }
}
