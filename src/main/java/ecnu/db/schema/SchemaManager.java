package ecnu.db.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.dbconnector.InputService;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.exception.schema.CannotFindSchemaException;
import ecnu.db.exception.schema.CircularReferenceException;
import ecnu.db.utils.CommonUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static ecnu.db.utils.CommonUtils.DUMP_FILE_POSTFIX;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SchemaManager {
    protected static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);
    private LinkedHashMap<String, Schema> schemas = new LinkedHashMap<>();
    private static final SchemaManager INSTANCE = new SchemaManager();

    // Private constructor suppresses
    // default public constructor
    private SchemaManager() {
    }

    public void storeSchemaInfo() throws IOException {
        String content = CommonUtils.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schemas);
        FileUtils.writeStringToFile(new File(CommonUtils.getResultDir() + "schema.json"), content, UTF_8);
    }

    public void loadSchemaInfo(String schemaInfoPath) throws IOException {
        schemas = CommonUtils.mapper.readValue(FileUtils.readFileToString(new File(schemaInfoPath), UTF_8),
                new TypeReference<LinkedHashMap<String, Schema>>() {
                });
    }

    public static SchemaManager getInstance() {
        return INSTANCE;
    }

    public void addSchema(String tableName, Schema schema) {
        schemas.put(tableName, schema);
    }

    public String getPrimaryKeys(String tableName) {
        return schemas.get(tableName).getPrimaryKeys();
    }

    public int getTableSize(String tableName) {
        return schemas.get(tableName).getTableSize();
    }

    public int getJoinTag(String tableName) {
        return schemas.get(tableName).getJoinTag();
    }

    public void setPrimaryKeys(String tableName, String primaryKeys) throws TouchstoneException {
        schemas.get(tableName).setPrimaryKeys(primaryKeys);
    }

    public void setForeignKeys(String localTable, String localColumns, String refTable, String refColumns) throws TouchstoneException {
        logger.info("table:" + localTable + ".column:" + localColumns + " -ref- table:" + refTable + ".column:" + refColumns);
        schemas.get(localTable).addForeignKey(localColumns, refTable, refColumns);
    }

    public Schema getSchema(String tableName) throws CannotFindSchemaException {
        Schema schema = schemas.get(tableName);
        if (schema == null) {
            throw new CannotFindSchemaException(tableName);
        }
        return schema;
    }


    /**
     * 根据输入的列名统计非重复值的个数，进而给出该列是否为主键
     *
     * @param pkTable 需要测试的主表
     * @param pkCol   主键
     * @param fkTable 外表
     * @param fkCol   外键
     * @return 该列是否为主键
     * @throws TouchstoneException 由于逻辑错误无法判断是否为主键的异常
     * @throws SQLException        无法通过数据库SQL查询获得多列属性的ndv
     */
    public boolean isPrimaryKey(String pkTable, String pkCol, String fkTable, String fkCol) throws TouchstoneException, SQLException {
        if (schemas.get(fkTable).isRefTable(fkCol, pkCol)) {
            return true;
        }
        if (schemas.get(pkTable).isRefTable(pkCol, fkCol)) {
            return false;
        }
        if (!pkCol.contains(",")) {
            if (ColumnManager.getInstance().getNdv(pkTable + "." + pkCol) ==
                    ColumnManager.getInstance().getNdv(fkTable + "." + fkCol)) {
                return getSchema(pkTable).getTableSize() < getSchema(fkTable).getTableSize();
            } else {
                return ColumnManager.getInstance().getNdv(pkTable + "." + pkCol) >
                        ColumnManager.getInstance().getNdv(fkTable + "." + fkCol);
            }
        } else {
            int leftTableNdv = InputService.getInputService().getDatabaseConnectorInterface().getMultiColNdv(pkTable, pkCol);
            int rightTableNdv = InputService.getInputService().getDatabaseConnectorInterface().getMultiColNdv(fkTable, fkCol);
            if (leftTableNdv == rightTableNdv) {
                return getSchema(pkTable).getTableSize() < getSchema(fkTable).getTableSize();
            } else {
                return leftTableNdv > rightTableNdv;
            }
        }
    }

    public Map<String, Schema> loadSchemas(String schemaPath) throws TouchstoneException, IOException {
        Map<String, Schema> schemas;
        File schemaFile = new File(schemaPath, String.format("schemas.%s", DUMP_FILE_POSTFIX));
        if (!schemaFile.isFile()) {
            throw new TouchstoneException(String.format("找不到%s", schemaFile.getAbsolutePath()));
        }
        schemas = CommonUtils.mapper.readValue(FileUtils.readFileToString(schemaFile, UTF_8), new TypeReference<HashMap<String, Schema>>() {
        });
        return schemas;
    }

    //todo
    public List<Schema> createTopologicalOrder() throws CircularReferenceException {

        return new ArrayList<>();
    }

    private static class GraphNode {
        public Schema schema;
        public int cnt;
        public List<Schema> edges = new LinkedList<>();

        public GraphNode(Schema schema, int cnt) {
            this.schema = schema;
            this.cnt = cnt;
        }
    }
}
