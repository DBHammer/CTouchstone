package ecnu.db.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.utils.exception.schema.CannotFindSchemaException;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static ecnu.db.utils.CommonUtils.CANONICAL_NAME_CONTACT_SYMBOL;
import static ecnu.db.utils.CommonUtils.CANONICAL_NAME_SPLIT_REGEX;

public class TableManager {
    public static final String SCHEMA_MANAGE_INFO = "/schema.json";
    protected static final Logger logger = LoggerFactory.getLogger(TableManager.class);
    private static final TableManager INSTANCE = new TableManager();

    public LinkedHashMap<String, Table> getSchemas() {
        return schemas;
    }

    private LinkedHashMap<String, Table> schemas = new LinkedHashMap<>();
    private File schemaInfoPath;

    public TableManager() {
    }

    public static TableManager getInstance() {
        return INSTANCE;
    }

    public void setResultDir(String resultDir) {
        this.schemaInfoPath = new File(resultDir + SCHEMA_MANAGE_INFO);
    }

    public void storeSchemaInfo() throws IOException {
        String content = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(schemas);
        CommonUtils.writeFile(schemaInfoPath.getPath(), content);
    }

    public void loadSchemaInfo() throws IOException {
        schemas = CommonUtils.MAPPER.readValue(CommonUtils.readFile(schemaInfoPath.getPath()), new TypeReference<>() {
        });
    }

    public void addSchema(String tableName, Table schema) {
        schemas.put(tableName, schema);
    }

    public String getPrimaryKeys(String tableName) throws CannotFindSchemaException {
        return getSchema(tableName).getPrimaryKeys();
    }

    /**
     * 输出指定行的基数
     *
     * @param fkCol 外键列
     * @return 外键行的基数
     */
    public int cardinalityConstraint(String fkCol) {
        String[] cols = fkCol.split("\\.");
        String tableName = cols[0] + "." + cols[1];
        String pkCol = schemas.get(tableName).getForeignKeys().get(fkCol);
        String[] pkCols = pkCol.split("\\.");
        String pkTable = pkCols[0] + "." + pkCols[1];
        double scale = CommonUtils.CardinalityScale;
        if(tableName.equals("public.orders")){
            scale = 1.5;
        }
        return (int) (scale * schemas.get(tableName).getTableSize() / schemas.get(pkTable).getTableSize());
    }


    public boolean isPrimaryKey(String canonicalColumnName) {
        String[] nameArray = canonicalColumnName.split("\\.");
        String tableName = nameArray[0] + "." + nameArray[1];
        Table table = schemas.get(tableName);
        if (table == null) {
            return false;
        }
        return table.getPrimaryKeys().contains(canonicalColumnName);
    }

    public boolean isForeignKey(String canonicalColumnName) {
        String[] nameArray = canonicalColumnName.split("\\.");
        String tableName = nameArray[0] + "." + nameArray[1];
        Table table = schemas.get(tableName);
        if (table == null) {
            return false;
        }
        return table.getForeignKeys().containsKey(canonicalColumnName);
    }

    public boolean containSchema(String tableName) {
        return schemas.containsKey(tableName);
    }

    public long getTableSize(String tableName) throws CannotFindSchemaException {
        return getSchema(tableName).getTableSize();
    }

    public int getJoinTag(String tableName) throws CannotFindSchemaException {
        return getSchema(tableName).getJoinTag();
    }

    public void setPrimaryKeys(String tableName, String primaryKeys) throws TouchstoneException {
        getSchema(tableName).setPrimaryKeys(tableName + "." + primaryKeys);
    }


    public void setForeignKeys(String localTable, String localColumns, String refTable, String refColumns) throws TouchstoneException {
        logger.debug("添加参照依赖： {}.{} 参照 {}.{}", localTable, localColumns, refTable, refColumns);
        getSchema(localTable).addForeignKey(localTable, localColumns, refTable, refColumns);
    }

    public boolean isRefTable(String locTable, String locColumn, String remoteColumn) throws CannotFindSchemaException {
        return getSchema(locTable).isRefTable(locTable + "." + locColumn, remoteColumn);
    }

    public boolean isRefTable(String locTable, String remoteTable) {
        return schemas.get(locTable).isRefTable(remoteTable);
    }


    /**
     * 根据join的连接顺序，排列表名。顺序从被参照表到参照表。
     *
     * @return 从被参照表到参照表排序的表名。
     */
    public List<String> createTopologicalOrder() {
        Graph<String, DefaultEdge> schemaGraph = new DefaultDirectedGraph<>(DefaultEdge.class);
        schemas.keySet().forEach(schemaGraph::addVertex);
        for (Map.Entry<String, Table> schemaName2Schema : schemas.entrySet()) {
            for (String refColumn : schemaName2Schema.getValue().getForeignKeys().values()) {
                String[] refInfo = refColumn.split(CANONICAL_NAME_SPLIT_REGEX);
                schemaGraph.addEdge(refInfo[0] + CANONICAL_NAME_CONTACT_SYMBOL + refInfo[1], schemaName2Schema.getKey());
            }
        }
        TopologicalOrderIterator<String, DefaultEdge> topologicalOrderIterator = new TopologicalOrderIterator<>(schemaGraph);
        List<String> orderedSchemas = new LinkedList<>();
        while (topologicalOrderIterator.hasNext()) {
            orderedSchemas.add(topologicalOrderIterator.next());
        }
        return orderedSchemas;
    }

    public List<String> getColumnNamesNotKey(String schemaName) throws CannotFindSchemaException {
        return getSchema(schemaName).getCanonicalColumnNamesNotFk();
    }

    public List<String> getColumnNames(String schemaName) throws CannotFindSchemaException {
        return getSchema(schemaName).getCanonicalColumnNames();
    }

    public String getPrimaryKeyColumn(String schemaName) throws CannotFindSchemaException {
        List<String> pkNames = new ArrayList<>(getSchema(schemaName).getPrimaryKeysList());
        var fks = getSchema(schemaName).getForeignKeys();
        if (fks != null) {
            pkNames.removeAll(fks.keySet());
        }
        return String.join(",", pkNames);
    }

    public Table getSchema(String tableName) throws CannotFindSchemaException {
        Table schema = schemas.get(tableName);
        if (schema == null) {
            throw new CannotFindSchemaException(tableName);
        }
        return schema;
    }
}
