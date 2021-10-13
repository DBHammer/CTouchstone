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
    protected static final Logger logger = LoggerFactory.getLogger(TableManager.class);
    private static final TableManager INSTANCE = new TableManager();
    private LinkedHashMap<String, Table> schemas = new LinkedHashMap<>();
    public static final String SCHEMA_MANAGE_INFO = "/schema.json";
    private File schemaInfoPath;

    private TableManager() {
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

    public boolean containSchema(String tableName) {
        return schemas.containsKey(tableName);
    }

    public int getTableSize(String tableName) throws CannotFindSchemaException {
        return getSchema(tableName).getTableSize();
    }

    public long getJoinTag(String tableName) throws CannotFindSchemaException {
        return getSchema(tableName).getJoinTag();
    }

    public void setPrimaryKeys(String tableName, String primaryKeys) throws TouchstoneException {
        getSchema(tableName).setPrimaryKeys(tableName + "." + primaryKeys);
    }

    public void setForeignKeys(String localTable, String localColumns, String refTable, String refColumns) throws TouchstoneException {
        logger.info("table:{}, column:{} -ref- table:{}, column:{}", localTable, localColumns, refTable, refColumns);
        getSchema(localTable).addForeignKey(localTable, localColumns, refTable, refColumns);
    }

    public boolean isRefTable(String locTable, String locColumn, String remoteColumn) throws CannotFindSchemaException {
        return getSchema(locTable).isRefTable(locTable + "." + locColumn, remoteColumn);
    }

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
        Collections.reverse(orderedSchemas);
        return orderedSchemas;
    }

    public List<String> getColumnNamesNotKey(String schemaName) throws CannotFindSchemaException {
        return getSchema(schemaName).getCanonicalColumnNamesNotFk();
    }

    public List<String> getColumnNames(String schemaName) throws CannotFindSchemaException {
        return getSchema(schemaName).getCanonicalColumnNames();
    }

    public String getPrimaryKeyColumn(String schemaName) throws CannotFindSchemaException {
        return getSchema(schemaName).getPrimaryKeys();
    }

    public Table getSchema(String tableName) throws CannotFindSchemaException {
        Table schema = schemas.get(tableName);
        if (schema == null) {
            throw new CannotFindSchemaException(tableName);
        }
        return schema;
    }
}
