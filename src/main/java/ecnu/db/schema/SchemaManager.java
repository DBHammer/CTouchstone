package ecnu.db.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.utils.exception.schema.CannotFindSchemaException;
import org.apache.commons.io.FileUtils;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static ecnu.db.utils.CommonUtils.CANONICAL_NAME_CONTACT_SYMBOL;
import static ecnu.db.utils.CommonUtils.CANONICAL_NAME_SPLIT_REGEX;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SchemaManager {
    protected static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);
    private static final SchemaManager INSTANCE = new SchemaManager();
    private LinkedHashMap<String, Schema> schemas = new LinkedHashMap<>();
    private File schemaInfoPath;

    // Private constructor suppresses
    // default public constructor
    private SchemaManager() {
    }

    public static SchemaManager getInstance() {
        return INSTANCE;
    }

    public void setResultDir(String resultDir) {
        if (schemaInfoPath == null) {
            this.schemaInfoPath = new File(resultDir + CommonUtils.SCHEMA_MANAGE_INFO);
        }
    }

    public void storeSchemaInfo() throws IOException {
        String content = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(schemas);
        FileUtils.writeStringToFile(schemaInfoPath, content, UTF_8);
    }

    public void loadSchemaInfo() throws IOException {
        schemas = CommonUtils.MAPPER.readValue(FileUtils.readFileToString(schemaInfoPath, UTF_8),
                new TypeReference<LinkedHashMap<String, Schema>>() {
                });
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

    private Schema getSchema(String tableName) throws CannotFindSchemaException {
        Schema schema = schemas.get(tableName);
        if (schema == null) {
            throw new CannotFindSchemaException(tableName);
        }
        return schema;
    }

    public boolean isRefTable(String locTable, String locColumn, String remoteColumn) throws CannotFindSchemaException {
        return getSchema(locTable).isRefTable(locColumn, remoteColumn);
    }

    public List<String> createTopologicalOrder() {
        Graph<String, DefaultEdge> schemaGraph = new DefaultDirectedGraph<>(DefaultEdge.class);
        schemas.keySet().forEach(schemaGraph::addVertex);
        for (Map.Entry<String, Schema> schemaName2Schema : schemas.entrySet()) {
            for (String refColumn : schemaName2Schema.getValue().getForeignKeys().values()) {
                String[] refInfo = refColumn.split(CANONICAL_NAME_SPLIT_REGEX);
                schemaGraph.addEdge(refInfo[0] + CANONICAL_NAME_CONTACT_SYMBOL + refInfo[1], schemaName2Schema.getKey());
            }
        }
        TopologicalOrderIterator<String, DefaultEdge> topologicalOrderIterator = new TopologicalOrderIterator<>(schemaGraph);
        List<String> schemas = new LinkedList<>();
        while (topologicalOrderIterator.hasNext()) {
            schemas.add(topologicalOrderIterator.next());
        }
        return schemas;
    }
}
