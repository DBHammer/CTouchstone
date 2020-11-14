package ecnu.db.generation;

import com.alibaba.druid.DbType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.analyzer.statical.QueryReader;
import ecnu.db.constraintchain.QueryInstantiation;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainReader;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.constraintchain.filter.ParameterResolver;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.exception.analyze.UnsupportedDBTypeException;
import ecnu.db.exception.schema.CircularReferenceException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.*;
import ecnu.db.tidb.TidbInfo;
import ecnu.db.utils.AbstractDatabaseInfo;
import ecnu.db.utils.config.GenerationConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.matchPattern;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author alan
 */
public class Generator {
    private static final Logger logger = LoggerFactory.getLogger(Generator.class);
    public static final int SINGLE_THREAD_TUPLE_SIZE = 100;
    private static final Pattern pattern = Pattern.compile("'(([0-9]+),[0,1])'");

    public static void generate(GenerationConfig config) throws IOException, TouchstoneException, InterruptedException, ExecutionException {
        ParameterResolver.items.clear();
        Map<String, List<ConstraintChain>> query2chains =
                ConstraintChainReader.readConstraintChain(new File(config.getInputPath(), "constraintChain.json"));
        Map<String, Schema> schemas = getSchemas(config);

        AbstractDatabaseInfo databaseInfo;
        switch (config.getDatabaseVersion()) {
            case TiDB3:
            case TiDB4:
                databaseInfo = new TidbInfo(config.getDatabaseVersion());
                break;
            default:
                throw new UnsupportedDBTypeException(config.getDatabaseVersion());
        }

        QueryInstantiation.compute(query2chains.values().stream().flatMap(Collection::stream).collect(Collectors.toList()), schemas);
        List<Schema> topologicalOrder = createTopologicalOrder(schemas);
        int threadNum = config.getThreadNum(), neededThreads = threadNum == 1 ? threadNum : threadNum / 2;
        File joinInfoPath = new File(config.getJoinInfoPath());
        if (joinInfoPath.isDirectory()) {
            FileUtils.deleteDirectory(joinInfoPath);
        }
        if (!joinInfoPath.mkdirs()) {
            throw new TouchstoneException(String.format("无法创建'%s'的临时存储文件夹", joinInfoPath.getPath()));
        }
        for (Schema schema : schemas.values()) {
            File schemaDir = new File(config.getJoinInfoPath(), schema.getCanonicalTableName());
            if (!schemaDir.mkdirs()) {
                throw new TouchstoneException(String.format("无法创建'%s'", schemaDir.getPath()));
            }
        }

        Multimap<String, ConstraintChain> schema2chains = getSchema2Chains(query2chains);
        //todo generate
        //generateTuples(config, schema2chains, schemas, topologicalOrder, neededThreads);

        Map<Integer, Parameter> id2Parameter = getId2Parameter(query2chains);

        DbType staticalDbType = databaseInfo.getStaticalDbVersion();
        for (File sqlFile : Objects.requireNonNull(new File(config.getInputPath(), "sql").listFiles())) {
            if (sqlFile.isFile() && sqlFile.getName().endsWith(".sql")) {
                List<String> queries = QueryReader.getQueriesFromFile(sqlFile.getPath(), staticalDbType);
                int index = 0;
                for (String query : queries) {
                    index++;
                    String queryCanonicalName = String.format("%s_%d", sqlFile.getName(), index);
                    List<List<String>> matches = matchPattern(pattern, query);
                    for (List<String> group : matches) {
                        int parameterId = Integer.parseInt(group.get(2));
                        query = query.replaceAll(group.get(0), String.format("'%s'", id2Parameter.get(parameterId).getData()));
                    }
                    FileUtils.writeStringToFile(new File(config.getOutputPath(), queryCanonicalName), query, UTF_8);
                }
            }

        }
    }

    private static Multimap<String, ConstraintChain> getSchema2Chains(Map<String, List<ConstraintChain>> query2chains) {
        Multimap<String, ConstraintChain> schema2chains = ArrayListMultimap.create();
        for (List<ConstraintChain> chains : query2chains.values()) {
            for (ConstraintChain chain : chains) {
                schema2chains.put(chain.getTableName(), chain);
            }
        }
        return schema2chains;
    }

    /**
     * 给定一张表，生成并持久化它的所有数据
     *
     * @param constraintChains 在表上计算的约束链
     * @param schema           表的模式定义和生成器定义
     * @param dataWriter       数据输出接口
     * @param rangeStart       负责生成的数据开始区间
     * @param rangeEnd         负责生成的数据结束区间
     * @throws IOException 无法完成数据写出
     */
    private static void generateTuples(Collection<ConstraintChain> constraintChains, Schema schema,
                                       BufferedWriter dataWriter,
                                       int rangeStart, int rangeEnd) throws IOException {
        //todo
//        int loop = (rangeEnd - rangeStart) / stepSize;
//        for (int i = 0; i < loop; i++) {
//            for (AbstractColumn column : schema.getColumns().values()) {
//                column.prepareGeneration(stepSize);
//            }
//            //schema.fillForeignKeys(rangeStart, stepSize, constraintChains);
//            rangeStart += stepSize;
//            dataWriter.write(schema.transferData());
//        }
//        if (rangeStart != rangeEnd) {
//            int retainSize = rangeEnd - rangeStart;
//            for (AbstractColumn column : schema.getColumns().values()) {
//                column.prepareGeneration(retainSize);
//            }
//            //schema.fillForeignKeys(rangeStart, retainSize, constraintChains);
//            dataWriter.write(schema.transferData());
//        }
//        dataWriter.close();
    }

    private static Map<Integer, Parameter> getId2Parameter(Map<String, List<ConstraintChain>> query2chains) {
        Map<Integer, Parameter> id2Parameter = new HashMap<>();
        for (String key : query2chains.keySet()) {
            List<Parameter> parameters = query2chains.get(key).stream().flatMap((l) -> l.getParameters().stream()).collect(Collectors.toList());
            parameters.forEach((param) -> {
                id2Parameter.put(param.getId(), param);
            });
        }
        return id2Parameter;
    }

    private static Map<String, Schema> getSchemas(GenerationConfig config) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(AbstractColumn.class, new ColumnDeserializer());
        mapper.registerModule(module);
        mapper.findAndRegisterModules();
        return mapper.readValue(
                FileUtils.readFileToString(new File(config.getInputPath(), "schema.json"), UTF_8),
                new TypeReference<HashMap<String, Schema>>() {
                });
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

    public static List<Schema> createTopologicalOrder(Map<String, Schema> schemas) throws CircularReferenceException {
        Map<String, GraphNode> graph = new HashMap<>();
        Set<Schema> notReferenced = new HashSet<>(schemas.values());
        for (Schema schema : schemas.values()) {
            GraphNode node = new GraphNode(schema, 0);
            graph.put(schema.getCanonicalTableName(), node);
        }
        for (Schema schema : schemas.values()) {
            if (schema.getForeignKeys() != null) {
                graph.get(schema.getCanonicalTableName()).cnt = schema.getForeignKeys().size();
                notReferenced.remove(schema);
                for (String externalColumnName : schema.getForeignKeys().values()) {
                    String[] externalColumnNames = externalColumnName.split("\\.");
                    String externalTableName = String.format("%s.%s", externalColumnNames[0], externalColumnNames[1]);
                    graph.get(externalTableName).edges.add(schema);
                }
            }
        }
        if (notReferenced.isEmpty()) {
            throw new CircularReferenceException();
        }
        List<Schema> topologicalOrder = new ArrayList<>();
        Deque<Schema> notReferencedQueue = new ArrayDeque<>(notReferenced);
        while (!notReferencedQueue.isEmpty()) {
            Schema schema = notReferencedQueue.peek();
            notReferencedQueue.pop();
            topologicalOrder.add(schema);
            for (Schema edge : graph.get(schema.getCanonicalTableName()).edges) {
                GraphNode node = graph.get(edge.getCanonicalTableName());
                node.cnt--;
                if (node.cnt == 0) {
                    notReferencedQueue.push(node.schema);
                }
            }
        }

        return topologicalOrder;
    }
//todo
//    public static String transferData() {
//        Map<String, List<String>> columnName2Data = new HashMap<>();
//        for (String columnName : columns.keySet()) {
//            AbstractColumn column = columns.get(columnName);
//            List<String> data;
//            switch (column.getColumnType()) {
//                case DATE:
//                    data = Arrays.stream(((DateColumn) column).getTupleData())
//                            .parallel()
//                            .map((d) -> String.format("'%s'", DateColumn.FMT.format(d)))
//                            .collect(Collectors.toList());
//                    break;
//                case DATETIME:
//                    data = Arrays.stream(((DateTimeColumn) column).getTupleData())
//                            .parallel()
//                            .map((d) -> String.format("'%s'", DateTimeColumn.FMT.format(d)))
//                            .collect(Collectors.toList());
//                    break;
//                case INTEGER:
//                    data = Arrays.stream(((IntColumn) column).getTupleData())
//                            .parallel()
//                            .mapToObj(Integer::toString)
//                            .collect(Collectors.toList());
//                    break;
//                case DECIMAL:
//                    data = Arrays.stream(((DecimalColumn) column).getTupleData())
//                            .parallel()
//                            .mapToObj((d) -> BigDecimal.valueOf(d).toString())
//                            .collect(Collectors.toList());
//                    break;
//                case VARCHAR:
//                    data = Arrays.stream(((StringColumn) column).getTupleData())
//                            .parallel()
//                            .map((d) -> String.format("'%s'", d))
//                            .collect(Collectors.toList());
//                    break;
//                case BOOL:
//                default:
//                    throw new UnsupportedOperationException();
//            }
//            columnName2Data.put(columnName, data);
//        }
//        StringBuilder data = new StringBuilder();
//        //todo 添加对于数据的格式化的处理
//        return data.toString();
//    }
}
