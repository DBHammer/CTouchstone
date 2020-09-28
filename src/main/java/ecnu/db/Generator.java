package ecnu.db;

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
import ecnu.db.exception.CircularReferenceException;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.exception.UnsupportedDBTypeException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.ColumnDeserializer;
import ecnu.db.tidb.TidbInfo;
import ecnu.db.utils.AbstractDatabaseInfo;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.GenerationConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.matchPattern;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * @author alan
 */
public class Generator {
    private static final Logger logger = LoggerFactory.getLogger(Generator.class);
    public static final int SINGLE_THREAD_TUPLE_SIZE = 100;
    private static final Pattern pattern = Pattern.compile("'(([0-9]+),[0,1])'");
    public static void generate(GenerationConfig config) throws IOException, TouchstoneToolChainException, InterruptedException, ExecutionException {
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
            throw new TouchstoneToolChainException(String.format("无法创建'%s'的临时存储文件夹", joinInfoPath.getPath()));
        }
        for (Schema schema : schemas.values()) {
            File schemaDir = new File(config.getJoinInfoPath(), schema.getTableName());
            if (!schemaDir.mkdirs()) {
                throw new TouchstoneToolChainException(String.format("无法创建'%s'", schemaDir.getPath()));
            }
        }

        Multimap<String, ConstraintChain> schema2chains = getSchema2Chains(query2chains);
        generateTuples(config, schema2chains, schemas, topologicalOrder, neededThreads);

        Map<Integer, Parameter> id2Parameter = getId2Parameter(query2chains);

        String staticalDbType = databaseInfo.getStaticalDbVersion();
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

    private static void generateTuples(GenerationConfig config,
                                       Multimap<String, ConstraintChain> schema2chains,
                                       Map<String, Schema> schemas,
                                       List<Schema> topologicalOrder, int neededThreads) throws InterruptedException, IOException, ExecutionException {
        ExecutorService service = Executors.newFixedThreadPool(config.getThreadPoolSize());
        int threadNum = config.getThreadNum(), epochSize = config.getEpochSize();
        for (Schema schema : topologicalOrder) {
            int tableSize = schema.getTableSize();
            int step;
            Collection<ConstraintChain> chains = schema2chains.get(schema.getTableName());
            AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(
                    Paths.get(config.getOutputPath(), schema.getTableName() + CommonUtils.SQL_FILE_POSTFIX), WRITE, CREATE);
            if (tableSize > SINGLE_THREAD_TUPLE_SIZE * threadNum) {
                step = tableSize / threadNum;
            } else {
                step = SINGLE_THREAD_TUPLE_SIZE;
            }
            List<Future<Integer>> tasks = new ArrayList<>();
            String newDatabaseName = (!config.isCrossMultiDatabase() && config.getDatabaseName() != null) ? config.getDatabaseName() : null;
            for (int i = 0; i < tableSize; i += step) {
                int finalI = i;
                Future<Integer> task = service.submit(() -> {
                    int tupleSize = (finalI + step) > tableSize ? tableSize - finalI : step;
                    try {
                        List<Future<Integer>> futures = new ArrayList<>();
                        for (int j = 0; j < tupleSize; j += epochSize) {
                            int prepareSize = (j + epochSize) > tupleSize ? tupleSize - j : epochSize;
                            for (AbstractColumn column : schema.getColumns().values()) {
                                column.prepareGeneration(prepareSize);
                            }
                            schema.prepareTuples(prepareSize, chains, schemas, config.getJoinInfoPath(), neededThreads);
                            String sqls = schema.generateInsertSqls(prepareSize, newDatabaseName);
                            ByteBuffer buffer = ByteBuffer.wrap(sqls.getBytes(UTF_8));
                            Future<Integer> future = fileChannel.write(buffer, 0);
                            futures.add(future);
                        }
                        for (Future<Integer> future : futures) {
                            future.get();
                        }
                    } catch (TouchstoneToolChainException | IOException | InterruptedException | ExecutionException e) {
                        logger.error(String.format("thread %d generate tuple failed", Thread.currentThread().getId()), e);
                        System.exit(1);
                    }
                    return 0;
                });
                tasks.add(task);
            }
            for (Future<Integer> task : tasks) {
                task.get();
            }
        }

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
            graph.put(schema.getTableName(), node);
        }
        for (Schema schema : schemas.values()) {
            if (schema.getForeignKeys() != null) {
                graph.get(schema.getTableName()).cnt = schema.getForeignKeys().size();
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
            for (Schema edge : graph.get(schema.getTableName()).edges) {
                GraphNode node = graph.get(edge.getTableName());
                node.cnt--;
                if (node.cnt == 0) {
                    notReferencedQueue.push(node.schema);
                }
            }
        }

        return topologicalOrder;
    }
}
