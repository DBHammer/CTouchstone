package ecnu.db.app;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.filter.ParameterResolver;
import ecnu.db.exception.TouchstoneException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.SchemaManager;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.ColumnDeserializer;
import ecnu.db.utils.config.GenerationConfig;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author alan
 */
public class Generator {
    public static final int SINGLE_THREAD_TUPLE_SIZE = 100;
    private static final Logger logger = LoggerFactory.getLogger(Generator.class);

    public static void generate(GenerationConfig config, SchemaManager schemaManager) throws IOException, TouchstoneException, InterruptedException, ExecutionException {
        ParameterResolver.items.clear();
//        Map<String, List<ConstraintChain>> query2chains =
//                ConstraintChainReader.readConstraintChain(new File(config.getInputPath(), "constraintChain.json"));
        Map<String, Schema> schemas = getSchemas(config);

        List<Schema> topologicalOrder = schemaManager.createTopologicalOrder();
        int threadNum = config.getThreadNum(), neededThreads = threadNum == 1 ? threadNum : threadNum / 2;
        File joinInfoPath = new File(config.getJoinInfoPath());
        if (joinInfoPath.isDirectory()) {
            FileUtils.deleteDirectory(joinInfoPath);
        }
        if (!joinInfoPath.mkdirs()) {
            throw new TouchstoneException(String.format("无法创建'%s'的临时存储文件夹", joinInfoPath.getPath()));
        }
//        for (Schema schema : schemas.values()) {
//            File schemaDir = new File(config.getJoinInfoPath(), schema.getCanonicalTableName());
//            if (!schemaDir.mkdirs()) {
//                throw new TouchstoneException(String.format("无法创建'%s'", schemaDir.getPath()));
//            }
//        }

//        Multimap<String, ConstraintChain> schema2chains = getSchema2Chains(query2chains);

        //     generateTuples(config, schema2chains, schemas, topologicalOrder, neededThreads);
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
