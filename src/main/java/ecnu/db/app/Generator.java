package ecnu.db.app;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.utils.config.GenerationConfig;
import ecnu.db.utils.exception.TouchstoneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author alan
 */
public class Generator {
    public static final int SINGLE_THREAD_TUPLE_SIZE = 100;
    private static final Logger logger = LoggerFactory.getLogger(Generator.class);

    public static void generate(GenerationConfig config) throws IOException, TouchstoneException, InterruptedException, ExecutionException {
//        ParameterResolver.ITEMS.clear();
//        SchemaManager.getInstance().loadSchemaInfo();
//        ColumnManager.getInstance().loadColumnDistribution();
//        Map<String, List<ConstraintChain>> query2chains = CommonUtils.loadConstrainChainResult();
//        Multimap<String, ConstraintChain> schema2chains = getSchema2Chains(query2chains);
//        for (String schmea : SchemaManager.getInstance().createTopologicalOrder()) {
//            generateTuples(config, schema2chains, schemas, topologicalOrder, neededThreads);
//        }
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

//    /**
//     * 给定一张表，生成并持久化它的所有数据
//     *
//     * @param constraintChains 在表上计算的约束链
//     * @param schema           表的模式定义和生成器定义
//     * @param dataWriter       数据输出接口
//     * @param rangeStart       负责生成的数据开始区间
//     * @param rangeEnd         负责生成的数据结束区间
//     * @throws IOException 无法完成数据写出
//     */
//    private static void generateTuples(Collection<ConstraintChain> constraintChains, Schema schema,
//                                       BufferedWriter dataWriter,
//                                       int rangeStart, int rangeEnd) throws IOException {
//        //todo
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
//    }

//    /**
//     * 准备好生成tuple
//     *
//     * @param pkStart 需要生成的数据起始
//     * @param range   需要生成的数据范围
//     * @param chains  约束链条
//     * @throws TouchstoneException 生成失败
//     */
//    public static JoinInfoTable computeFksAndPkJoinInfo(int pkStart, int range, Collection<ConstraintChain> chains)
//            throws TouchstoneException {
//        Map<Integer, boolean[]> pkBitMap = new HashMap<>();
//        Table<String, Integer, boolean[]> fkBitMap = HashBasedTable.create();
//        for (ConstraintChain chain : chains) {
//            chain.evaluate(range, pkBitMap, fkBitMap);
//        }
//        for (Map.Entry<String, Map<ConstraintChainFkJoinNode, boolean[]>> entry : fkBitMap.rowMap().entrySet()) {
//            Map<ConstraintChainFkJoinNode, boolean[]> fkBitMap4Join = entry.getValue();
//            List<ConstraintChainFkJoinNode> nodes = new ArrayList<>(fkBitMap4Join.keySet()).stream()
//                    .sorted(Comparator.comparingInt(ConstraintChainFkJoinNode::getPkTag)).collect(Collectors.toList());
//            for (int i = 0; i < size; i++) {
//                long bitMap = 1L;
//                for (ConstraintChainFkJoinNode node : nodes) {
//                    bitMap = (fkBitMap4Join.get(node)[i] ? 1L : 0L) & (bitMap << 1);
//                }
//            }
//        }
//    }

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
