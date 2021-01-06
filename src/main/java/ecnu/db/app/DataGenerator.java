package ecnu.db.app;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.joininfo.JoinInfoTable;
import ecnu.db.joininfo.JoinInfoTableManager;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.SchemaManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static ecnu.db.utils.CommonUtils.stepSize;

@CommandLine.Command(name = "generate", description = "generate database according to gathered information",
        mixinStandardHelpOptions = true, sortOptions = false)
class DataGenerator implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(DataGenerator.class);
    @CommandLine.Option(names = {"-c", "--config_path"}, required = true, description = "the config path for data generation")
    private String configPath;
    @CommandLine.Option(names = {"-o", "--output_path"}, description = "output path for data and join info")
    private String outputPath;
    @CommandLine.Option(names = {"-i", "--generator_id"}, description = "the id of current generator")
    private int generatorId;
    @CommandLine.Option(names = {"-n", "--num"}, description = "size of generators")
    private int generatorNum;

    private static Multimap<String, ConstraintChain> getSchema2Chains(Map<String, List<ConstraintChain>> query2chains) {
        Multimap<String, ConstraintChain> schema2chains = ArrayListMultimap.create();
        for (List<ConstraintChain> chains : query2chains.values()) {
            for (ConstraintChain chain : chains) {
                schema2chains.put(chain.getTableName(), chain);
            }
        }
        return schema2chains;
    }

    @Override
    public Integer call() throws Exception {
        SchemaManager.getInstance().setResultDir(configPath);
        SchemaManager.getInstance().loadSchemaInfo();
        ColumnManager.getInstance().setResultDir(configPath);
        ColumnManager.getInstance().loadColumnDistribution();
        JoinInfoTableManager.getInstance().setJoinInfoTablePath(configPath);
        Map<String, List<ConstraintChain>> query2chains = CommonUtils.loadConstrainChainResult(configPath);
        int stepRange = stepSize * generatorNum;
        Multimap<String, ConstraintChain> schema2chains = getSchema2Chains(query2chains);
        for (String schemaName : SchemaManager.getInstance().createTopologicalOrder()) {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            logger.info("开始输出表数据" + schemaName);
            int resultStart = stepSize * generatorId;
            int tableSize = SchemaManager.getInstance().getTableSize(schemaName);
            List<String> attColumnNames = SchemaManager.getInstance().getColumnNamesNotKey(schemaName);
            List<String> allColumnNames = SchemaManager.getInstance().getColumnNames(schemaName);
            System.out.println(allColumnNames);
            String pkName = SchemaManager.getInstance().getPrimaryKeyColumn(schemaName);
            JoinInfoTable joinInfoTable = new JoinInfoTable();
            while (resultStart < tableSize) {
                int range = Math.min(resultStart + stepSize, tableSize) - resultStart;
                ColumnManager.getInstance().prepareGeneration(attColumnNames, range);
                computeFksAndPkJoinInfo(joinInfoTable, resultStart, range, schema2chains.get(schemaName));
                ColumnManager.getInstance().setData(pkName, LongStream.range(resultStart, resultStart + range).toArray());
                List<List<String>> columnData = allColumnNames.stream().parallel()
                        .map(attColumnName -> ColumnManager.getInstance().getData(attColumnName))
                        .collect(Collectors.toList());
                List<String> rowData = IntStream.range(0, columnData.get(0).size()).parallel()
                        .mapToObj(i -> columnData.stream().map(column -> column.get(i)).collect(Collectors.joining(",")))
                        .collect(Collectors.toList());
                executorService.submit(() -> {
                    try {
                        Files.write(Paths.get(outputPath + "/" + schemaName + generatorId), rowData,
                                StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                resultStart += range + stepRange;
            }
            executorService.shutdown();
            if (executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                logger.info("输出表数据" + schemaName + "完成");
            }
            JoinInfoTableManager.getInstance().putJoinInfoTable(pkName, joinInfoTable);
        }
        return 0;
    }

    /**
     * 准备好生成tuple
     * todo 处理复合主键
     * todo 性能问题
     *
     * @param pkStart 需要生成的数据起始
     * @param range   需要生成的数据范围
     * @param chains  约束链条
     * @throws TouchstoneException 生成失败
     */
    public void computeFksAndPkJoinInfo(JoinInfoTable joinInfoTable, int pkStart, int range,
                                        Collection<ConstraintChain> chains) throws TouchstoneException {
        long[] pkBitMap = new long[range];
        Map<String, long[]> fkBitMap = new HashMap<>();
        for (ConstraintChain chain : chains) {
            chain.evaluate(pkBitMap, fkBitMap);
        }
        Map<Long, List<int[]>> status2RowId = IntStream.range(0, pkBitMap.length).boxed().parallel()
                .collect(Collectors.groupingBy(
                        i -> pkBitMap[i],
                        Collector.of(
                                ArrayList::new,
                                (result, i) -> result.add(new int[]{i + pkStart}),
                                (result1, result2) -> {
                                    result1.addAll(result2);
                                    return result1;
                                }
                        )));
        status2RowId.entrySet().parallelStream().forEach(joinInfoTable::addJoinInfo);
        for (Map.Entry<String, long[]> fk2BitMap : fkBitMap.entrySet()) {
            String[] tables = fk2BitMap.getKey().split(":");
            Map<Long, List<Integer>> bitMap2RowId = IntStream.range(0, fk2BitMap.getValue().length).boxed().parallel()
                    .collect(Collectors.groupingByConcurrent(i -> fk2BitMap.getValue()[i], Collectors.toList()));
            long[] data = new long[range];
            bitMap2RowId.forEach((key, value) -> {
                int[][] keys = JoinInfoTableManager.getInstance().getFks(tables[1], key, value.size());
                IntStream.range(0, value.size()).parallel().forEach(index -> data[value.get(index)] = keys[index][0]);
            });
            ColumnManager.getInstance().setData(tables[0], data);
        }
    }
}
