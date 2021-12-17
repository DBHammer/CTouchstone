package ecnu.db.generator;

import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.ConstraintChainManager;
import ecnu.db.generator.joininfo.JoinInfoTable;
import ecnu.db.generator.joininfo.JoinInfoTableManager;
import ecnu.db.schema.Column;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.TableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
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

import static ecnu.db.utils.CommonUtils.STEP_SIZE;

@CommandLine.Command(name = "generate", description = "generate database according to gathered information",
        mixinStandardHelpOptions = true, sortOptions = false)
public class DataGenerator implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(DataGenerator.class);
    @CommandLine.Option(names = {"-c", "--config_path"}, required = true, description = "the config path for data generation")
    private String configPath;
    @CommandLine.Option(names = {"-o", "--output_path"}, description = "output path for data and join info")
    private String outputPath;
    @CommandLine.Option(names = {"-i", "--generator_id"}, description = "the id of current generator")
    private int generatorId;
    @CommandLine.Option(names = {"-n", "--num"}, description = "size of generators")
    private int generatorNum;

    private static Map<String, Collection<ConstraintChain>> getSchema2Chains(Map<String, List<ConstraintChain>> query2chains) {
        Map<String, Collection<ConstraintChain>> schema2chains = new HashMap<>();
        for (List<ConstraintChain> chains : query2chains.values()) {
            for (ConstraintChain chain : chains) {
                if (!schema2chains.containsKey(chain.getTableName())) {
                    schema2chains.put(chain.getTableName(), new ArrayList<>());
                }
                schema2chains.get(chain.getTableName()).add(chain);
            }
        }
        return schema2chains;
    }

    private void init() throws IOException {
        TableManager.getInstance().setResultDir(configPath);
        TableManager.getInstance().loadSchemaInfo();
        ColumnManager.getInstance().setResultDir(configPath);
        ColumnManager.getInstance().loadColumnMetaData();
        ConstraintChainManager.getInstance().setResultDir(configPath);
        JoinInfoTableManager.getInstance().setJoinInfoTablePath(configPath);
        File dataDir = new File(outputPath);
        if (dataDir.isDirectory() && dataDir.listFiles() != null) {
            Arrays.stream(Objects.requireNonNull(dataDir.listFiles()))
                    .filter(File::delete)
                    .forEach(file -> logger.info("删除{}", file.getName()));
        }
    }

    @Override
    public Integer call() throws Exception {
        init();
        Map<String, List<ConstraintChain>> query2chains = ConstraintChainManager.getInstance().loadConstrainChainResult(configPath);
        int stepRange = STEP_SIZE * generatorNum;
        Map<String, Collection<ConstraintChain>> schema2chains = getSchema2Chains(query2chains);
        for (String schemaName : TableManager.getInstance().createTopologicalOrder()) {
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            logger.info("开始输出表数据{}", schemaName);
            int resultStart = STEP_SIZE * generatorId;
            int tableSize = TableManager.getInstance().getTableSize(schemaName);
            List<String> attColumnNames = TableManager.getInstance().getColumnNamesNotKey(schemaName);
            List<String> allColumnNames = TableManager.getInstance().getColumnNames(schemaName);
            String pkName = TableManager.getInstance().getPrimaryKeyColumn(schemaName);
            JoinInfoTable joinInfoTable = new JoinInfoTable();
            while (resultStart < tableSize) {
                int range = Math.min(resultStart + STEP_SIZE, tableSize) - resultStart;
                ColumnManager.getInstance().prepareGeneration(attColumnNames, range);
                List<String> rowData = generateAttRows(attColumnNames, range);
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
                logger.info("输出表数据{}完成", schemaName);
            }
            JoinInfoTableManager.getInstance().putJoinInfoTable(pkName, joinInfoTable);
        }
        return 0;
    }

    private static List<String> generateAttRows(List<String> attColumnNames, int range) {
        List<Column> columns = attColumnNames.stream().map(col -> ColumnManager.getInstance().getColumn(col)).toList();
        return IntStream.range(0, range).parallel()
                .mapToObj(
                        rowId -> columns.stream()
                                .map(col -> col.output(rowId))
                                .collect(Collectors.joining(","))
                ).toList();
    }


}
