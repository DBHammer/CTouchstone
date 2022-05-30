package ecnu.db.generator;


import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.ConstraintChainManager;
import ecnu.db.generator.joininfo.JoinStatus;
import ecnu.db.generator.joininfo.RuleTable;
import ecnu.db.generator.joininfo.RuleTableManager;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


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
    @CommandLine.Option(names = {"-l", "--step_size"}, description = "the size of each batch", defaultValue = "7000000")
    private int stepSize;

    private Map<String, List<ConstraintChain>> schema2chains;

    private DataWriter dataWriter;

    private static Map<String, List<ConstraintChain>> getSchema2Chains(Map<String, List<ConstraintChain>> query2chains) {
        Map<String, List<ConstraintChain>> schema2chains = new HashMap<>();
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
        //载入schema配置文件
        TableManager.getInstance().setResultDir(configPath);
        TableManager.getInstance().loadSchemaInfo();
        //载入分布配置文件
        ColumnManager.getInstance().setResultDir(configPath);
        ColumnManager.getInstance().loadColumnMetaData();
        ColumnManager.getInstance().loadColumnDistribution();
        //载入约束链，并进行transform
        ConstraintChainManager.getInstance().setResultDir(configPath);
        Map<String, List<ConstraintChain>> query2chains = ConstraintChainManager.getInstance().loadConstrainChainResult(configPath);
        ConstraintChainManager.getInstance().cleanConstrainChains(query2chains);
        schema2chains = getSchema2Chains(query2chains);
        // 删除上次生成的数据
        File dataDir = new File(outputPath);
        if (dataDir.isDirectory() && dataDir.listFiles() != null) {
            Arrays.stream(Objects.requireNonNull(dataDir.listFiles()))
                    .filter(File::delete)
                    .forEach(file -> logger.info("删除{}", file.getName()));
        }
        // 初始化数据生成器
        dataWriter = new DataWriter(outputPath, generatorId);
    }

    @Override
    public Integer call() throws Exception {
        init();
        long start = System.currentTimeMillis();
        for (String schemaName : TableManager.getInstance().createTopologicalOrder()) {
            logger.info("开始输出表数据{}", schemaName);
            dataWriter.reset();
            //对约束链进行分类
            List<ConstraintChain> allChains = schema2chains.get(schemaName);
            List<ConstraintChain> haveFkConstrainChains = new ArrayList<>();
            List<ConstraintChain> onlyPkConstrainChains = new ArrayList<>();
            List<ConstraintChain> fkAndPkConstrainChains = new ArrayList<>();
            ConstraintChainManager.getInstance().classifyConstraintChain(allChains,
                    haveFkConstrainChains, onlyPkConstrainChains, fkAndPkConstrainChains);
            KeysGenerator keysGenerator = new KeysGenerator(haveFkConstrainChains);
            long tableSize = TableManager.getInstance().getTableSize(schemaName);
            long resultStart;
            long stepRange;
            long batchSize;
            if (stepSize * generatorNum > tableSize) {
                batchSize = tableSize / generatorNum;
                resultStart = batchSize * generatorId;
                if (generatorId == generatorNum - 1) {
                    batchSize = tableSize - resultStart;
                }
            } else {
                batchSize = stepSize;
                resultStart = stepSize * generatorId;
            }
            stepRange = stepSize * (generatorNum - 1);
            List<String> attColumnNames = TableManager.getInstance().getColumnNamesNotKey(schemaName);
            ColumnManager.getInstance().cacheAttributeColumn(attColumnNames);
            String pkName = TableManager.getInstance().getPrimaryKeyColumn(schemaName);
            int totolSize = 0;
            while (resultStart < tableSize) {
                int range = (int) (Math.min(resultStart + batchSize, tableSize) - resultStart);
                //生成属性列数据
                ColumnManager.getInstance().prepareGeneration(range, true);
                //转换为字符串准备输出
                ExecutorService service = Executors.newSingleThreadExecutor();
                Future<List<StringBuilder>> futureRowData = service.submit(() -> ConstraintChainManager.generateAttRows(attColumnNames, range));
                //创建主键状态矩阵
                boolean[][] pkStatus = new boolean[onlyPkConstrainChains.size() + fkAndPkConstrainChains.size()][range];
                //处理不需要外键填充的主键状态
                onlyPkConstrainChains.stream().parallel().forEach(constraintChain ->
                        pkStatus[constraintChain.getJoinTag()] = constraintChain.evaluateFilterStatus(range));
                List<long[]> fksList = null;
                // 如果存在外键，进行外键填充
                if (!haveFkConstrainChains.isEmpty()) {
                    ConstructCpModel.setPkRange(BigDecimal.valueOf(range).divide(BigDecimal.valueOf(tableSize), CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION));
                    // 计算外键的filter status
                    boolean[][] filterStatus = haveFkConstrainChains.stream().parallel()
                            .map(constraintChain -> constraintChain.evaluateFilterStatus(range)).toArray(boolean[][]::new);
                    // 生成每一行对应的主键状态 row -> col -> col_status
                    List<List<Map.Entry<boolean[], Long>>> fkStatus = keysGenerator.populateFkStatus(haveFkConstrainChains, filterStatus, range);
                    // 根据外键状态和filter status推演主键状态表
                    int chainIndex = 0;
                    for (ConstraintChain fkAndPkConstrainChain : haveFkConstrainChains) {
                        if (fkAndPkConstrainChains.contains(fkAndPkConstrainChain)) {
                            fkAndPkConstrainChain.computePkStatus(fkStatus, filterStatus[chainIndex], pkStatus);
                        }
                        chainIndex++;
                    }
                    //生成外键的填充
                    fksList = keysGenerator.populateFks(fkStatus);
                }
                List<StringBuilder> rowData = futureRowData.get();
                if (fksList != null) {
                    for (int i = 0; i < rowData.size(); i++) {
                        StringBuilder row = new StringBuilder();
                        for (long l : fksList.get(i)) {
                            row.append(l).append(',');
                        }
                        rowData.get(i).insert(0, row);
                    }
                }
                if (pkStatus.length > 0) {
                    Map<JoinStatus, Long> pkHistogram = keysGenerator.printPkStatusMatrix(pkStatus);
                    var pkStatus2Location = RuleTableManager.getInstance().addRuleTable(pkName, pkHistogram, resultStart);
                    for (int i = 0; i < rowData.size(); i++) {
                        boolean[] status = new boolean[pkStatus.length];
                        for (int colIndex = 0; colIndex < pkStatus.length; colIndex++) {
                            status[colIndex] = pkStatus[colIndex][i];
                        }
                        rowData.get(i).insert(0, pkStatus2Location.get(new JoinStatus(status)).getAndIncrement() + ",");
                    }
                } else if (!pkName.isEmpty()) {
                    for (int i = 0; i < rowData.size(); i++) {
                        rowData.get(i).insert(0, (resultStart + i) + ",");
                    }
                }
                dataWriter.addWriteTask(schemaName, rowData);
                totolSize += range;
                resultStart += range + stepRange;
            }
            if (pkName != null && !pkName.isEmpty()) {
                RuleTable ruleTable = RuleTableManager.getInstance().getRuleTable(pkName);
                if (ruleTable != null) {
                    ruleTable.setScaleFactor((double) tableSize / totolSize);
                }
            }
            if (dataWriter.waitWriteFinish()) {
                logger.info("输出表数据{}完成", schemaName);
            }
        }
        logger.info("总用时:{}", System.currentTimeMillis() - start);
        return 0;
    }
}
