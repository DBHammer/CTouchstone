package ecnu.db.generator;

import com.google.ortools.Loader;
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.IntVar;
import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.ConstraintChainManager;
import ecnu.db.generator.joininfo.JoinInfoTableManager;
import ecnu.db.generator.joininfo.JoinStatus;
import ecnu.db.generator.joininfo.RuleTableManager;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        Loader.loadNativeLibraries();
        TableManager.getInstance().setResultDir(configPath);
        TableManager.getInstance().loadSchemaInfo();
        ColumnManager.getInstance().setResultDir(configPath);
        ColumnManager.getInstance().loadColumnMetaData();
        ColumnManager.getInstance().loadColumnDistribution();
        ConstraintChainManager.getInstance().setResultDir(configPath);
        JoinInfoTableManager.getInstance().setJoinInfoTablePath(configPath);
        File dataDir = new File(outputPath);
        if (dataDir.isDirectory() && dataDir.listFiles() != null) {
            Arrays.stream(Objects.requireNonNull(dataDir.listFiles()))
                    .filter(File::delete)
                    .forEach(file -> logger.info("删除{}", file.getName()));
        }

    }

    Map<JoinStatus, Long> countStatus(boolean[][] result) {
        int statusNum = result.length;
        if (statusNum == 0) {
            return null;
        }
        int range = result[0].length;
        if (range == 0) {
            return null;
        }
        return IntStream.range(0, range).parallel().mapToObj(rowId -> {
            boolean[] ret = new boolean[statusNum];
            for (int i = 0; i < result.length; i++) {
                ret[i] = result[i][rowId];
            }
            return new JoinStatus(ret);
        }).collect(Collectors.groupingByConcurrent(Function.identity(), Collectors.counting()));
    }

    @Override
    public Integer call() throws Exception {
        init();
        Map<String, List<ConstraintChain>> query2chains = ConstraintChainManager.getInstance().loadConstrainChainResult(configPath);
        ConstraintChainManager.getInstance().cleanConstrainChains(query2chains);
        int stepRange = STEP_SIZE * (generatorNum - 1);
        Map<String, List<ConstraintChain>> schema2chains = getSchema2Chains(query2chains);
        long start = System.currentTimeMillis();
        for (String schemaName : TableManager.getInstance().createTopologicalOrder()) {
            logger.info("开始输出表数据{}", schemaName);
            //对约束链进行分类
            List<ConstraintChain> allChains = schema2chains.get(schemaName);
            List<ConstraintChain> haveFkConstrainChains = new ArrayList<>();
            List<ConstraintChain> onlyPkConstrainChains = new ArrayList<>();
            List<ConstraintChain> fkAndPkConstrainChains = new ArrayList<>();
            SortedMap<String, List<Integer>> involveFks = new TreeMap<>();
            if (allChains != null) {
                ConstraintChainManager.getInstance().classifyConstraintChain(allChains,
                        haveFkConstrainChains, onlyPkConstrainChains, fkAndPkConstrainChains);
                // 统计所有的联合状态
                involveFks = ConstraintChainManager.getInvolvedFks(allChains);
            }
            List<List<boolean[]>> allStatus = ConstraintChainManager.getAllDistinctStatus(involveFks);
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            int resultStart = STEP_SIZE * generatorId;
            int tableSize = TableManager.getInstance().getTableSize(schemaName);

            List<String> attColumnNames = TableManager.getInstance().getColumnNamesNotKey(schemaName);
            String pkName = TableManager.getInstance().getPrimaryKeyColumn(schemaName);

            while (resultStart < tableSize) {
                int range = Math.min(resultStart + STEP_SIZE, tableSize) - resultStart;
                //生成属性列数据
                ColumnManager.getInstance().prepareGeneration(attColumnNames, range);
                //转换为字符串准备输出
                List<StringBuilder> rowData = ConstraintChainManager.generateAttRows(attColumnNames, range);
                //创建主键状态矩阵
                boolean[][] pkStatus = new boolean[onlyPkConstrainChains.size() + fkAndPkConstrainChains.size()][range];
                //处理不需要外键填充的主键状态
                onlyPkConstrainChains.stream().parallel().forEach(constraintChain ->
                        pkStatus[constraintChain.getJoinTag()] = constraintChain.evaluateFilterStatus(range));

                if (!haveFkConstrainChains.isEmpty()) {
                    boolean[][] filterStatus = haveFkConstrainChains.stream().parallel()
                            .map(constraintChain -> constraintChain.evaluateFilterStatus(range)).toArray(boolean[][]::new);
                    SortedMap<JoinStatus, Long> filterHistogram = new TreeMap<>(countStatus(filterStatus));
                    List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus = getAllVarStatus(filterHistogram, allStatus);
                    long[] result = computeWithCpModel(haveFkConstrainChains, filterHistogram, filterStatus2PkStatus, filterStatus, range);
                    List<List<boolean[]>> fkStatus = populateFkStatus(filterHistogram, result, range, filterStatus, allStatus);
                    int chainIndex = 0;
                    for (ConstraintChain fkAndPkConstrainChain : fkAndPkConstrainChains) {
                        fkAndPkConstrainChain.computePkStatus(fkStatus, filterStatus[chainIndex++], pkStatus);
                    }
                    List<StringBuilder> fksList = RuleTableManager.getInstance().populateFks(involveFks, fkStatus);
                    for (int i = 0; i < rowData.size(); i++) {
                        rowData.get(i).insert(0, fksList.get(i));
                    }
                }
                if (pkStatus.length > 0) {
                    var pkStatus2Location = RuleTableManager.getInstance().addRuleTable(pkName, printPkStatusMatrix(pkStatus), resultStart);
                    for (int i = 0; i < rowData.size(); i++) {
                        boolean[] status = new boolean[pkStatus.length];
                        for (int colIndex = 0; colIndex < pkStatus.length; colIndex++) {
                            status[colIndex] = pkStatus[colIndex][i];
                        }
                        rowData.get(i).insert(0, new StringBuilder().append(pkStatus2Location.get(new JoinStatus(status)).getAndIncrement()).append(","));
                    }
                } else if (!pkName.isEmpty()) {
                    for (int i = 0; i < rowData.size(); i++) {
                        rowData.get(i).insert(0, new StringBuilder().append(resultStart + i).append(","));
                    }
                }
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
        }
        logger.info("总用时:{}", System.currentTimeMillis() - start);
        return 0;
    }

    public Map<JoinStatus, Long> printPkStatusMatrix(boolean[][] pkStatus) {
        if (pkStatus.length != 0) {
            logger.debug("PK Info");
            Map<JoinStatus, Long> pkHistogram = countStatus(pkStatus);
            for (Map.Entry<JoinStatus, Long> s : pkHistogram.entrySet()) {
                logger.debug("size:{}\tstatus:{}", s.getValue(), s.getKey());
            }
            return pkHistogram;
        } else {
            return null;
        }
    }

    public int[] computeFilterSizeForEachFilter(boolean[][] filterStatus) {
        int[] filterSize = new int[filterStatus.length];
        for (int i = 0; i < filterStatus.length; i++) {
            int size = 0;
            boolean[] status = filterStatus[i];
            for (boolean b : status) {
                if (b) {
                    size++;
                }
            }
            filterSize[i] = size;
        }
        return filterSize;
    }

    public List<Map.Entry<JoinStatus, List<boolean[]>>> getAllVarStatus(SortedMap<JoinStatus, Long> filterHistogram,
                                                                        List<List<boolean[]>> allPkStatus) {
        List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus = new ArrayList<>();
        for (JoinStatus joinStatus : filterHistogram.keySet()) {
            for (List<boolean[]> status : allPkStatus) {
                filterStatus2PkStatus.add(new AbstractMap.SimpleEntry<>(joinStatus, status));
            }
        }
        return filterStatus2PkStatus;
    }

    public Map<JoinStatus, Integer> getAllVarLocation(SortedMap<JoinStatus, Long> filterHistogram,
                                                      List<List<boolean[]>> allPkStatus) {
        Map<JoinStatus, Integer> filterStatus2JoinLocation = new TreeMap<>();
        int i = 0;
        for (JoinStatus joinStatus : filterHistogram.keySet()) {
            filterStatus2JoinLocation.put(joinStatus, i * allPkStatus.size());
            i++;
        }
        return filterStatus2JoinLocation;
    }

    /**
     * 根据join info table计算不同status的填充数量
     *
     * @param haveFkConstrainChains 具有外键的约束链
     * @param filterHistogram       filter status的统计直方图
     * @param filterStatus2PkStatus filter status -> pk status的所有填充方案
     * @param filterStatus          每一个约束链的filterStatus, 用于计算其过滤的size
     * @param range                 每个填充方案的的上界
     * @return 一个可行的填充方案
     */
    public long[] computeWithCpModel(List<ConstraintChain> haveFkConstrainChains,
                                     SortedMap<JoinStatus, Long> filterHistogram,
                                     List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus,
                                     boolean[][] filterStatus, int range) {
        logger.info("filterHistogram: {}", filterHistogram);
        CpModel model = new CpModel();
        IntVar[] vars = RuleTableManager.getInstance().initModel(filterHistogram, filterStatus2PkStatus, model, range);
        int chainIndex = 0;
        int[] filterSize = computeFilterSizeForEachFilter(filterStatus);
        for (ConstraintChain haveFkConstrainChain : haveFkConstrainChains) {
            haveFkConstrainChain.addConstraint2Model(model, vars, chainIndex, filterSize[chainIndex], range - filterSize[chainIndex], filterStatus2PkStatus);
            chainIndex++;
        }
        CpSolver solver = new CpSolver();
        solver.getParameters().setEnumerateAllSolutions(false);
        CpSolverStatus status = solver.solve(model);
        long[] result = new long[vars.length];
        if (status == CpSolverStatus.OPTIMAL || status == CpSolverStatus.FEASIBLE) {
            logger.info("用时{}ms", solver.wallTime() * 1000);
            int i = 0;
            for (IntVar intVar : vars) {
                result[i++] = solver.value(intVar);
            }
        } else {
            logger.error("No solution found.");
        }
        return result;
    }


    public List<List<boolean[]>> populateFkStatus(SortedMap<JoinStatus, Long> filterHistogram,
                                                  long[] result, int range,
                                                  boolean[][] filterStatus, List<List<boolean[]>> allStatus) {
        List<List<boolean[]>> fkStatus = new ArrayList<>(range);
        Map<JoinStatus, Integer> filterStatus2JoinLocation = getAllVarLocation(filterHistogram, allStatus);
        for (int j = 0; j < filterStatus[0].length; j++) {
            boolean[] status = new boolean[filterStatus.length];
            for (int k = 0; k < filterStatus.length; k++) {
                status[k] = filterStatus[k][j];
            }
            int index = filterStatus2JoinLocation.get(new JoinStatus(status));
            boolean hasNewIndex = false;
            while (result[index] == 0) {
                index++;
                hasNewIndex = true;
            }
            result[index]--;
            if (hasNewIndex) {
                filterStatus2JoinLocation.put(new JoinStatus(status), index);
            }
            fkStatus.add(allStatus.get(index % allStatus.size()));
        }
        return fkStatus;
    }
}
