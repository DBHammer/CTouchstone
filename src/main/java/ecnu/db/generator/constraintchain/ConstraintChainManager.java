package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.generator.constraintchain.join.ConstraintChainFkJoinNode;
import ecnu.db.generator.joininfo.RuleTableManager;
import ecnu.db.schema.Column;
import ecnu.db.schema.ColumnManager;
import ecnu.db.utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ecnu.db.utils.CommonUtils.readFile;

public class ConstraintChainManager {
    public static final String CONSTRAINT_CHAINS_INFO = "/constraintChain.json";
    private static final Logger logger = LoggerFactory.getLogger(ConstraintChainManager.class);
    private static final ConstraintChainManager INSTANCE = new ConstraintChainManager();
    private static final String[] COLOR_LIST = {"#FFFFCC", "#CCFFFF", "#FFCCCC"};
    private static final String GRAPH_TEMPLATE = "digraph \"%s\" {rankdir=BT;" + System.lineSeparator() + "%s}";
    private String resultDir;

    private ConstraintChainManager() {
    }

    public static ConstraintChainManager getInstance() {
        return INSTANCE;
    }

    public void setResultDir(String resultDir) {
        this.resultDir = resultDir;
    }

    public Map<String, List<ConstraintChain>> loadConstrainChainResult(String resultDir) throws IOException {
        return CommonUtils.MAPPER.readValue(readFile(resultDir + CONSTRAINT_CHAINS_INFO), new TypeReference<>() {
        });
    }

    /**
     * 清理不影响键值填充的约束链
     *
     * @param query2chains query和约束链的map
     */
    public void cleanConstrainChains(Map<String, List<ConstraintChain>> query2chains) {
        for (var query2ConstraintChains : query2chains.entrySet()) {
            Iterator<ConstraintChain> constraintChainIterator = query2ConstraintChains.getValue().iterator();
            while (constraintChainIterator.hasNext()) {
                ConstraintChain constraintChain = constraintChainIterator.next();
                if (constraintChain.getNodes().stream().allMatch(
                        node -> node.getConstraintChainNodeType() == ConstraintChainNodeType.FILTER)) {
                    logger.info("由于没有参与Join和与键值有关的Aggregation, 移除查询{}中的约束链{}", query2ConstraintChains.getKey(), constraintChain);
                    constraintChainIterator.remove();
                }
            }
        }
    }

    /**
     * 返回所有约束链的参照表和参照的Tag
     *
     * @param constraintChains 需要分析的约束链
     * @return 所有参照表和参照的Tag
     */
    public static SortedMap<String, List<Integer>> getInvolvedFks(List<ConstraintChain> constraintChains) {
        SortedMap<String, List<Integer>> involveFks = new TreeMap<>();
        // 找到涉及到的参照表和参照的tag
        constraintChains.forEach(constraintChain -> constraintChain.getNodes().stream()
                .filter(constraintChainNode -> constraintChainNode.getConstraintChainNodeType() == ConstraintChainNodeType.FK_JOIN)
                .map(ConstraintChainFkJoinNode.class::cast)
                .forEach(fkJoinNode -> {
                            involveFks.computeIfAbsent(fkJoinNode.getRefCols(), v -> new ArrayList<>());
                            involveFks.get(fkJoinNode.getRefCols()).add(fkJoinNode.getPkTag());
                        }
                ));
        //对所有的位置进行排序
        involveFks.values().forEach(Collections::sort);
        //标记约束链对应的status的位置
        constraintChains.forEach(constraintChain -> constraintChain.getNodes().stream()
                .filter(constraintChainNode -> constraintChainNode.getConstraintChainNodeType() == ConstraintChainNodeType.FK_JOIN)
                .map(ConstraintChainFkJoinNode.class::cast)
                .forEach(fkJoinNode -> {
                            fkJoinNode.joinStatusIndex = involveFks.keySet().stream().toList().indexOf(fkJoinNode.getRefCols());
                            fkJoinNode.joinStatusLocation = involveFks.get(fkJoinNode.getRefCols()).indexOf(fkJoinNode.getPkTag());
                        }
                )
        );
        return involveFks;
    }

    /**
     * 输入约束链，返回涉及到所有状态 组织结构如下
     * pkCol1 ---- pkCol2 ----- pkCol3
     * status1 --- status2 ---- status3
     * status11--- status22---- status33
     * ......
     *
     * @param involveFks 涉及到参照主键
     * @return 所有可能的状态组
     */
    public static List<List<boolean[]>> getAllDistinctStatus(SortedMap<String, List<Integer>> involveFks) {
        // 获得每个列的涉及到的status
        // 表 -> 表内所有的不同join status -> join status
        List<List<boolean[]>> col2AllStatus = involveFks.entrySet().stream()
                .map(col2Location -> RuleTableManager.getInstance().getAllStatusRule(col2Location.getKey(), col2Location.getValue()))
                .toList();
        int diffStatusSize = 1;
        for (List<boolean[]> tableStatus : col2AllStatus) {
            diffStatusSize *= tableStatus.size();
        }
        int[] loopSize = new int[col2AllStatus.size()];
        int currentSize = 1;
        for (int i = 0; i < col2AllStatus.size(); i++) {
            currentSize = col2AllStatus.get(i).size() * currentSize;
            loopSize[i] = diffStatusSize / currentSize;
        }
        List<List<boolean[]>> allDiffStatus = IntStream.range(0, diffStatusSize)
                .mapToObj(index -> {
                    List<boolean[]> result = new ArrayList<>();
                    for (int i = 0; i < col2AllStatus.size(); i++) {
                        List<boolean[]> tableStatus = col2AllStatus.get(i);
                        result.add(tableStatus.get((index) / loopSize[i] % tableStatus.size()));
                    }
                    return result;
                }).toList();
        if (!allDiffStatus.isEmpty()) {
            logger.debug("共计{}种状态，参照列为{}", allDiffStatus.size(), involveFks);
            for (List<boolean[]> booleans : allDiffStatus) {
                logger.debug(booleans.stream().map(Arrays::toString).collect(Collectors.joining("\t\t")));
            }
        }
        return allDiffStatus;
    }

    /**
     * 对一组约束链进行分类
     *
     * @param allChains              输入的约束链
     * @param haveFkConstrainChains  含有外键和Agg的约束链
     * @param onlyPkConstrainChains  只有主键的约束链
     * @param fkAndPkConstrainChains 既含有主键又含有外键的约束链
     */
    public void classifyConstraintChain(List<ConstraintChain> allChains, List<ConstraintChain> haveFkConstrainChains,
                                        List<ConstraintChain> onlyPkConstrainChains, List<ConstraintChain> fkAndPkConstrainChains) {
        for (ConstraintChain constraintChain : allChains) {
            if (constraintChain.getNodes().stream().allMatch(node ->
                    node.getConstraintChainNodeType() == ConstraintChainNodeType.PK_JOIN ||
                            node.getConstraintChainNodeType() == ConstraintChainNodeType.FILTER)) {
                onlyPkConstrainChains.add(constraintChain);
            } else {
                if (constraintChain.getNodes().stream().anyMatch(node ->
                        node.getConstraintChainNodeType() == ConstraintChainNodeType.PK_JOIN)) {
                    fkAndPkConstrainChains.add(constraintChain);
                }
                haveFkConstrainChains.add(constraintChain);
            }
        }
        logger.debug("含有外键约束的链有{}条，不需要推导主键信息的约束链有{}条，需要推导主键信息的约束链有{}条",
                haveFkConstrainChains.size(), onlyPkConstrainChains.size(), fkAndPkConstrainChains.size());
    }


    public void storeConstraintChain(Map<String, List<ConstraintChain>> query2constraintChains) throws IOException {
        String allConstraintChainsContent = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(query2constraintChains);
        CommonUtils.writeFile(resultDir + CONSTRAINT_CHAINS_INFO, allConstraintChainsContent);
        if (new File(resultDir + "/pic/").mkdir()) {
            logger.info("创建约束链的图形化文件夹");
        }
        for (Map.Entry<String, List<ConstraintChain>> stringListEntry : query2constraintChains.entrySet()) {
            String path = resultDir + "/pic/" + stringListEntry.getKey() + ".dot";
            File file = new File(resultDir + "/pic/");
            File[] array = file.listFiles();
            assert array != null;
            if (!graphIsExists(array, stringListEntry.getKey() + ".dot")) {
                String graph = presentConstraintChains(stringListEntry.getKey(), stringListEntry.getValue());
                CommonUtils.writeFile(path, graph);
            } else {
                String oldGraph = Files.readString(Paths.get(path));
                String graph = presentConstraintChains(stringListEntry.getKey(), stringListEntry.getValue());
                if (removeData(graph).equals(removeData(oldGraph))) {
                    CommonUtils.writeFile(path, graph);
                } else {
                    //String currentTime = Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()).format(DateTimeFormatter.RFC_1123_DATE_TIME);
                    Calendar date = Calendar.getInstance();
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
                    String currentTime = format.format(date.getTime());
                    String newPath = resultDir + "/pic/" + currentTime + "_" + stringListEntry.getKey() + ".dot";
                    CommonUtils.writeFile(newPath + "", graph);
                    logger.warn("graph {} is different", stringListEntry.getKey());
                }
            }
        }
    }

    private String presentConstraintChains(String queryName, List<ConstraintChain> constraintChains) {
        StringBuilder graph = new StringBuilder();
        HashMap<String, ConstraintChain.SubGraph> subGraphHashMap = new HashMap<>(constraintChains.size());
        constraintChains.sort(Comparator.comparing(ConstraintChain::getTableName));
        for (int i = 0; i < constraintChains.size(); i++) {
            graph.append(constraintChains.get(i).presentConstraintChains(subGraphHashMap, COLOR_LIST[i % COLOR_LIST.length]));
        }
        String subGraphs = subGraphHashMap.values().stream().
                map(ConstraintChain.SubGraph::toString).sorted().collect(Collectors.joining(""));

        return String.format(GRAPH_TEMPLATE, queryName, subGraphs + graph);
    }

    private boolean graphIsExists(File[] array, String graphName) {
        return Arrays.stream(array).map(File::getName).anyMatch(fileName -> fileName.equals(graphName));
    }

    public static List<StringBuilder> generateAttRows(List<String> attColumnNames, int range) {
        List<Column> columns = attColumnNames.stream().map(col -> ColumnManager.getInstance().getColumn(col)).toList();
        return IntStream.range(0, range).parallel()
                .mapToObj(
                        rowId -> {
                            StringBuilder row = new StringBuilder(columns.get(0).output(rowId));
                            for (int i = 1; i < columns.size(); i++) {
                                row.append(',').append(columns.get(i).output(rowId));
                            }
                            return row;
                        }
                ).toList();
    }

    private String removeData(String graph) {
        //Pattern data = Pattern.compile(", data:[^}]");
        String newGraph = graph.replaceAll("\\{id:[0-9]+, data:[^}]+", "");
        newGraph = newGraph.replaceAll("key[0-9]+", "key");
        newGraph = newGraph.replaceAll("color=\"#[F|C]+\"", "color");
        return newGraph;
    }
}
