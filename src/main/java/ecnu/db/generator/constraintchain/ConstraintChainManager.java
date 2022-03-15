package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.core.type.TypeReference;
import ecnu.db.LanguageManager;
import ecnu.db.generator.constraintchain.agg.ConstraintChainAggregateNode;
import ecnu.db.generator.constraintchain.join.ConstraintChainFkJoinNode;
import ecnu.db.schema.Column;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.schema.CannotFindSchemaException;
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
    private final ResourceBundle rb = LanguageManager.getInstance().getRb();
    private static final String WORKLOAD_DIR = "/workload";
    private ConstraintChainManager() {
    }

    public static ConstraintChainManager getInstance() {
        return INSTANCE;
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
        //传递约束链
        for (ConstraintChain constraintChain : constraintChains) {
            for (int i = 0; i < constraintChain.getNodes().size(); i++) {
                if (constraintChain.getNodes().get(i).getConstraintChainNodeType() == ConstraintChainNodeType.AGGREGATE) {
                    ConstraintChainAggregateNode aggregateNode = (ConstraintChainAggregateNode) constraintChain.getNodes().get(i);
                    if (aggregateNode.getGroupKey() != null) {
                        String fkCol = aggregateNode.getGroupKey().get(0);
                        String[] cols = fkCol.split("\\.");
                        try {
                            String remoteCol = TableManager.getInstance().getSchema(cols[0] + "." + cols[1]).getForeignKeys().get(fkCol);
                            aggregateNode.joinStatusIndex = involveFks.keySet().stream().toList().indexOf(remoteCol);
                        } catch (CannotFindSchemaException e) {
                            e.printStackTrace();
                        }
                    } else {
                        aggregateNode.joinStatusIndex = -1;
                    }
                }
            }
        }
        return involveFks;
    }

    /**
     * 输入约束链，返回涉及到所有状态 组织结构如下
     * pkCol1 ---- pkCol2 ----- pkCol3
     * status1 --- status2 ---- status3
     * status11--- status22---- status33
     * ......
     *
     * @param col2AllStatus 涉及到参照主键
     * @return 所有可能的状态组
     */
    public static List<List<boolean[]>> getAllDistinctStatus(List<List<boolean[]>> col2AllStatus) {
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
        return allDiffStatus;
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

    public void setResultDir(String resultDir) {
        this.resultDir = resultDir;
    }

    public Map<String, List<ConstraintChain>> loadConstrainChainResult(String resultDir) throws IOException {
        String path = resultDir + "/workload";
        File sqlDic = new File(path);
        File[] sqlArray = sqlDic.listFiles();
        assert sqlArray != null;
        Map<String, List<ConstraintChain>> result = new HashMap<>();
        for (File file : sqlArray) {
            File[] graphArray = file.listFiles();
            assert graphArray != null;
            for (File file1 : graphArray) {
                if (file1.getName().contains("json")) {
                    Map<String, List<ConstraintChain>> eachresult = CommonUtils.MAPPER.readValue(readFile(file1.getPath()), new TypeReference<>() {
                    });
                    result.putAll(eachresult);
                }
            }
        }
        return result;
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
                        node -> node.getConstraintChainNodeType() == ConstraintChainNodeType.FILTER ||
                                (node.getConstraintChainNodeType() == ConstraintChainNodeType.AGGREGATE &&
                                        ((ConstraintChainAggregateNode) node).getGroupKey() == null))) {
                    logger.info(rb.getString("RemoveConstraintChain1"), query2ConstraintChains.getKey(), constraintChain);
                    constraintChainIterator.remove();
                }
            }
        }
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
        if (allChains == null) {
            return;
        }
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
        logger.debug(rb.getString("ConstraintChainClassification"),
                haveFkConstrainChains.size(), onlyPkConstrainChains.size(), fkAndPkConstrainChains.size());
    }

    public void storeConstraintChain(Map<String, List<ConstraintChain>> query2constraintChains) throws IOException {
        File workLoadDic = new File(resultDir + WORKLOAD_DIR);
        if (!workLoadDic.exists()) {
            workLoadDic.mkdir();
        }
        for (Map.Entry<String, List<ConstraintChain>> entry : query2constraintChains.entrySet()) {
            String constraintChainsContent = CommonUtils.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(entry);
            File sqlDic = new File(resultDir + WORKLOAD_DIR + "/" + entry.getKey().split("\\.")[0]);
            if (!sqlDic.exists()) {
                sqlDic.mkdir();
            }
            CommonUtils.writeFile(resultDir + WORKLOAD_DIR + "/" + entry.getKey().split("\\.")[0] + "/" + entry.getKey() + ".json", constraintChainsContent);
        }
        for (Map.Entry<String, List<ConstraintChain>> stringListEntry : query2constraintChains.entrySet()) {
            String path = resultDir + "/workload" + "/" + stringListEntry.getKey().split("\\.")[0] + "/" + stringListEntry.getKey() + ".dot";
            File file = new File(resultDir + WORKLOAD_DIR + "/" + stringListEntry.getKey().split("\\.")[0]);
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
                    Calendar date = Calendar.getInstance();
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
                    String currentTime = format.format(date.getTime());
                    String newPath = resultDir + WORKLOAD_DIR + "/" + stringListEntry.getKey().split("\\.")[0] + "/" + currentTime + stringListEntry.getKey() + ".dot";
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

    private String removeData(String graph) {
        String newGraph = graph.replaceAll("\\{id:[0-9]+, data:[^}]+", "");
        newGraph = newGraph.replaceAll("key[0-9]+", "key");
        newGraph = newGraph.replaceAll("color=\"#[F|C]+\"", "color");
        return newGraph;
    }
}
