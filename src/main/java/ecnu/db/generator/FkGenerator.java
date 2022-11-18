package ecnu.db.generator;

import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.ConstraintChainNode;
import ecnu.db.generator.constraintchain.agg.ConstraintChainAggregateNode;
import ecnu.db.generator.constraintchain.join.ConstraintChainFkJoinNode;
import ecnu.db.generator.joininfo.JoinStatus;
import ecnu.db.generator.joininfo.MergedRuleTable;
import ecnu.db.generator.joininfo.RuleTableManager;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FkGenerator {
    private final long tableSize;

    private final Map<Integer, Long> distinctFkIndex2Cardinality = new HashMap<>();
    private final int[] involvedChainIndexes;
    private final List<List<ConstraintChainNode>> chainNodesList = new LinkedList<>();

    private final JoinStatus[][] jointPkStatus;

    private final JoinStatus[] outputStatusForEachPk;

    private final MergedRuleTable[] ruleTables;


    FkGenerator(List<ConstraintChain> fkConstrainChains, List<String> fkGroup, long tableSize) {
        this.tableSize = tableSize;
        List<Integer> involvedChainIndexesList = new ArrayList<>();
        for (ConstraintChain fkConstrainChain : fkConstrainChains) {
            var involvedNodes = fkConstrainChain.getInvolvedNodes(fkGroup);
            if (!involvedNodes.isEmpty()) {
                involvedChainIndexesList.add(fkConstrainChain.getChainIndex());
                chainNodesList.add(involvedNodes);
            }
        }
        involvedChainIndexes = involvedChainIndexesList.stream().mapToInt(Integer::intValue).toArray();
        // 获得每个fk列的status, 标记外键对应的index
        LinkedHashMap<String, int[]> involvedFkCol2JoinTags = generateFkIndex(fkGroup, chainNodesList);
        // 对于每一个外键组，确定主键状态
        JoinStatus[][] pkCol2AllStatus = new JoinStatus[involvedFkCol2JoinTags.size()][];
        int i = 0;
        ruleTables = new MergedRuleTable[involvedFkCol2JoinTags.size()];
        for (Map.Entry<String, int[]> involvedFk2JoinTag : involvedFkCol2JoinTags.entrySet()) {
            String pkCol = TableManager.getInstance().getRefKey(involvedFk2JoinTag.getKey());
            ruleTables[i] = RuleTableManager.getInstance().getRuleTable(pkCol, involvedFk2JoinTag.getValue());
            boolean withNull = ColumnManager.getInstance().getNullPercentage(involvedFk2JoinTag.getKey()).compareTo(BigDecimal.ZERO) > 0;
            pkCol2AllStatus[i] = ruleTables[i].getPkStatus(withNull);
            i++;
        }
        // 计算联合status
        jointPkStatus = getPkJointStatus(pkCol2AllStatus);
        chainNodesList.stream().flatMap(Collection::stream)
                .filter(ConstraintChainFkJoinNode.class::isInstance)
                .map(ConstraintChainFkJoinNode.class::cast).forEach(node -> node.initJoinResultStatus(jointPkStatus));
        // 计算输出的status
        outputStatusForEachPk = computeOutputStatus(fkConstrainChains.size());
        for (Map.Entry<Integer, Long> fkIndex2Cardinality : distinctFkIndex2Cardinality.entrySet()) {
            long fkColCardinality = ColumnManager.getInstance().getNdv(fkGroup.get(fkIndex2Cardinality.getKey()));
            fkIndex2Cardinality.setValue(fkColCardinality);
        }
    }


    private void applySharePkConstraint(ConstructCpModel cpModel, int range) {
        BigDecimal batchPercentage = BigDecimal.valueOf(range).divide(BigDecimal.valueOf(tableSize), CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION);
        for (var distinctFKIndex : distinctFkIndex2Cardinality.keySet()) {
            Map<JoinStatus, ArrayList<Integer>> status2PkIndex = new HashMap<>();
            for (int i = 0; i < jointPkStatus.length; i++) {
                JoinStatus status = jointPkStatus[i][distinctFKIndex];
                status2PkIndex.computeIfAbsent(status, v -> new ArrayList<>());
                status2PkIndex.get(status).add(i);
            }
            Map<ArrayList<Integer>, Long> pkIndex2Size = new HashMap<>();
            MergedRuleTable ruleTable = ruleTables[distinctFKIndex];
            for (Map.Entry<JoinStatus, ArrayList<Integer>> statusArrayListEntry : status2PkIndex.entrySet()) {
                long pkStatusSize = ruleTable.getStatusSize(statusArrayListEntry.getKey());
                BigDecimal bBatchPkStatusSize = BigDecimal.valueOf(pkStatusSize).multiply(batchPercentage);
                long batchPkStatusSize = bBatchPkStatusSize.setScale(0, RoundingMode.UP).longValue();
                pkIndex2Size.put(statusArrayListEntry.getValue(), batchPkStatusSize);
            }
            cpModel.applyFKShareConstraint(distinctFKIndex, pkIndex2Size);
        }
    }

    private ConstructCpModel constructConstraintProblem(Map<JoinStatus, Long> statusHistogram, int range) {
        ConstructCpModel constructCpModel = new ConstructCpModel();
        constructCpModel.initModel(statusHistogram, jointPkStatus.length, range);
        for (var distinctFkCol2Cardinality : distinctFkIndex2Cardinality.entrySet()) {
            constructCpModel.initDistinctModel(distinctFkCol2Cardinality.getKey(), distinctFkCol2Cardinality.getValue(), tableSize);
        }
        for (int chainIndex = 0; chainIndex < chainNodesList.size(); chainIndex++) {
            boolean[][] canBeInput = new boolean[statusHistogram.size()][jointPkStatus.length];
            int i = 0;
            long filterSize = 0;
            for (var status2Size : statusHistogram.entrySet()) {
                boolean filterStatus = status2Size.getKey().status()[chainIndex];
                filterSize += filterStatus ? status2Size.getValue() : 0;
                Arrays.fill(canBeInput[i++], filterStatus);
            }
            long unFilterSize = range - filterSize;
            for (ConstraintChainNode constraintChainNode : chainNodesList.get(chainIndex)) {
                if (constraintChainNode instanceof ConstraintChainFkJoinNode fkJoinNode) {
                    fkJoinNode.addJoinDistinctConstraint(constructCpModel, filterSize, canBeInput);
                    filterSize = fkJoinNode.addJoinCardinalityConstraint(constructCpModel, filterSize, unFilterSize, canBeInput);
                    unFilterSize = -1;
                } else if (constraintChainNode instanceof ConstraintChainAggregateNode aggregateNode) {
                    aggregateNode.addJoinDistinctConstraint(constructCpModel, filterSize, canBeInput);
                }
            }
        }
        applySharePkConstraint(constructCpModel, range);
        return constructCpModel;
    }


    public long[][] generateFK(boolean[][] statusVectorOfEachRow) {
        // 统计每种状态的数据量
        if (involvedChainIndexes.length == 0) {
            return new long[0][0];
        }
        JoinStatus[] involvedStatuses = Arrays.stream(statusVectorOfEachRow).parallel()
                .map(arr -> FkGenerator.chooseCorrespondingStatus(arr, involvedChainIndexes)).toArray(JoinStatus[]::new);
        SortedMap<JoinStatus, Long> statusHistogram = generateStatusHistogram(involvedStatuses);
        int range = statusVectorOfEachRow.length;
        ConstructCpModel cpModel = constructConstraintProblem(statusHistogram, range);
        long[][] populateSolution = cpModel.solve();
        Map<Integer, FkRange[][]> fkIndex2Range = new HashMap<>();
        for (Integer fkIndex : distinctFkIndex2Cardinality.keySet()) {
            fkIndex2Range.put(fkIndex, cpModel.getDistinctResult(fkIndex));
        }
        HashMap<JoinStatus, Integer> status2Index = new HashMap<>();
        int i = 0;
        for (JoinStatus joinStatus : statusHistogram.keySet()) {
            status2Index.put(joinStatus, i++);
        }
        int[] filterIndexes = Arrays.stream(involvedStatuses).parallel().mapToInt(status2Index::get).toArray();
        int[] pkStatuses = new int[statusVectorOfEachRow.length];
        long[] remainSize = new long[statusVectorOfEachRow.length];
        int[] filterStatusPkPopulatedIndex = new int[statusHistogram.size()];
        Arrays.fill(filterStatusPkPopulatedIndex, 0);
        for (int rowId = 0; rowId < statusVectorOfEachRow.length; rowId++) {
            int filterIndex = filterIndexes[rowId];
            int pkStatusIndex = filterStatusPkPopulatedIndex[filterIndex];
            while (populateSolution[filterIndex][pkStatusIndex] == 0) {
                pkStatusIndex++;
                filterStatusPkPopulatedIndex[filterIndex] = pkStatusIndex;
            }
            pkStatuses[rowId] = pkStatusIndex;
            remainSize[rowId] = populateSolution[filterIndex][pkStatusIndex];
            populateSolution[filterIndex][pkStatusIndex]--;
        }
        long[][] fkColValues = new long[jointPkStatus[0].length][statusVectorOfEachRow.length];
        ExecutorService executorPool = Executors.newFixedThreadPool(jointPkStatus[0].length);
        for (int fkColIndex = 0; fkColIndex < jointPkStatus[0].length; fkColIndex++) {
            int finalFkColIndex = fkColIndex;
            if (fkIndex2Range.containsKey(fkColIndex)) {
                executorPool.submit(() -> {
                    FkRange[][] fkRangeForFk = fkIndex2Range.get(finalFkColIndex);
                    for (int rowId = 0; rowId < statusVectorOfEachRow.length; rowId++) {
                        int pkStatusIndex = pkStatuses[rowId];
                        JoinStatus[] populateStatus = jointPkStatus[pkStatusIndex];
                        int filterIndex = filterIndexes[rowId];
                        FkRange fkRange = fkRangeForFk[filterIndex][pkStatusIndex];
                        long index = fkRange.start + fkRange.range - 1;
                        advanceFkRange(fkRange, remainSize[rowId]);
                        fkColValues[finalFkColIndex][rowId] = ruleTables[finalFkColIndex].getKey(populateStatus[finalFkColIndex], index);
                    }
                });
            } else {
                executorPool.submit(() -> {
                            for (int rowId = 0; rowId < statusVectorOfEachRow.length; rowId++) {
                                int pkStatusIndex = pkStatuses[rowId];
                                JoinStatus[] populateStatus = jointPkStatus[pkStatusIndex];
                                fkColValues[finalFkColIndex][rowId] = ruleTables[finalFkColIndex].getKey(populateStatus[finalFkColIndex], 0);
                            }
                        }
                );
            }
        }
        executorPool.shutdown();
        try {
            executorPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // 计算每一行数据的输出状态
        int chainSize = statusVectorOfEachRow[0].length;
        IntStream.range(0, statusVectorOfEachRow.length).parallel().forEach(rowId -> {
            boolean[] outputStatus = outputStatusForEachPk[pkStatuses[rowId]].status();
            for (int fkColIndex = 0; fkColIndex < chainSize; fkColIndex++) {
                statusVectorOfEachRow[rowId][fkColIndex] &= outputStatus[fkColIndex];
            }
        });
        return fkColValues;
    }

    private void advanceFkRange(FkRange fkRange, long currentCardinality) {
        boolean withRemainingRange = fkRange.range > 1;
        if (withRemainingRange) {
            boolean moveToNextRange = ThreadLocalRandom.current().nextDouble() * currentCardinality <= fkRange.range;
            if (moveToNextRange) {
                fkRange.range--;
            }
        }
    }

    /**
     * 输入约束链，返回涉及到所有状态 组织结构如下
     * pkCol1 ---- pkCol2 ----- pkCol3
     * status1 --- status2 ---- status3
     * status11--- status22---- status33
     * ......
     *
     * @param pkCol2AllStatus 涉及到参照主键，组织为主键列 -> status（boolean[]）
     * @return 所有可能的状态组 组织为 -> joint status -> 各列主键
     */
    private JoinStatus[][] getPkJointStatus(JoinStatus[][] pkCol2AllStatus) {
        int allStatusSize = 1;
        for (JoinStatus[] pkStatus : pkCol2AllStatus) {
            allStatusSize *= pkStatus.length;
        }
        int[] loopForEachPk = new int[pkCol2AllStatus.length];
        int currentSize = 1;
        for (int i = 0; i < pkCol2AllStatus.length; i++) {
            currentSize = pkCol2AllStatus[i].length * currentSize;
            loopForEachPk[i] = allStatusSize / currentSize;
        }
        JoinStatus[][] allDiffStatus = new JoinStatus[allStatusSize][];
        for (int index = 0; index < allStatusSize; index++) {
            JoinStatus[] result = new JoinStatus[pkCol2AllStatus.length];
            for (int colIndex = 0; colIndex < pkCol2AllStatus.length; colIndex++) {
                JoinStatus[] pkStatus = pkCol2AllStatus[colIndex];
                result[colIndex] = pkStatus[index / loopForEachPk[colIndex] % pkStatus.length];
            }
            allDiffStatus[index] = result;
        }
        return allDiffStatus;
    }


    public static SortedMap<JoinStatus, Long> generateStatusHistogram(JoinStatus[] involvedStatuses) {
        Map<JoinStatus, Long> histogram = Arrays.stream(involvedStatuses).parallel()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        return new TreeMap<>(histogram);
    }

    public static JoinStatus chooseCorrespondingStatus(boolean[] originStatus, int[] involvedChainIndexes) {
        boolean[] ret = new boolean[involvedChainIndexes.length];
        int i = 0;
        for (int involvedChainIndex : involvedChainIndexes) {
            ret[i++] = originStatus[involvedChainIndex];
        }
        return new JoinStatus(ret);
    }

    private JoinStatus[] computeOutputStatus(int allChainSize) {
        JoinStatus[] outputStatus = new JoinStatus[jointPkStatus.length];
        for (int j = 0; j < outputStatus.length; j++) {
            boolean[] status = new boolean[allChainSize];
            Arrays.fill(status, true);
            outputStatus[j] = new JoinStatus(status);
        }
        for (int pkStatusIndex = 0; pkStatusIndex < outputStatus.length; pkStatusIndex++) {
            for (int currentChainIndex = 0; currentChainIndex < involvedChainIndexes.length; currentChainIndex++) {
                List<ConstraintChainFkJoinNode> chainFkJoinNodes = chainNodesList.get(currentChainIndex).stream()
                        .filter(ConstraintChainFkJoinNode.class::isInstance).map(ConstraintChainFkJoinNode.class::cast).toList();
                JoinStatus[] pkStatusOfRow = jointPkStatus[pkStatusIndex];
                boolean status = true;
                for (ConstraintChainFkJoinNode chainFkJoinNode : chainFkJoinNodes) {
                    status &= pkStatusOfRow[chainFkJoinNode.joinStatusIndex].status()[chainFkJoinNode.joinStatusLocation];
                }
                int chainIndex = involvedChainIndexes[currentChainIndex];
                outputStatus[pkStatusIndex].status()[chainIndex] = status;
            }
        }
        return outputStatus;
    }

    /**
     * 返回所有约束链的参照表和参照的Tag
     *
     * @param chainNodesList 需要分析的约束链
     */
    private LinkedHashMap<String, int[]> generateFkIndex(List<String> fkCols, List<List<ConstraintChainNode>> chainNodesList) {
        // 找到涉及到的参照表和参照的tag
        LinkedHashMap<String, List<Integer>> involvedFkCol2JoinTags = new LinkedHashMap<>();
        for (String fkCol : fkCols) {
            involvedFkCol2JoinTags.put(fkCol, new ArrayList<>());
        }
        List<ConstraintChainFkJoinNode> involvedFkNodes = chainNodesList.stream().flatMap(Collection::stream)
                .filter(ConstraintChainFkJoinNode.class::isInstance).map(ConstraintChainFkJoinNode.class::cast).toList();
        for (ConstraintChainFkJoinNode fkNode : involvedFkNodes) {
            involvedFkCol2JoinTags.get(fkNode.getLocalCols()).add(fkNode.getPkTag());
            fkNode.joinStatusIndex = fkCols.indexOf(fkNode.getLocalCols());
            if (fkNode.getPkDistinctProbability() != null) {
                distinctFkIndex2Cardinality.put(fkNode.joinStatusIndex, 0L);
            }
        }
        //对所有的位置进行排序
        involvedFkCol2JoinTags.values().forEach(Collections::sort);
        //标记约束链对应的status的位置
        for (ConstraintChainFkJoinNode fkJoinNode : involvedFkNodes) {
            fkJoinNode.joinStatusLocation = involvedFkCol2JoinTags.get(fkJoinNode.getLocalCols()).indexOf(fkJoinNode.getPkTag());
        }
        // 绑定Aggregation到相关的Fk上
        List<ConstraintChainAggregateNode> involvedAggNodes = chainNodesList.stream().flatMap(Collection::stream)
                .filter(ConstraintChainAggregateNode.class::isInstance).map(ConstraintChainAggregateNode.class::cast).toList();
        for (ConstraintChainAggregateNode aggregateNode : involvedAggNodes) {
            String groupKey = aggregateNode.getGroupKey().get(0);
            var fkNode = involvedFkNodes.stream().filter(node -> node.getLocalCols().equals(groupKey)).findAny();
            if (fkNode.isPresent()) {
                aggregateNode.joinStatusIndex = fkNode.get().joinStatusIndex;
                distinctFkIndex2Cardinality.put(aggregateNode.joinStatusIndex, 0L);
            } else {
                throw new UnsupportedOperationException("无法下推的agg约束");
            }
        }
        LinkedHashMap<String, int[]> ret = new LinkedHashMap<>();
        for (Map.Entry<String, List<Integer>> entry : involvedFkCol2JoinTags.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().stream().mapToInt(Integer::intValue).toArray());
        }
        return ret;
    }
}
