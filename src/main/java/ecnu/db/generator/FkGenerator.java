package ecnu.db.generator;

import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.ConstraintChainNode;
import ecnu.db.generator.constraintchain.agg.ConstraintChainAggregateNode;
import ecnu.db.generator.constraintchain.join.ConstraintChainFkJoinNode;
import ecnu.db.generator.joininfo.JoinStatus;
import ecnu.db.generator.joininfo.RuleTable;
import ecnu.db.generator.joininfo.RuleTableManager;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.CommonUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FkGenerator {
    private final long tableSize;

    private final Map<Integer, Long> distinctFkIndex2Cardinality = new HashMap<>();
    private final List<Integer> involvedChainIndexes = new ArrayList<>();
    private final List<List<ConstraintChainNode>> chainNodesList = new LinkedList<>();

    private final JoinStatus[][] jointPkStatus;

    private final JoinStatus[] outputStatusForEachPk;

    private final RuleTable[] ruleTables;


    FkGenerator(List<ConstraintChain> fkConstrainChains, List<String> fkGroup, long tableSize) {
        this.tableSize = tableSize;
        for (ConstraintChain fkConstrainChain : fkConstrainChains) {
            var involvedNodes = fkConstrainChain.getInvolvedNodes(fkGroup);
            if (!involvedNodes.isEmpty()) {
                involvedChainIndexes.add(fkConstrainChain.getChainIndex());
                chainNodesList.add(involvedNodes);
            }
        }
        // 获得每个fk列的status, 标记外键对应的index
        LinkedHashMap<String, List<Integer>> involvedFkCol2JoinTags = generateFkIndex(fkGroup, chainNodesList);
        // 对于每一个外键组，确定主键状态
        JoinStatus[][] pkCol2AllStatus = new JoinStatus[involvedFkCol2JoinTags.size()][];
        int i = 0;
        ruleTables = new RuleTable[involvedFkCol2JoinTags.size()];
        for (Map.Entry<String, List<Integer>> involvedFk2JoinTag : involvedFkCol2JoinTags.entrySet()) {
            String pkCol = TableManager.getInstance().getRefKey(involvedFk2JoinTag.getKey());
            ruleTables[i] = RuleTableManager.getInstance().getRuleTable(pkCol);
            boolean withNull = ColumnManager.getInstance().getNullPercentage(involvedFk2JoinTag.getKey()).compareTo(BigDecimal.ZERO) > 0;
            pkCol2AllStatus[i] = ruleTables[i].mergeRules(involvedFk2JoinTag.getValue(), withNull);
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
            RuleTable ruleTable = ruleTables[distinctFKIndex];
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
        if (involvedChainIndexes.isEmpty()) {
            return new long[0][0];
        }
        SortedMap<JoinStatus, Long> statusHistogram = generateStatusHistogram(statusVectorOfEachRow, involvedChainIndexes);
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
        Integer[] filterStatusPkPopulatedIndex = new Integer[statusHistogram.size()];
        Arrays.fill(filterStatusPkPopulatedIndex, 0);
        long[][] fkColValues = new long[jointPkStatus[0].length][statusVectorOfEachRow.length];
        for (int rowId = 0; rowId < statusVectorOfEachRow.length; rowId++) {
            JoinStatus involvedStatus = chooseCorrespondingStatus(statusVectorOfEachRow[rowId], involvedChainIndexes);
            int filterIndex = status2Index.get(involvedStatus);
            int pkStatusIndex = filterStatusPkPopulatedIndex[filterIndex];
            while (populateSolution[filterIndex][pkStatusIndex] == 0) {
                pkStatusIndex++;
                filterStatusPkPopulatedIndex[filterIndex] = pkStatusIndex;
            }
            JoinStatus[] populateStatus = jointPkStatus[pkStatusIndex];
            for (int fkColIndex = 0; fkColIndex < populateStatus.length; fkColIndex++) {
                long index = -1;
                if (fkIndex2Range.containsKey(fkColIndex)) {
                    long currentCardinality = populateSolution[filterIndex][pkStatusIndex];
                    FkRange fkRange = fkIndex2Range.get(fkColIndex)[filterIndex][pkStatusIndex];
                    boolean withRemainingRange = fkRange.range > 0;
                    boolean moveToNextRange = ThreadLocalRandom.current().nextDouble() * currentCardinality <= fkRange.range;
                    if (withRemainingRange && moveToNextRange) {
                        fkRange.range--;
                    }
                    index = fkRange.start + fkRange.range;
                }
                fkColValues[fkColIndex][rowId] = ruleTables[fkColIndex].getKey(populateStatus[fkColIndex], index);
            }
            boolean[] outputStatus = outputStatusForEachPk[pkStatusIndex].status();
            for (int j = 0; j < statusVectorOfEachRow[rowId].length; j++) {
                statusVectorOfEachRow[rowId][j] &= outputStatus[j];
            }
            populateSolution[filterIndex][pkStatusIndex]--;
        }
        return fkColValues;
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


    public static SortedMap<JoinStatus, Long> generateStatusHistogram(boolean[][] statusVectorOfEachRow, List<Integer> involvedChainIndexes) {
        if (involvedChainIndexes.isEmpty()) {
            return new TreeMap<>();
        }
        var map = IntStream.range(0, statusVectorOfEachRow.length).parallel()
                .mapToObj(i -> chooseCorrespondingStatus(statusVectorOfEachRow[i], involvedChainIndexes))
                .collect(Collectors.groupingByConcurrent(Function.identity(), Collectors.counting()));
        return new TreeMap<>(map);
    }

    public static JoinStatus chooseCorrespondingStatus(boolean[] originStatus, List<Integer> involvedChainIndexes) {
        boolean[] ret = new boolean[involvedChainIndexes.size()];
        int i = 0;
        for (Integer involvedChainIndex : involvedChainIndexes) {
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
            for (int currentChainIndex = 0; currentChainIndex < involvedChainIndexes.size(); currentChainIndex++) {
                List<ConstraintChainFkJoinNode> chainFkJoinNodes = chainNodesList.get(currentChainIndex).stream()
                        .filter(ConstraintChainFkJoinNode.class::isInstance).map(ConstraintChainFkJoinNode.class::cast).toList();
                JoinStatus[] pkStatusOfRow = jointPkStatus[pkStatusIndex];
                boolean status = true;
                for (ConstraintChainFkJoinNode chainFkJoinNode : chainFkJoinNodes) {
                    status &= pkStatusOfRow[chainFkJoinNode.joinStatusIndex].status()[chainFkJoinNode.joinStatusLocation];
                }
                int chainIndex = involvedChainIndexes.get(currentChainIndex);
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
    private LinkedHashMap<String, List<Integer>> generateFkIndex(List<String> fkCols, List<List<ConstraintChainNode>> chainNodesList) {
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
        return involvedFkCol2JoinTags;
    }
}
