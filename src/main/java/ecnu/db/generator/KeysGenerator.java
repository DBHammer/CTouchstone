package ecnu.db.generator;

import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.ConstraintChainManager;
import ecnu.db.generator.fkgenerate.BoundFkGenerate;
import ecnu.db.generator.fkgenerate.FkGenerate;
import ecnu.db.generator.fkgenerate.RandomFkGenerate;
import ecnu.db.generator.joininfo.JoinStatus;
import ecnu.db.generator.joininfo.RuleTable;
import ecnu.db.generator.joininfo.RuleTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KeysGenerator {
    Logger logger = LoggerFactory.getLogger(KeysGenerator.class);

    // 约束链中的外键对应的主键的位置。 主键名 -> 一组主键的join info status中对应的位置
    private final SortedMap<String, List<Integer>> involveFks;

    /**
     * * 约束链中涉及到的所有状态 组织结构如下
     * * pkCol1 ---- pkCol2 ----- pkCol3
     * * status1 --- status2 ---- status3
     * * status11--- status22---- status33
     */
    private final List<List<boolean[]>> allStatus;

    // 主键的状态Hash对应的size。 一组  主键状态hash -> 主键状态的大小
    private final List<Map<Integer, Long>> statusHash2Size;

    public KeysGenerator(List<ConstraintChain> haveFkConstrainChains) {
        // 统计所有的联合状态
        involveFks = ConstraintChainManager.getInvolvedFks(haveFkConstrainChains);
        // 获得每个列的涉及到的status
        // 表 -> 表内所有的不同join status -> join status
        List<List<boolean[]>> col2AllStatus = involveFks.entrySet().stream()
                .map(col2Location -> RuleTableManager.getInstance().getAllStatusRule(col2Location.getKey(), col2Location.getValue()))
                .toList();
        allStatus = ConstraintChainManager.getAllDistinctStatus(col2AllStatus);
        if (!allStatus.isEmpty()) {
            logger.debug("共计{}种状态，参照列为{}", allStatus.size(), involveFks);
            for (List<boolean[]> booleans : allStatus) {
                logger.debug(booleans.stream().map(Arrays::toString).collect(Collectors.joining("\t\t")));
            }
        }
        statusHash2Size = new ArrayList<>(allStatus.size());
        for (Map.Entry<String, List<Integer>> table2Location : involveFks.entrySet()) {
            Map<Integer, Long> sizes = RuleTableManager.getInstance().getStatueSize(table2Location.getKey(), table2Location.getValue());
            statusHash2Size.add(sizes);
        }
    }

    public long[] generateFkSolution(List<ConstraintChain> haveFkConstrainChains, SortedMap<JoinStatus, Long> filterHistogram,
                                     boolean[][] filterStatus, int range) {
        // 为每个filter status 匹配对应的pk status 生成所有的solution方案
        List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus = getAllVarStatus(filterHistogram, allStatus);
        int[] filterSize = computeFilterSizeForEachFilter(filterStatus);
        long[] result = ConstructCpModel.computeWithCpModel(haveFkConstrainChains, filterHistogram, statusHash2Size, filterStatus2PkStatus, filterSize, range);
        logger.info("填充方案");
        if (result.length > filterStatus2PkStatus.size()) {
            int step = result.length / (filterStatus2PkStatus.get(0).getValue().size() + 1);
            int num = filterStatus2PkStatus.get(0).getValue().size();
            IntStream.range(0, step).filter(i -> result[i] > 0).forEach(i -> {
                        long[] cardinalityResult = new long[num];
                        for (int j = 0; j < num; j++) {
                            cardinalityResult[j] = result[step * (j + 1) + i];
                        }
                        logger.info("{}->{} size:{} cardinality:{}", filterStatus2PkStatus.get(i).getKey(),
                                filterStatus2PkStatus.get(i).getValue().stream().map(Arrays::toString).toList(), result[i], Arrays.toString(cardinalityResult));
                    }
            );
        } else {
            IntStream.range(0, result.length).filter(i -> result[i] > 0).forEach(i ->
                    logger.info("{}->{} size:{}", filterStatus2PkStatus.get(i).getKey(),
                            filterStatus2PkStatus.get(i).getValue().stream().map(Arrays::toString).toList(), result[i]));
        }
        return result;
    }

    private int[] computeFilterSizeForEachFilter(boolean[][] filterStatus) {
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

    private Map<JoinStatus, Long> countStatus(boolean[][] result) {
        int statusNum = result.length;
        if (statusNum == 0) {
            return new HashMap<>();
        }
        int range = result[0].length;
        if (range == 0) {
            return new HashMap<>();
        }
        return IntStream.range(0, range).parallel().mapToObj(rowId -> {
            boolean[] ret = new boolean[statusNum];
            for (int i = 0; i < result.length; i++) {
                ret[i] = result[i][rowId];
            }
            return new JoinStatus(ret);
        }).collect(Collectors.groupingByConcurrent(Function.identity(), Collectors.counting()));
    }

    public List<List<Map.Entry<boolean[], Long>>> populateFkStatus(List<ConstraintChain> haveFkConstrainChains,
                                                                   boolean[][] filterStatus, int range) {
        // 统计status的分布直方图
        SortedMap<JoinStatus, Long> filterHistogram = new TreeMap<>(countStatus(filterStatus));
        long[] result = generateFkSolution(haveFkConstrainChains, filterHistogram, filterStatus, range);
        logger.info("statusHash2Size" + statusHash2Size);
        boolean hasCardinalityConstraint = result.length > filterHistogram.size() * allStatus.size();
        List<List<FkGenerate>> cardinalityRangeForEachFk = new ArrayList<>();
        List<String> refCols = new ArrayList<>(involveFks.keySet());
        if (hasCardinalityConstraint) {
            int step = filterHistogram.size() * allStatus.size();
            int fkNum = result.length / step - 1;
            for (int i = 1; i <= fkNum; i++) {
                int resultRangeStart = step * i;
                int fkIndex = i - 1;
                List<FkGenerate> cardinalityRangeForFk = new ArrayList<>();
                RuleTable ruleTable = RuleTableManager.getInstance().getRuleTable(refCols.get(fkIndex));
                for (int rangeIndex = resultRangeStart; rangeIndex < resultRangeStart + step; rangeIndex++) {
                    if (result[rangeIndex] == 0) {
                        if (refCols.get(fkIndex).contains("public.orders.o_orderkey")) {
                            cardinalityRangeForFk.add(new RandomFkGenerate(-1L, -1L, 0));
                        } else {
                            cardinalityRangeForFk.add(new RandomFkGenerate(-1L, -1L, 0));
                        }
                    } else {
                        int ruleHash = Arrays.hashCode(allStatus.get(rangeIndex % allStatus.size()).get(fkIndex));
                        long currentCounter = ruleTable.getRuleCounter().get(ruleHash);
                        long nextCounter = currentCounter + result[rangeIndex];
                        if (refCols.get(fkIndex).contains("public.orders.o_orderkey")) {
                            cardinalityRangeForFk.add(new BoundFkGenerate(currentCounter, nextCounter, result[rangeIndex - resultRangeStart]));
                        } else {
                            cardinalityRangeForFk.add(new RandomFkGenerate(currentCounter, nextCounter, result[rangeIndex - resultRangeStart]));
                        }
                        ruleTable.getRuleCounter().put(ruleHash, nextCounter);
                    }
                }
                cardinalityRangeForEachFk.add(cardinalityRangeForFk);
            }
        }
//        System.out.println(cardinalityRangeForEachFk);

        List<List<Map.Entry<boolean[], Long>>> fkStatus2PkIndex = new ArrayList<>(range);
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
            List<boolean[]> fkStatus = allStatus.get(index % allStatus.size());
            List<Map.Entry<boolean[], Long>> rowFkStatus2Index = new ArrayList<>();
            for (int fkIndex = 0; fkIndex < fkStatus.size(); fkIndex++) {
                if (cardinalityRangeForEachFk.isEmpty()) {
                    rowFkStatus2Index.add(new AbstractMap.SimpleEntry<>(fkStatus.get(fkIndex), -1L));
                } else {
                    FkGenerate pkRange = cardinalityRangeForEachFk.get(fkIndex).get(index);
                    if (!pkRange.isValid()) {
                        rowFkStatus2Index.add(new AbstractMap.SimpleEntry<>(fkStatus.get(fkIndex), -1L));
                    } else {
                        rowFkStatus2Index.add(new AbstractMap.SimpleEntry<>(fkStatus.get(fkIndex), pkRange.getValue()));
                    }
                }
            }
            fkStatus2PkIndex.add(rowFkStatus2Index);
        }
        return fkStatus2PkIndex;
    }

    public List<long[]> populateFks(List<List<Map.Entry<boolean[], Long>>> fkStatus) {
        List<RuleTable> ruleTables = new ArrayList<>();
        for (String refTable : involveFks.keySet()) {
            ruleTables.add(RuleTableManager.getInstance().getRuleTable(refTable));
        }
        List<long[]> fksList = new ArrayList<>();
        for (var status2Index : fkStatus) {
            long[] fks = new long[status2Index.size()];
            int i = 0;
            for (int statusTableIndex = 0; statusTableIndex < status2Index.size(); statusTableIndex++) {
                fks[i++] = ruleTables.get(statusTableIndex).getKey(status2Index.get(statusTableIndex).getKey(), status2Index.get(statusTableIndex).getValue());
            }
            fksList.add(fks);
        }
        return fksList;
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

    public Map<JoinStatus, Long> printPkStatusMatrix(boolean[][] pkStatus) {
        if (pkStatus.length != 0) {
            logger.debug("PK Info");
            Map<JoinStatus, Long> pkHistogram = countStatus(pkStatus);
            for (Map.Entry<JoinStatus, Long> s : pkHistogram.entrySet()) {
                logger.debug("status:{} size:{}", s.getKey(), s.getValue());
            }
            return pkHistogram;
        } else {
            return new HashMap<>();
        }
    }


}
