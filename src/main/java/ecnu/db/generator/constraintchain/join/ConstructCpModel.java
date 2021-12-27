package ecnu.db.generator.constraintchain.join;

import com.google.ortools.sat.*;
import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.joininfo.JoinStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.IntStream;

public class ConstructCpModel {
    static Logger logger = LoggerFactory.getLogger(ConstructCpModel.class);
    public static HashSet<Integer> validCardinality = new HashSet<>();

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
    public static long[] computeWithCpModel(List<ConstraintChain> haveFkConstrainChains,
                                            SortedMap<JoinStatus, Long> filterHistogram,
                                            List<Map<Integer, Long>> statusSize,
                                            List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus,
                                            boolean[][] filterStatus, int range) {
        validCardinality.clear();
        logger.info("filterHistogram: {}", filterHistogram);
        CpModel model = new CpModel();
        IntVar[] vars;
        if (haveFkConstrainChains.stream().anyMatch(ConstraintChain::hasCardinalityConstraint)) {
            vars = initDistinctModel(filterHistogram, filterStatus2PkStatus, model, range);
        } else {
            vars = initModel(filterHistogram, filterStatus2PkStatus, model, range);
        }
        int chainIndex = 0;
        int[] filterSize = computeFilterSizeForEachFilter(filterStatus);
        for (ConstraintChain haveFkConstrainChain : haveFkConstrainChains) {
            haveFkConstrainChain.addConstraint2Model(model, vars, chainIndex, filterSize[chainIndex], range - filterSize[chainIndex], statusSize, filterStatus2PkStatus);
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

    private static IntVar[] initDistinctModel(SortedMap<JoinStatus, Long> filterHistogram,
                                              List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus,
                                              CpModel model, int range) {
        int anchor = filterStatus2PkStatus.size();
        int varNum = (filterStatus2PkStatus.get(0).getValue().size() + 1) * anchor;
        IntVar[] vars = new IntVar[varNum];
        logger.debug("create {} vars", varNum);
        for (int i = 0; i < anchor; i++) {
            vars[i] = model.newIntVar(0, range, String.valueOf(i));
            for (int j = 1; j <= filterStatus2PkStatus.get(0).getValue().size(); j++) {
                vars[i + j * anchor] = model.newIntVar(0, range, String.valueOf(i + j * anchor));
                model.addLessOrEqual(vars[i + j * anchor], vars[i]);
            }
        }
        int i = 0;
        int allPkSize = filterStatus2PkStatus.size() / filterHistogram.size();
        for (Map.Entry<JoinStatus, Long> filter2Status : filterHistogram.entrySet()) {
            model.addEquality(LinearExpr.sum(Arrays.copyOfRange(vars, i, i + allPkSize)), filter2Status.getValue());
            i += allPkSize;
        }
        return vars;
    }

    private static IntVar[] initModel(SortedMap<JoinStatus, Long> filterHistogram,
                                      List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus,
                                      CpModel model, int range) {
        int varNum = filterStatus2PkStatus.size();
        IntVar[] vars = new IntVar[varNum];
        logger.debug("create {} vars", varNum);
        for (int i = 0; i < varNum; i++) {
            vars[i] = model.newIntVar(0, range, String.valueOf(i));
        }
        int i = 0;
        int allPkSize = filterStatus2PkStatus.size() / filterHistogram.size();
        for (Map.Entry<JoinStatus, Long> filter2Status : filterHistogram.entrySet()) {
            model.addEquality(LinearExpr.sum(Arrays.copyOfRange(vars, i, i + allPkSize)), filter2Status.getValue());
            i += allPkSize;
        }
        return vars;
    }

    private static int[] computeFilterSizeForEachFilter(boolean[][] filterStatus) {
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

    public static void addIndexJoinFkConstraint(CpModel model, IntVar[] vars, int indexJoinSize,
                                                ConstraintChainFkJoinNode fkJoinNode, boolean[] canBeInput,
                                                List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus) {
        // 获取join对应的位置
        int joinStatusIndex = fkJoinNode.joinStatusIndex;
        int joinStatusLocation = fkJoinNode.joinStatusLocation;
        // 找到有效的CpModel变量
        List<IntVar> indexJoinVars = new ArrayList<>();
        boolean status = !fkJoinNode.getType().isAnti();
        IntStream.range(0, filterStatus2PkStatus.size()).filter(i -> !canBeInput[i]).forEach(i -> {
            if (filterStatus2PkStatus.get(i).getValue().get(joinStatusIndex)[joinStatusLocation] == status) {
                indexJoinVars.add(vars[i]);
            }
        });
        logger.info("输出的数据量为:{}, 为第{}个表的第{}个状态", indexJoinSize, joinStatusIndex, joinStatusLocation);
        model.addEquality(LinearExpr.sum(indexJoinVars.toArray(IntVar[]::new)), indexJoinSize);
    }

    public static void addAggCardinalityConstraint(int joinStatusIndex,
                                                   CpModel model, IntVar[] vars, int pkCardinalitySize,
                                                   List<Map<Integer, Long>> statusHash2Size,
                                                   boolean[] canBeInput,
                                                   List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus) {
        // 找到有效的CpModel变量
        List<IntVar> cardinalityVars = new ArrayList<>();
        int anchor = vars.length / (filterStatus2PkStatus.get(0).getValue().size() + 1) * (joinStatusIndex + 1);
        Map<Integer, ArrayList<Integer>> hash2Index = new HashMap<>();
        IntStream.range(0, filterStatus2PkStatus.size()).filter(i -> canBeInput[i]).forEach(i -> {
            boolean[] fkStatus = filterStatus2PkStatus.get(i).getValue().get(joinStatusIndex);
            Integer fkHash = Arrays.hashCode(fkStatus);
            hash2Index.computeIfAbsent(fkHash, v -> new ArrayList<>());
            hash2Index.get(fkHash).add(anchor + i);
            cardinalityVars.add(vars[anchor + i]);
            validCardinality.add(i);
        });
        model.addEquality(LinearExpr.sum(cardinalityVars.toArray(IntVar[]::new)), pkCardinalitySize);
        var pkStatusHash2Size = statusHash2Size.get(joinStatusIndex);
        for (Map.Entry<Integer, ArrayList<Integer>> hash2VarIndex : hash2Index.entrySet()) {
            IntVar[] sameCardinalitySize = new IntVar[hash2VarIndex.getValue().size()];
            for (int i = 0; i < sameCardinalitySize.length; i++) {
                sameCardinalitySize[i] = vars[hash2VarIndex.getValue().get(i)];
            }
            model.addLessOrEqual(LinearExpr.sum(sameCardinalitySize), pkStatusHash2Size.get(hash2VarIndex.getKey()));
        }
    }

    public static void addCardinalityConstraint(int joinStatusIndex, int joinStatusLocation,
                                                CpModel model, IntVar[] vars, int pkCardinalitySize,
                                                List<Map<Integer, Long>> statusHash2Size,
                                                boolean[] canBeInput,
                                                List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus) {
        // 找到有效的CpModel变量
        List<IntVar> cardinalityVars = new ArrayList<>();
        int anchor = vars.length / (filterStatus2PkStatus.get(0).getValue().size() + 1) * (joinStatusIndex + 1);
        Map<Integer, ArrayList<Integer>> hash2Index = new HashMap<>();
        IntStream.range(0, filterStatus2PkStatus.size()).filter(i -> canBeInput[i]).forEach(i -> {
            boolean[] fkStatus = filterStatus2PkStatus.get(i).getValue().get(joinStatusIndex);
            if (fkStatus[joinStatusLocation]) {
                Integer fkHash = Arrays.hashCode(fkStatus);
                hash2Index.computeIfAbsent(fkHash, v -> new ArrayList<>());
                hash2Index.get(fkHash).add(anchor + i);
                cardinalityVars.add(vars[anchor + i]);
                validCardinality.add(i);
            }
        });
        model.addEquality(LinearExpr.sum(cardinalityVars.toArray(IntVar[]::new)), pkCardinalitySize);
        var pkStatusHash2Size = statusHash2Size.get(joinStatusIndex);
        for (Map.Entry<Integer, ArrayList<Integer>> hash2VarIndex : hash2Index.entrySet()) {
            IntVar[] sameCardinalitySize = new IntVar[hash2VarIndex.getValue().size()];
            for (int i = 0; i < sameCardinalitySize.length; i++) {
                sameCardinalitySize[i] = vars[hash2VarIndex.getValue().get(i)];
            }
            model.addLessOrEqual(LinearExpr.sum(sameCardinalitySize), pkStatusHash2Size.get(hash2VarIndex.getKey()));
        }
    }

    public static void addEqJoinFkConstraint(CpModel model, IntVar[] vars, int eqJoinSize,
                                             ConstraintChainFkJoinNode fkJoinNode, boolean[] canBeInput,
                                             List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus) {
        // 获取join对应的位置
        int joinStatusIndex = fkJoinNode.joinStatusIndex;
        int joinStatusLocation = fkJoinNode.joinStatusLocation;
        List<IntVar> validVars = new ArrayList<>();
        boolean status = !fkJoinNode.getType().isAnti();
        IntStream.range(0, filterStatus2PkStatus.size()).filter(i -> canBeInput[i]).forEach(i -> {
            if (filterStatus2PkStatus.get(i).getValue().get(joinStatusIndex)[joinStatusLocation] == status) {
                validVars.add(vars[i]);
            } else {
                canBeInput[i] = false;
            }
        });
        logger.info("输出的数据量为:{}, 为第{}个表的第{}个状态", eqJoinSize, joinStatusIndex, joinStatusLocation);
        model.addEquality(LinearExpr.sum(validVars.toArray(IntVar[]::new)), eqJoinSize);
    }
}
