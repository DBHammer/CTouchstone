package ecnu.db.generator;

import com.google.ortools.Loader;
import com.google.ortools.sat.*;
import ecnu.db.generator.constraintchain.ConstraintChain;
import ecnu.db.generator.constraintchain.join.ConstraintChainFkJoinNode;
import ecnu.db.generator.joininfo.JoinStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.IntStream;

public class ConstructCpModel {
    static Logger logger = LoggerFactory.getLogger(ConstructCpModel.class);
    private static BigDecimal pkRange;
    private static CpModel model;
    private static IntVar[] vars;

    static {
        Loader.loadNativeLibraries();
    }

    private ConstructCpModel() {
    }

    public static void setPkRange(BigDecimal pkRange) {
        ConstructCpModel.pkRange = pkRange;
    }

    /**
     * 根据join info table计算不同status的填充数量
     *
     * @param haveFkConstrainChains 具有外键的约束链
     * @param filterHistogram       filter status的统计直方图
     * @param filterStatus2PkStatus filter status -> pk status的所有填充方案
     * @param filterSize            每一个约束链的filterStatus中状态为T的行数量
     * @param range                 每个填充方案的的上界
     * @return 一个可行的填充方案
     */
    public static long[] computeWithCpModel(List<ConstraintChain> haveFkConstrainChains,
                                            SortedMap<JoinStatus, Long> filterHistogram,
                                            List<Map<JoinStatus, Long>> statusHash2Size,
                                            List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus,
                                            int[] filterSize, int range) {
        logger.info("filterHistogram: {}", filterHistogram);
        model = new CpModel();
        if (haveFkConstrainChains.stream().anyMatch(ConstraintChain::hasCardinalityConstraint)) {
            vars = initDistinctModel(filterHistogram, filterStatus2PkStatus, range);
        } else {
            vars = initModel(filterHistogram, filterStatus2PkStatus, range);
        }
        int chainIndex = 0;
        for (ConstraintChain haveFkConstrainChain : haveFkConstrainChains) {
            haveFkConstrainChain.addConstraint2Model(chainIndex, filterSize[chainIndex],
                    range - filterSize[chainIndex], statusHash2Size, filterStatus2PkStatus);
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
                                              int range) {
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
                                      int range) {
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

    public static void addIndexJoinFkConstraint(int indexJoinSize,
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
        logger.info("indexjoin输出的数据量为:{}, 为第{}个表的第{}个状态", indexJoinSize, joinStatusIndex, joinStatusLocation);
        model.addEquality(LinearExpr.sum(indexJoinVars.toArray(IntVar[]::new)), indexJoinSize);
    }

    public static void addAggCardinalityConstraint(int joinStatusIndex,
                                                   int pkCardinalitySize, int cardinalityBound,
                                                   List<Map<JoinStatus, Long>> statusHash2Size,
                                                   boolean[] canBeInput,
                                                   List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus) {
        // 找到有效的CpModel变量
        List<IntVar> cardinalityVars = new ArrayList<>();
        // 找到var中对应的range空间
        int anchor = vars.length / (filterStatus2PkStatus.get(0).getValue().size() + 1) * (joinStatusIndex + 1);
        // 维护同一个status的所有var，他们共享了一个pk status的size
        Map<JoinStatus, ArrayList<IntVar>> hash2Index = new HashMap<>();
        // 与join不同的是 agg 不需要join的location
        IntStream.range(0, filterStatus2PkStatus.size()).filter(i -> canBeInput[i]).forEach(i -> {
            JoinStatus fkStatus = new JoinStatus(filterStatus2PkStatus.get(i).getValue().get(joinStatusIndex));
            hash2Index.computeIfAbsent(fkStatus, v -> new ArrayList<>());
            hash2Index.get(fkStatus).add(vars[anchor + i]);
            cardinalityVars.add(vars[anchor + i]);
            model.addLessOrEqual(vars[i], LinearExpr.affine(vars[anchor + i], cardinalityBound, 0));
        });
        model.addEquality(LinearExpr.sum(cardinalityVars.toArray(IntVar[]::new)), pkCardinalitySize);
        var pkStatusHash2Size = statusHash2Size.get(joinStatusIndex);
        for (Map.Entry<JoinStatus, ArrayList<IntVar>> hash2VarIndex : hash2Index.entrySet()) {
            long maxLimitation = pkRange.multiply(BigDecimal.valueOf(pkStatusHash2Size.get(hash2VarIndex.getKey()))).setScale(0, RoundingMode.UP).longValue();
            logger.info("cardinality limitation is {}", maxLimitation);
            model.addLessOrEqual(LinearExpr.sum(hash2VarIndex.getValue().toArray(new IntVar[0])), maxLimitation);
        }
    }

    public static void addCardinalityConstraint(int joinStatusIndex, int joinStatusLocation,
                                                int pkCardinalitySize, int cardinalityBound,
                                                List<Map<JoinStatus, Long>> statusHash2Size,
                                                boolean[] canBeInput,
                                                List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus) {
        // 找到有效的CpModel变量
        List<IntVar> cardinalityVars = new ArrayList<>();
        int anchor = vars.length / (filterStatus2PkStatus.get(0).getValue().size() + 1) * (joinStatusIndex + 1);
        Map<JoinStatus, ArrayList<IntVar>> hash2Index = new HashMap<>();
        IntStream.range(0, filterStatus2PkStatus.size()).filter(i -> canBeInput[i]).forEach(i -> {
            boolean[] fkStatus = filterStatus2PkStatus.get(i).getValue().get(joinStatusIndex);
            if (fkStatus[joinStatusLocation]) {
                JoinStatus fkStatusObj = new JoinStatus(fkStatus);
                hash2Index.computeIfAbsent(fkStatusObj, v -> new ArrayList<>());
                hash2Index.get(fkStatusObj).add(vars[anchor + i]);
                cardinalityVars.add(vars[anchor + i]);
                model.addLessOrEqual(vars[i], LinearExpr.affine(vars[anchor + i], cardinalityBound, 0));
            }
        });
        model.addEquality(LinearExpr.sum(cardinalityVars.toArray(IntVar[]::new)), pkCardinalitySize);
        var pkStatusHash2Size = statusHash2Size.get(joinStatusIndex);
        for (Map.Entry<JoinStatus, ArrayList<IntVar>> hash2VarIndex : hash2Index.entrySet()) {
            long maxLimitation = pkRange.multiply(BigDecimal.valueOf(pkStatusHash2Size.get(hash2VarIndex.getKey()))).setScale(0, RoundingMode.UP).longValue();
            logger.info("cardinality limitation is {}", maxLimitation);
            model.addLessOrEqual(LinearExpr.sum(hash2VarIndex.getValue().toArray(new IntVar[0])), maxLimitation);
        }
    }

    public static void addEqJoinFkConstraint(int eqJoinSize,
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
