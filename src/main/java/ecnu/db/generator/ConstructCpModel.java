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

    public static long[] solve() {
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

    public static void initDistinctModel(SortedMap<JoinStatus, Long> filterHistogram, int anchor, int varNum, int range) {
        model = new CpModel();
        vars = new IntVar[varNum];
        logger.debug("create {} vars", varNum);
        for (int i = 0; i < anchor; i++) {
            vars[i] = model.newIntVar(0, range, String.valueOf(i));
            for (int j = 1; j <= varNum / anchor - 1; j++) {
                vars[i + j * anchor] = model.newIntVar(0, range, String.valueOf(i + j * anchor));
                model.addLessOrEqual(vars[i + j * anchor], vars[i]);
            }
        }
        int i = 0;
        int allPkSize = anchor / filterHistogram.size();
        for (Map.Entry<JoinStatus, Long> filter2Status : filterHistogram.entrySet()) {
            model.addEquality(LinearExpr.sum(Arrays.copyOfRange(vars, i, i + allPkSize)), filter2Status.getValue());
            i += allPkSize;
        }
    }

    /**
     * 根据join info table计算不同status的填充数量
     *
     * @param filterHistogram filter status的统计直方图
     * @param varNum          所有填充方案的数量
     * @param range           每个填充方案的的上界
     */
    public static void initModel(SortedMap<JoinStatus, Long> filterHistogram, int varNum, int range) {
        model = new CpModel();
        vars = new IntVar[varNum];
        logger.debug("create {} vars", varNum);
        for (int i = 0; i < varNum; i++) {
            vars[i] = model.newIntVar(0, range, String.valueOf(i));
        }
        int i = 0;
        int allPkSize = varNum / filterHistogram.size();
        for (Map.Entry<JoinStatus, Long> filter2Status : filterHistogram.entrySet()) {
            model.addEquality(LinearExpr.sum(Arrays.copyOfRange(vars, i, i + allPkSize)), filter2Status.getValue());
            i += allPkSize;
        }
    }

    public static void addJoinDistinctConstraint(int joinStatusIndex, int joinStatusLocation,
                                                 int pkCardinalitySize, long fkTableSize, long fkColCardinality,
                                                 List<Map<JoinStatus, Long>> status2Size,
                                                 boolean[] canBeInput,
                                                 List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus) {
        // 找到有效的CpModel变量
        List<IntVar> cardinalityVars = new ArrayList<>();
        // 找到var中对应的range空间
        int anchor = vars.length / (filterStatus2PkStatus.get(0).getValue().size() + 1) * (joinStatusIndex + 1);
        // 维护同一个status的所有var，他们共享了一个pk status的size
        Map<JoinStatus, ArrayList<IntVar>> status2Index = new HashMap<>();
        for (int i = 0; i < filterStatus2PkStatus.size(); i++) {
            if (canBeInput[i]) {
                JoinStatus fkStatus = new JoinStatus(filterStatus2PkStatus.get(i).getValue().get(joinStatusIndex));
                if (joinStatusLocation < 0 || fkStatus.status()[joinStatusLocation]) {
                    status2Index.computeIfAbsent(fkStatus, v -> new ArrayList<>());
                    status2Index.get(fkStatus).add(vars[anchor + i]);
                    cardinalityVars.add(vars[anchor + i]);
                    model.addLessOrEqual(LinearExpr.term(vars[i], fkColCardinality), LinearExpr.term(vars[anchor + i], fkTableSize));
                }
            }
        }
        model.addEquality(LinearExpr.sum(cardinalityVars.toArray(IntVar[]::new)), pkCardinalitySize);
        var pkStatus2Size = status2Size.get(joinStatusIndex);
        for (Map.Entry<JoinStatus, ArrayList<IntVar>> hash2VarIndex : status2Index.entrySet()) {
            BigDecimal bMaxLimitation = BigDecimal.valueOf(pkStatus2Size.get(hash2VarIndex.getKey())).multiply(pkRange);
            long maxLimitation = bMaxLimitation.setScale(0, RoundingMode.UP).longValue();
            logger.info("cardinality limitation is {}", maxLimitation);
            model.addLessOrEqual(LinearExpr.sum(hash2VarIndex.getValue().toArray(new IntVar[0])), maxLimitation);
        }
    }

    public static void addJoinCardinalityConstraint(List<Integer> validIndexes, int eqJoinSize) {
        IntVar[] validVars = new IntVar[validIndexes.size()];
        for (int i = 0; i < validVars.length; i++) {
            validVars[i] = vars[validIndexes.get(i)];
        }
        model.addEquality(LinearExpr.sum(validVars), eqJoinSize);
    }
}
