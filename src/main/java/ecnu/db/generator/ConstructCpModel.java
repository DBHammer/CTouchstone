package ecnu.db.generator;

import com.google.ortools.Loader;
import com.google.ortools.sat.*;
import ecnu.db.generator.joininfo.JoinStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ConstructCpModel {

    private static final double DISTINCT_FK_SKEW = 2;
    private final Logger logger = LoggerFactory.getLogger(ConstructCpModel.class);
    private final CpModel model = new CpModel();
    private final CpSolver solver = new CpSolver();
    private IntVar[][] vars;
    private final Map<Integer, IntVar[][]> fkDistinctVars = new HashMap<>();
    private final List<IntVar> involvedVars = new LinkedList<>();


    static {
        Loader.loadNativeLibraries();
    }

    public long[][] solve() {
        solver.getParameters().setEnumerateAllSolutions(false);
        CpSolverStatus status = solver.solve(model);
        if (status == CpSolverStatus.OPTIMAL || status == CpSolverStatus.FEASIBLE) {
            logger.info("用时{}ms", solver.wallTime() * 1000);
            int filterStatusCount = vars.length;
            int pkStatusCount = vars[0].length;
            long[][] rowCountForEachStatus = new long[filterStatusCount][pkStatusCount];
            for (int filterIndex = 0; filterIndex < filterStatusCount; filterIndex++) {
                for (int pkStatusIndex = 0; pkStatusIndex < pkStatusCount; pkStatusIndex++) {
                    rowCountForEachStatus[filterIndex][pkStatusCount] = solver.value(vars[filterIndex][pkStatusIndex]);
                }
            }
            return rowCountForEachStatus;
        } else {
            throw new UnsupportedOperationException("No solution found.");
        }
    }

    public FkRange[][] getDistinctResult(int fkColsIndex) {
        // todo 根据ruletable 递增range的start
        IntVar[][] distinctVars = fkDistinctVars.get(fkColsIndex);
        FkRange[][] fkRanges = new FkRange[distinctVars.length][distinctVars[0].length];
        for (int pkStatusIndex = 0; pkStatusIndex < fkRanges[0].length; pkStatusIndex++) {
            long start = 0L;
            for (int filterIndex = 0; filterIndex < fkRanges.length; filterIndex++) {
                long range = solver.value(distinctVars[filterIndex][pkStatusIndex]);
                fkRanges[filterIndex][pkStatusIndex] = new FkRange(start, range);
                start += range;
            }
        }
        return fkRanges;
    }

    public void initDistinctModel(int fkColIndex, long fkColCardinality, long fkTableSize,
                                  Map<ArrayList<Integer>, Long> samePkStatusIndexes2Limitations) {
        IntVar[][] distinctVars = new IntVar[vars.length][vars[0].length];
        // todo 用fk的最大重复次数来替代
        fkTableSize = (long) (fkTableSize * DISTINCT_FK_SKEW);
        for (int filterIndex = 0; filterIndex < distinctVars.length; filterIndex++) {
            for (int pkIndex = 0; pkIndex < distinctVars[0].length; pkIndex++) {
                IntVar numVar = vars[filterIndex][pkIndex];
                String varName = fkColIndex + "-" + filterIndex + "-" + pkIndex;
                IntVar distinctVar = model.newIntVarFromDomain(numVar.getDomain(), varName);
                model.addLessOrEqual(distinctVar, numVar);
                // distinct的外键均匀分布与每个range中, i.e., x / tableSize <= d/fkColCardinality
                model.addLessOrEqual(LinearExpr.term(numVar, fkColCardinality), LinearExpr.term(distinctVar, fkTableSize));
                distinctVars[filterIndex][pkIndex] = distinctVar;
            }
        }
        for (var pkIndexes2Limitation : samePkStatusIndexes2Limitations.entrySet()) {
            var samePkStatusIndexes = pkIndexes2Limitation.getKey();
            IntVar[] sameStatusDistinctVars = new IntVar[samePkStatusIndexes.size() * distinctVars.length];
            int i = 0;
            for (IntVar[] distinctVar : distinctVars) {
                for (Integer pkIndex : samePkStatusIndexes) {
                    sameStatusDistinctVars[i++] = distinctVar[pkIndex];
                }
            }
            model.addLessOrEqual(LinearExpr.sum(sameStatusDistinctVars), pkIndexes2Limitation.getValue());
        }
        fkDistinctVars.put(fkColIndex, distinctVars);
    }


    /**
     * 根据join info table计算不同status的填充数量
     *
     * @param filterHistogram  filter status的统计直方图
     * @param pkJointStatusNum 所有联合主键的数量
     * @param range            每个填充方案的的上界
     */
    public void initModel(Map<JoinStatus, Long> filterHistogram, int pkJointStatusNum, int range) {
        vars = new IntVar[filterHistogram.size()][pkJointStatusNum];
        for (int i = 0; i < filterHistogram.size(); i++) {
            for (int j = 0; j < pkJointStatusNum; j++) {
                vars[i][j] = model.newIntVar(0, range, i + "-" + j);
            }
        }
        int i = 0;
        for (Map.Entry<JoinStatus, Long> status2Size : filterHistogram.entrySet()) {
            model.addEquality(LinearExpr.sum(vars[i++]), status2Size.getValue());
        }
    }

    public void addJoinDistinctValidVar(int fkColIndex, int filterIndex, int pkStatusIndex) {
        involvedVars.add(fkDistinctVars.get(fkColIndex)[filterIndex][pkStatusIndex]);
    }

    public void addJoinCardinalityValidVar(int filterIndex, int pkStatusIndex) {
        involvedVars.add(vars[filterIndex][pkStatusIndex]);
    }

    public void addJoinCardinalityConstraint(long eqJoinSize) {
        model.addEquality(LinearExpr.sum(involvedVars.toArray(new IntVar[0])), eqJoinSize);
        involvedVars.clear();
    }
}
