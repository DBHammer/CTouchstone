package ecnu.db.generator.joininfo;

import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.LinearExpr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class RuleTableManager {
    private final Map<String, RuleTable> ruleTableMap = new HashMap<>();
    private final Logger logger = LoggerFactory.getLogger(RuleTableManager.class);
    private static final RuleTableManager INSTANCE = new RuleTableManager();

    public static RuleTableManager getInstance() {
        return INSTANCE;
    }

    private RuleTableManager() {
    }

    public List<boolean[]> getAllStatusRule(String colName, List<Integer> location) {
        return ruleTableMap.get(colName).getStatus(location);
    }

    public Map<JoinStatus, AtomicLong> addRuleTable(String tableName, Map<JoinStatus, Long> pkHistogram, long indexStart) {
        ruleTableMap.computeIfAbsent(tableName, v -> new RuleTable());
        Map<JoinStatus, AtomicLong> pkStatus2Index = new HashMap<>();
        long accumulativeIndex = indexStart;
        for (Map.Entry<JoinStatus, Long> pk2Size : pkHistogram.entrySet()) {
            long size = pk2Size.getValue();
            ruleTableMap.get(tableName).addRule(pk2Size.getKey(),
                    new AbstractMap.SimpleEntry<>(accumulativeIndex, accumulativeIndex + size));
            pkStatus2Index.put(pk2Size.getKey(), new AtomicLong(accumulativeIndex));
            accumulativeIndex += size;
        }
        return pkStatus2Index;
    }

    public List<StringBuilder> populateFks(SortedMap<String, List<Integer>> involveFks,
                                           List<List<boolean[]>> fkStatus) {
        List<RuleTable> ruleTables = new ArrayList<>();
        for (String refTable : involveFks.keySet()) {
            ruleTables.add(ruleTableMap.get(refTable));
        }
        List<List<Integer>> location = new ArrayList<>(involveFks.size());
        location.addAll(involveFks.values());
        List<StringBuilder> fksList = new ArrayList<>();
        for (List<boolean[]> status : fkStatus) {
            StringBuilder fks = new StringBuilder();
            for (int statusTableIndex = 0; statusTableIndex < status.size(); statusTableIndex++) {
                long fk = ruleTables.get(statusTableIndex).getKey(location.get(statusTableIndex), status.get(statusTableIndex));
                fks.append(fk).append(",");
            }
            fksList.add(fks);
        }
        return fksList;
    }

    public IntVar[] initModel(SortedMap<JoinStatus, Long> filterHistogram,
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
}
