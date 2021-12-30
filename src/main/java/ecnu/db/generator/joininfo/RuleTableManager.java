package ecnu.db.generator.joininfo;

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

    public RuleTable getRuleTable(String colName){
        return ruleTableMap.get(colName);
    }

    public long getStatueSize(String colName, List<Integer> location, boolean[] status) {
        return ruleTableMap.get(colName).getSize(location, status);
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

}
