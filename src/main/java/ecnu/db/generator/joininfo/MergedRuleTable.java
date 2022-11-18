package ecnu.db.generator.joininfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class MergedRuleTable {
    Map<JoinStatus, List<Map.Entry<Long, Long>>> mergedRules = new HashMap<>();
    Map<JoinStatus, Long> mergedSize;
    Map<JoinStatus, Long> ruleCounter;

    public MergedRuleTable(Map<JoinStatus, List<Map.Entry<Long, Long>>> mergedRules) {
        this.mergedRules = mergedRules;
        mergedSize = new HashMap<>();
        ruleCounter = new HashMap<>();
        for (var mergedRule : mergedRules.entrySet()) {
            long size = mergedRule.getValue().stream().mapToLong(range -> range.getValue() - range.getKey()).sum();
            mergedSize.put(mergedRule.getKey(), size);
            ruleCounter.put(mergedRule.getKey(), 0L);
        }
    }

    public JoinStatus[] getPkStatus(boolean withNull) {
        JoinStatus[] pkStatuses = new JoinStatus[mergedRules.size()];
        int i = 0;
        for (JoinStatus joinStatus : mergedRules.keySet()) {
            pkStatuses[i++] = joinStatus;
        }
        // deal with null
        if (withNull) {
            int statusLength = new ArrayList<>(mergedSize.keySet()).get(0).status().length;
            JoinStatus allFalseStatus = new JoinStatus(new boolean[statusLength]);
            if (!mergedRules.containsKey(allFalseStatus)) {
                JoinStatus[] copy = new JoinStatus[pkStatuses.length + 1];
                System.arraycopy(pkStatuses, 0, copy, 0, pkStatuses.length);
                copy[copy.length - 1] = allFalseStatus;
                pkStatuses = copy;
            }
        }
        return pkStatuses;
    }

    public long getStatusSize(JoinStatus status) {
        return mergedSize.get(status);
    }

    public Map<JoinStatus, Long> getRuleCounter() {
        return ruleCounter;
    }

    public long getKey(JoinStatus joinStatus, long index) {
        // todo 根据ruletable 递增range的start
        if (!mergedRules.containsKey(joinStatus)) {
            return Long.MIN_VALUE;
        }
        if (index < 0) {
            index = ThreadLocalRandom.current().nextLong(mergedSize.get(joinStatus));
        }
        long currentSize = 0;
        for (Map.Entry<Long, Long> range : mergedRules.get(joinStatus)) {
            long nextSize = currentSize + range.getValue() - range.getKey();
            if (nextSize > index) {
                return index - currentSize + range.getKey();
            }
            currentSize = nextSize;
        }
        return Long.MIN_VALUE;
    }
}
