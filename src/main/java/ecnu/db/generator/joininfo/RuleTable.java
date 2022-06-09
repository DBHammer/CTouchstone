package ecnu.db.generator.joininfo;

import ecnu.db.generator.FkGenerator;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class RuleTable {
    Map<JoinStatus, List<Map.Entry<Long, Long>>> rules = new HashMap<>();
    Map<JoinStatus, List<Map.Entry<Long, Long>>> mergedRules = new HashMap<>();
    Map<JoinStatus, Long> mergedSize = new HashMap<>();
    Map<JoinStatus, Long> ruleCounter = new HashMap<>();

    public Map<JoinStatus, Long> getRuleCounter() {
        return ruleCounter;
    }

    public void addRule(JoinStatus status, Map.Entry<Long, Long> range) {
        rules.computeIfAbsent(status, value -> new ArrayList<>());
        rules.get(status).add(range);
    }

    public JoinStatus[] mergeRules(List<Integer> location, boolean withNull) {
        mergedRules = new HashMap<>();
        for (Map.Entry<JoinStatus, List<Map.Entry<Long, Long>>> joinStatusListEntry : rules.entrySet()) {
            boolean[] joinStatus = joinStatusListEntry.getKey().status();
            JoinStatus pkStatus = FkGenerator.chooseCorrespondingStatus(joinStatus, location);
            mergedRules.computeIfAbsent(pkStatus, v -> new ArrayList<>());
            mergedRules.get(pkStatus).addAll(joinStatusListEntry.getValue());
        }
        mergedSize = new HashMap<>();
        ruleCounter = new HashMap<>();
        for (var mergedRule : mergedRules.entrySet()) {
            long size = mergedRule.getValue().stream().mapToLong(range -> range.getValue() - range.getKey()).sum();
            mergedSize.put(mergedRule.getKey(), size);
            ruleCounter.put(mergedRule.getKey(), 0L);
        }
        JoinStatus[] pkStatuses = new JoinStatus[mergedRules.size()];
        int i = 0;
        for (JoinStatus joinStatus : mergedRules.keySet()) {
            pkStatuses[i++] = joinStatus;
        }
        // deal with null
        if (withNull) {
            JoinStatus allFalseStatus = new JoinStatus(new boolean[location.size()]);
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

    public long getKey(JoinStatus joinStatus, long index) {
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
