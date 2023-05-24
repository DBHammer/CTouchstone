package ecnu.db.generator.joininfo;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class MergedRuleTable {
    Map<JoinStatus, Rule> status2Rule = new HashMap<>();

    private static class Rule {
        TreeMap<Long, Long> mergedRules;
        long totalSize;
        long assignCounter;
        long assignMaxIndexForTheBatchCounter;

        public Rule(TreeMap<Long, Long> mergedRules, long totalSize, long assignCounter, long assignMaxIndexForTheBatchCounter) {
            this.mergedRules = mergedRules;
            this.totalSize = totalSize;
            this.assignCounter = assignCounter;
            this.assignMaxIndexForTheBatchCounter = assignMaxIndexForTheBatchCounter;
        }
    }

    public MergedRuleTable(Map<JoinStatus, List<PkRange>> mergedRules) {
        for (Map.Entry<JoinStatus, List<PkRange>> status2PkRanges : mergedRules.entrySet()) {
            long totalNum = 0;
            TreeMap<Long, Long> beforeNum2Range = new TreeMap<>();
            for (PkRange pkRange : status2PkRanges.getValue()) {
                beforeNum2Range.put(totalNum, pkRange.start() - totalNum);
                totalNum += pkRange.end() - pkRange.start();
            }
            status2Rule.put(status2PkRanges.getKey(), new Rule(beforeNum2Range, totalNum, 0L, 0L));
        }
    }

    public JoinStatus[] getPkStatus(boolean withNull) {
        JoinStatus[] pkStatuses = new JoinStatus[status2Rule.size()];
        int i = 0;
        for (JoinStatus joinStatus : status2Rule.keySet()) {
            pkStatuses[i++] = joinStatus;
        }
        // deal with null
        if (withNull) {
            int statusLength = new ArrayList<>(status2Rule.keySet()).get(0).status().length;
            JoinStatus allFalseStatus = new JoinStatus(new boolean[statusLength]);
            if (!status2Rule.containsKey(allFalseStatus)) {
                JoinStatus[] copy = new JoinStatus[pkStatuses.length + 1];
                System.arraycopy(pkStatuses, 0, copy, 0, pkStatuses.length);
                copy[copy.length - 1] = allFalseStatus;
                pkStatuses = copy;
            }
        }
        return pkStatuses;
    }

    public long getStatusSize(JoinStatus status) {
        return status2Rule.get(status).totalSize;
    }

    public void refreshRuleCounter() {
        status2Rule.values().forEach(rule -> {
            rule.assignCounter += rule.assignMaxIndexForTheBatchCounter;
            rule.assignMaxIndexForTheBatchCounter = 0;
        });
    }

    public long getKey(JoinStatus joinStatus, long index) {
        Rule rule = status2Rule.get(joinStatus);
        if (rule == null) {
            return Long.MIN_VALUE;
        }
        if (index < 0) {
            index = ThreadLocalRandom.current().nextLong(rule.totalSize);
        } else {
            if (rule.assignMaxIndexForTheBatchCounter < index) {
                rule.assignMaxIndexForTheBatchCounter = index;
            }
            index += rule.assignCounter;
        }
        var beforeNum2PkRange = rule.mergedRules.floorEntry(index);
        return index + beforeNum2PkRange.getValue();
    }
}
