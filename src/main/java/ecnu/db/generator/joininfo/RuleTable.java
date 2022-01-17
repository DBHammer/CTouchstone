package ecnu.db.generator.joininfo;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class RuleTable {
    double scaleFactor = 1;
    Map<JoinStatus, List<Map.Entry<Long, Long>>> rules = new HashMap<>();
    Map<Integer, List<Map.Entry<Long, Long>>> mergedRules = new HashMap<>();
    Map<Integer, Long> mergedSize = new HashMap<>();
    Map<Integer, Long> ruleCounter = new HashMap<>();

    public void setScaleFactor(double scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    public Map<Integer, Long> getRuleCounter() {
        return ruleCounter;
    }

    public void addRule(JoinStatus status, Map.Entry<Long, Long> range) {
        rules.computeIfAbsent(status, value -> new ArrayList<>());
        rules.get(status).add(range);
    }

    public Map<Integer, Long> mergeRules(List<Integer> location) {
        mergedRules = new HashMap<>();
        for (Map.Entry<JoinStatus, List<Map.Entry<Long, Long>>> joinStatusListEntry : rules.entrySet()) {
            boolean[] joinStatus = joinStatusListEntry.getKey().status();
            boolean[] subStatus = new boolean[location.size()];
            for (int i = 0; i < location.size(); i++) {
                subStatus[i] = joinStatus[location.get(i)];
            }
            int statusHash = Arrays.hashCode(subStatus);
            mergedRules.computeIfAbsent(statusHash, v -> new ArrayList<>());
            mergedRules.get(statusHash).addAll(joinStatusListEntry.getValue());
        }
        mergedSize = new HashMap<>();
        ruleCounter = new HashMap<>();
        for (Map.Entry<Integer, List<Map.Entry<Long, Long>>> status2KeyList : mergedRules.entrySet()) {
            long size = 0;
            for (Map.Entry<Long, Long> range : status2KeyList.getValue()) {
                size += range.getValue() - range.getKey();
            }
            mergedSize.put(status2KeyList.getKey(), (long) (size * scaleFactor));
            ruleCounter.put(status2KeyList.getKey(), 0L);
        }
        return mergedSize;
    }

    public long getKey(boolean[] status, long index) {
        int statusHash = Arrays.hashCode(status);
        List<Map.Entry<Long, Long>> ranges = mergedRules.get(statusHash);
        long currentSize = 0;
        // 近似处理跨表参照关系
        if (index < 0) {
            var range = ranges.get(ThreadLocalRandom.current().nextInt(ranges.size()));
            return range.getKey() + ThreadLocalRandom.current().nextLong(Math.min(48, range.getValue() - range.getKey()));
        }

        //todo may be error for other benchmark
        while (true) {
            for (Map.Entry<Long, Long> range : ranges) {
                long nextSize = currentSize + range.getValue() - range.getKey();
                if (nextSize > index) {
                    return range.getKey() + index - currentSize;
                } else {
                    currentSize = nextSize;
                }
            }
        }
    }


    public List<boolean[]> getStatus(List<Integer> location) {
        Collections.sort(location);
        Map<Integer, boolean[]> allStatus = new HashMap<>();
        for (JoinStatus joinStatus : rules.keySet()) {
            boolean[] result = new boolean[location.size()];
            boolean[] status = joinStatus.status();
            for (int i = 0; i < result.length; i++) {
                result[i] = status[location.get(i)];
            }
            allStatus.putIfAbsent(Arrays.hashCode(result), result);
        }
        return allStatus.values().stream().toList();
    }

}
