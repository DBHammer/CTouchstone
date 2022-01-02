package ecnu.db.generator.joininfo;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class RuleTable {
    Map<JoinStatus, List<Map.Entry<Long, Long>>> rules = new HashMap<>();

    Map<Integer, List<Map.Entry<Long, Long>>> mergedRules = new HashMap<>();

    Map<Integer, Long> mergedSize = new HashMap<>();

    Map<Integer, Long> ruleCounter = new HashMap<>();

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
            mergedSize.put(status2KeyList.getKey(), size);
            ruleCounter.put(status2KeyList.getKey(), 0L);
        }
        System.out.println(mergedRules);
        System.out.println(mergedSize);
        return mergedSize;
    }

    public long getKey(boolean[] status, long index) {
        int statusHash = Arrays.hashCode(status);
        List<Map.Entry<Long, Long>> ranges = mergedRules.get(statusHash);
        long currentSize = 0;
        if (index < 0) {
            long currentIndex = ruleCounter.get(statusHash);
            long totalSize = mergedSize.get(statusHash);
            long validRange = totalSize - currentIndex;
            if (validRange > 0) {
                index = ThreadLocalRandom.current().nextLong(validRange) + currentIndex;
            } else {
                index = ThreadLocalRandom.current().nextLong(totalSize);
            }
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
            System.out.println("error overhead");
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
