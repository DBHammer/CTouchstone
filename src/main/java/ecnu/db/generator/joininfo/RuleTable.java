package ecnu.db.generator.joininfo;

import java.util.*;

public class RuleTable {
    Map<JoinStatus, List<Map.Entry<Long, Long>>> rules = new HashMap<>();

    public void addRule(JoinStatus status, Map.Entry<Long, Long> range) {
        rules.computeIfAbsent(status, value -> new ArrayList<>());
        rules.get(status).add(range);
    }

    public long getKey(List<Integer> location, boolean[] status) {
        for (Map.Entry<JoinStatus, List<Map.Entry<Long, Long>>> joinStatusListEntry : rules.entrySet()) {
            JoinStatus joinStatus = joinStatusListEntry.getKey();
            boolean[] joinStatusStatus = joinStatus.status();
            int i = 0;
            boolean found = true;
            for (Integer integer : location) {
                if (joinStatusStatus[integer] != status[i++]) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return joinStatusListEntry.getValue().get(0).getKey();
            }
        }
        return -1;
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
